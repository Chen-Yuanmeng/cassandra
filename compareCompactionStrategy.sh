#!/usr/bin/env bash
set -euo pipefail

KS=ttl_compare
ROWS_PER_BATCH=200           # rows per memtable flush to keep SSTables small and numerous
CONCURRENT_COMPACTORS=1      # forced bottleneck to create backlog pressure
COMPACTION_THROUGHPUT_MBPS=8 # throttle compaction bandwidth

# Observation windows (seconds) that cover: short TTL expiry, medium decay, long TTL untouched
OBSERVATION_WINDOWS=(0 120 240 360 600)

# Each entry: "TierLabel|TTLSeconds"; order is intentionally interleaved to break natural grouping
BATCH_PLAN=(
  "A|45"   "C|3600" "A|50"   "B|420"  "C|5400" "A|40"
  "B|510"  "C|4800" "A|55"   "B|360"  "C|6000" "A|35"
  "C|3900" "B|600"  "A|60"   "C|4500" "B|330"  "A|48"
  "B|540"  "C|4200" "A|52"   "C|5200" "B|300"  "A|58"
)

TTLAWARE_COMPACTION_OPTIONS="{
  'class': 'org.apache.cassandra.db.compaction.TTLAwareCompactionStrategy',
  'ttl_horizon_seconds': '900'
}"
UNIFIED_COMPACTION_OPTIONS="{
  'class': 'org.apache.cassandra.db.compaction.UnifiedCompactionStrategy'
}"

log() {
  printf '==> %s\n' "$1"
}

describe_objective() {
  cat <<'MSG'
==> Experiment Goal
  * Reproduce heavy-backlog compaction pressure (>=20 SSTables, skewed TTLs, throttled compactions).
  * Compare UnifiedCompactionStrategy vs TTL-Aware on:
      - SSTable prioritization (which generations merge first?)
      - Timeliness of expired/tombstone reclamation
      - Pending compaction backlog trend
      - Disturbance to long-TTL data (are they dragged into compaction?)

==> Core Idea (one-liner)
    Flood the node with interleaved TTL tiers so that compaction cannot keep up, then observe whether
    TTL-Aware prefers high-reclaim-value SSTables while Unified behaves size/generation driven.

MSG
}

remind_constraints() {
  cat <<'MSG'
==> Environment Requirements
  1. Single Cassandra node; keep memtable/cache settings unchanged between runs.
  2. Limit compaction resources (concurrent_compactors = 1, low throughput) to force backlog.
  3. Single table, no manual nodetool compact allowed. Only background compaction may act.
  4. Table uses SimpleStrategy RF=1 to avoid repair cooperation.
MSG
}

configure_cluster_limits() {
  log "Throttling compaction: concurrent_compactors=${CONCURRENT_COMPACTORS}, throughput=${COMPACTION_THROUGHPUT_MBPS}MB/s"
  bin/nodetool setconcurrentcompactors "${CONCURRENT_COMPACTORS}" >/dev/null
  bin/nodetool setcompactionthroughput "${COMPACTION_THROUGHPUT_MBPS}" >/dev/null
}

create_schema() {
  local table_name=$1
  local compaction_json=$2

  log "(Re)creating keyspace ${KS} and table ${table_name}"
  bin/cqlsh <<EOF
DROP KEYSPACE IF EXISTS ${KS};

CREATE KEYSPACE ${KS}
WITH replication = {'class':'SimpleStrategy','replication_factor':1};

USE ${KS};

CREATE TABLE ${table_name} (
  k int,
  c int,
  v text,
  PRIMARY KEY (k, c)
) WITH compaction = ${compaction_json}
  AND gc_grace_seconds = 60; -- fast tombstone reaping in this isolated lab
EOF
}

ingest_batches() {
  local table_name=$1
  local run_tag=$2
  local row_cursor=0

  local batch_number=1
  for entry in "${BATCH_PLAN[@]}"; do
    IFS='|' read -r tier ttl <<<"$entry"
    log "${run_tag}: Batch ${batch_number}/${#BATCH_PLAN[@]} | Tier ${tier} | TTL=${ttl}s"

    for i in $(seq 1 ${ROWS_PER_BATCH}); do
      local clustering=$((row_cursor + i))
      echo "INSERT INTO ${KS}.${table_name} (k, c, v) VALUES (1, ${clustering}, '${tier,,}_${clustering}') USING TTL ${ttl};"
    done | bin/cqlsh

    log "${run_tag}: Flush to seal SSTable #${batch_number}"
    bin/nodetool flush "${KS}" "${table_name}"

    list_sstables "${table_name}" "post-batch-${batch_number}"

    row_cursor=$((row_cursor + ROWS_PER_BATCH))
    batch_number=$((batch_number + 1))
  done
}

list_sstables() {
  local table_name=$1
  local stage=$2
  log "SSTable layout (${stage})"
  ls -1 data/data/${KS}/${table_name}-*/ || log "No SSTables yet"
}

capture_state() {
  local table_name=$1
  local run_tag=$2
  local window_label=$3

  log "${run_tag}: Observation window ${window_label}"
  bin/nodetool compactionstats | sed "s/^/${run_tag} | /"
  bin/nodetool tablestats ${KS}.${table_name} | sed "s/^/${run_tag} | tablestats | /"

  sample_sstable_metadata "${table_name}" "${run_tag}" "${window_label}"
  list_sstables "${table_name}" "${run_tag}-window-${window_label}"
}

sample_sstable_metadata() {
  local table_name=$1
  local run_tag=$2
  local window_label=$3

  if [[ ! -x tools/bin/sstablemetadata ]]; then
    log "${run_tag}: tools/bin/sstablemetadata not found; skipping tombstone sampling"
    return
  fi

  log "${run_tag}: Sampling droppable tombstones (${window_label})"
  find data/data/${KS}/${table_name}-*/ -maxdepth 1 -name '*Data.db' | head -n 5 | while read -r sstable; do
    printf '%s | %s | %s\n' "${run_tag}" "${window_label}" "${sstable}"
    tools/bin/sstablemetadata "${sstable}" | grep -E 'Estimated droppable tombstones|Estimated total rows' || true
  done
}

observe_background_compaction() {
  local table_name=$1
  local run_tag=$2

  local previous_wait=0
  for window in "${OBSERVATION_WINDOWS[@]}"; do
    local sleep_span=$((window - previous_wait))
    if (( sleep_span > 0 )); then
      log "${run_tag}: Waiting ${sleep_span}s (cumulative ${window}s) to let background compaction proceed"
      sleep ${sleep_span}
    fi
    capture_state "${table_name}" "${run_tag}" "T+${window}s"
    previous_wait=${window}
  done
}

run_trial() {
  local run_id=$1
  local run_label=$2
  local compaction_json=$3

  local table_name="tbl_${run_id}_${RANDOM}"

  log "================ ${run_label} ================"
  create_schema "${table_name}" "${compaction_json}"
  ingest_batches "${table_name}" "${run_label}"
  observe_background_compaction "${table_name}" "${run_label}"

  log "${run_label}: Final row count snapshot"
  bin/cqlsh <<EOF
SELECT COUNT(*) AS live_rows FROM ${KS}.${table_name};
EOF

  log "${run_label}: Trial complete\n"
}

main() {
  describe_objective
  remind_constraints
  configure_cluster_limits

  run_trial "ttla" "TTL-Aware" "${TTLAWARE_COMPACTION_OPTIONS}"
  run_trial "ucs" "Unified" "${UNIFIED_COMPACTION_OPTIONS}"

  log "Experiment finished. Compare logs for prioritization, backlog evolution, and long-TTL disturbance."
}

main "$@"
