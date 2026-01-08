#!/usr/bin/env bash
set -euo pipefail

KS=ttl_test
TB="tbl_${RANDOM}"
TTL=30
ROWS=1000

echo "==> Step 1: create keyspace and table"

bin/cqlsh <<EOF
DROP KEYSPACE IF EXISTS ${KS};

CREATE KEYSPACE ${KS}
WITH replication = {'class':'SimpleStrategy','replication_factor':1};

USE ${KS};

CREATE TABLE ${TB} (
  k int,
  c int,
  v text,
  PRIMARY KEY (k, c)
) WITH compaction = {
  'class': 'org.apache.cassandra.db.compaction.TTLAwareCompactionStrategy',
  'ttl_horizon_seconds': '20'
};
EOF

echo "==> Step 2: insert TTL data (${ROWS} rows, TTL=${TTL}s)"

for i in $(seq 1 ${ROWS}); do
  echo "INSERT INTO ${KS}.${TB} (k, c, v) VALUES (1, ${i}, 'value_${i}') USING TTL ${TTL};"
done | bin/cqlsh

echo "==> SSTables after inserts, before flush:"
ls data/data/${KS}/${TB}-*/ -l

echo "==> Step 3: flush to generate SSTables"
bin/nodetool flush ${KS} ${TB}

echo "==> SSTables after flush:"
ls data/data/${KS}/${TB}-*/ -l

echo "==> Step 4: wait for TTL expiration"
sleep $((TTL + 10))

echo "==> Step 5: force compaction"
bin/nodetool compact ${KS} ${TB}

echo "==> Step 6: compaction stats"
bin/nodetool compactionstats

echo "==> SSTables after compaction:"
ls data/data/${KS}/${TB}-*/ -l

echo "==> Step 7: query table (should be empty or near-empty)"
bin/cqlsh <<EOF
SELECT COUNT(*) FROM ${KS}.${TB};
EOF

echo "==> DONE"
