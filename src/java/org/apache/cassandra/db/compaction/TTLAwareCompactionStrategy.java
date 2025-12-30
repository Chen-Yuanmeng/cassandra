/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.SplittingSizeTieredCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.schema.CompactionParams;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Compaction strategy that explicitly promotes SSTables containing expiring cells or tombstones by
 * incorporating TTL and deletion metadata into the compaction score.
 */
public class TTLAwareCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(TTLAwareCompactionStrategy.class);

    private final TTLAwareCompactionStrategyOptions ttlOptions;
    private volatile int estimatedRemainingTasks;
    @VisibleForTesting
    protected final Set<SSTableReader> sstables = new HashSet<>();

    public TTLAwareCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        ttlOptions = new TTLAwareCompactionStrategyOptions(options);
        estimatedRemainingTasks = 0;
    }

    @Override
    public synchronized AbstractCompactionTask getNextBackgroundTask(long gcBefore)
    {
        List<SSTableReader> previousCandidate = null;
        while (true)
        {
            List<SSTableReader> candidates = getNextBackgroundSSTables(gcBefore);
            if (candidates.isEmpty())
                return null;

            if (candidates.equals(previousCandidate))
            {
                logger.warn("Could not acquire references for compacting SSTables {} twice in a row. Will retry later.", candidates);
                return null;
            }

            LifecycleTransaction transaction = cfs.getTracker().tryModify(candidates, OperationType.COMPACTION);
            if (transaction != null)
                return new CompactionTask(cfs, transaction, gcBefore);

            previousCandidate = candidates;
        }
    }

    @Override
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(long gcBefore, boolean splitOutput)
    {
        Collection<SSTableReader> filtered = filterSuspectSSTables(sstables);
        if (filtered.isEmpty())
            return null;

        LifecycleTransaction txn = cfs.getTracker().tryModify(filtered, OperationType.COMPACTION);
        if (txn == null)
            return null;

        AbstractCompactionTask task = splitOutput ? new SplittingCompactionTask(cfs, txn, gcBefore)
                                                  : new CompactionTask(cfs, txn, gcBefore);
        return Collections.singletonList(task);
    }

    @Override
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> toCompact, long gcBefore)
    {
        if (toCompact.isEmpty())
            return null;

        LifecycleTransaction transaction = cfs.getTracker().tryModify(toCompact, OperationType.COMPACTION);
        if (transaction == null)
        {
            logger.trace("Unable to mark {} for compaction; another compaction probably took it first", toCompact);
            return null;
        }

        return new CompactionTask(cfs, transaction, gcBefore).setUserDefined(true);
    }

    @Override
    public synchronized void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

    @Override
    public synchronized void removeSSTable(SSTableReader removed)
    {
        sstables.remove(removed);
    }

    @Override
    protected synchronized Set<SSTableReader> getSSTables()
    {
        return ImmutableSet.copyOf(sstables);
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return ttlOptions.maxCandidateBytes;
    }

    @Override
    public String toString()
    {
        return String.format("TTLAwareCompactionStrategy[min=%d,max=%d]", cfs.getMinimumCompactionThreshold(), cfs.getMaximumCompactionThreshold());
    }

    private synchronized List<SSTableReader> getNextBackgroundSSTables(long gcBefore)
    {
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        List<ScoredSSTable> scored = scoreCandidates(gcBefore);
        if (scored.isEmpty())
        {
            updateRemainingTasks(0);
            return Collections.emptyList();
        }

        scored.sort(Comparator.comparingDouble(ScoredSSTable::score).reversed());

        List<SSTableReader> selected = new ArrayList<>(Math.min(scored.size(), maxThreshold));
        long accumulatedBytes = 0L;
        for (ScoredSSTable candidate : scored)
        {
            if (selected.size() >= maxThreshold)
                break;

            if (candidate.score < ttlOptions.minimumTriggerScore && selected.size() >= minThreshold)
                break;

            long nextSize = candidate.sstable.bytesOnDisk();
            long newSize = accumulatedBytes + nextSize;
            if (newSize > ttlOptions.maxCandidateBytes)
            {
                if (selected.isEmpty())
                    continue;
                break;
            }

            selected.add(candidate.sstable);
            accumulatedBytes = newSize;

            if (accumulatedBytes >= ttlOptions.maxCandidateBytes)
                break;
        }

        if (selected.size() < minThreshold)
        {
            SSTableReader singleton = pickSingleton(scored, gcBefore);
            if (singleton != null)
            {
                updateRemainingTasks(1);
                return Collections.singletonList(singleton);
            }

            updateRemainingTasks(0);
            return Collections.emptyList();
        }

        updateRemainingTasks(estimateTasks(scored.size(), minThreshold, maxThreshold));
        return selected;
    }

    private List<ScoredSSTable> scoreCandidates(long gcBefore)
    {
        List<ScoredSSTable> scored = new ArrayList<>();
        for (SSTableReader sstable : filterSuspectSSTables(cfs.getUncompactingSSTables()))
        {
            if (!sstables.contains(sstable))
                continue;

            double score = score(sstable, gcBefore);
            if (!Double.isFinite(score))
                continue;

            scored.add(new ScoredSSTable(sstable, score));
        }
        return scored;
    }

    private void updateRemainingTasks(int tasks)
    {
        estimatedRemainingTasks = tasks;
        cfs.getCompactionStrategyManager().compactionLogger.pending(this, tasks);
    }

    private int estimateTasks(int sstableCount, int minThreshold, int maxThreshold)
    {
        if (sstableCount < minThreshold)
            return 0;
        return (int) Math.ceil((double) sstableCount / maxThreshold);
    }

    private SSTableReader pickSingleton(List<ScoredSSTable> scored, long gcBefore)
    {
        SSTableReader best = null;
        double bestScore = Double.NEGATIVE_INFINITY;
        for (ScoredSSTable candidate : scored)
        {
            if (!worthDroppingTombstones(candidate.sstable, gcBefore))
                continue;

            if (candidate.score > bestScore)
            {
                best = candidate.sstable;
                bestScore = candidate.score;
            }
        }
        return best;
    }

    private double score(SSTableReader sstable, long gcBefore)
    {
        double tombstoneDensity = clamp01(sstable.getEstimatedDroppableTombstoneRatio(gcBefore));
        double ttlUrgency = computeTtlUrgency(sstable);
        double ageFactor = computeAgeFactor(sstable);
        double sizePenalty = computeSizePenalty(sstable);

        return ttlOptions.alpha * tombstoneDensity
             + ttlOptions.beta * ttlUrgency
             + ttlOptions.gamma * ageFactor
             - ttlOptions.delta * sizePenalty;
    }

    private double computeTtlUrgency(SSTableReader sstable)
    {
        StatsMetadata metadata = sstable.getSSTableMetadata();
        long minDeletionTime = metadata.minLocalDeletionTime;
        if (minDeletionTime == Long.MAX_VALUE || ttlOptions.ttlHorizonSeconds <= 0)
            return 0.0d;

        long nowSeconds = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis());
        long secondsUntilExpiry = minDeletionTime - nowSeconds;
        if (secondsUntilExpiry <= 0)
            return 1.0d;

        if (secondsUntilExpiry >= ttlOptions.ttlHorizonSeconds)
            return 0.0d;

        double urgency = 1.0d - ((double) secondsUntilExpiry / ttlOptions.ttlHorizonSeconds);
        return clamp01(urgency);
    }

    private double computeAgeFactor(SSTableReader sstable)
    {
        if (ttlOptions.ageHorizonMicros <= 0)
            return 0.0d;

        long nowMicros = TimeUnit.MILLISECONDS.toMicros(currentTimeMillis());
        long maxTimestamp = sstable.getMaxTimestamp();
        if (maxTimestamp == Long.MIN_VALUE || maxTimestamp == Long.MAX_VALUE)
            return 0.0d;

        long ageMicros = Math.max(0L, nowMicros - maxTimestamp);
        double normalized = (double) ageMicros / ttlOptions.ageHorizonMicros;
        return clamp01(normalized);
    }

    private double computeSizePenalty(SSTableReader sstable)
    {
        if (ttlOptions.sizePenaltyBytes <= 0)
            return 0.0d;

        double penalty = (double) sstable.bytesOnDisk() / ttlOptions.sizePenaltyBytes;
        return clamp01(penalty);
    }

    private static double clamp01(double value)
    {
        if (value < 0.0d)
            return 0.0d;
        if (value > 1.0d)
            return 1.0d;
        return value;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> unchecked = AbstractCompactionStrategy.validateOptions(options);
        TTLAwareCompactionStrategyOptions.validateOptions(options, unchecked);
        unchecked.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        unchecked.remove(CompactionParams.Option.MAX_THRESHOLD.toString());
        return unchecked;
    }

    private static final class ScoredSSTable
    {
        final SSTableReader sstable;
        final double score;

        private ScoredSSTable(SSTableReader sstable, double score)
        {
            this.sstable = sstable;
            this.score = score;
        }

        double score()
        {
            return score;
        }
    }

    private static final class TTLAwareCompactionStrategyOptions
    {
        static final String ALPHA_OPTION = "alpha";
        static final String BETA_OPTION = "beta";
        static final String GAMMA_OPTION = "gamma";
        static final String DELTA_OPTION = "delta";
        static final String TTL_HORIZON_SECONDS_OPTION = "ttl_horizon_seconds";
        static final String AGE_HORIZON_SECONDS_OPTION = "age_horizon_seconds";
        static final String SIZE_PENALTY_MB_OPTION = "size_penalty_target_mb";
        static final String MAX_CANDIDATE_MB_OPTION = "max_candidate_bytes_mb";
        static final String MIN_TRIGGER_SCORE_OPTION = "minimum_trigger_score";

        static final double DEFAULT_ALPHA = 1.0d;
        static final double DEFAULT_BETA = 0.7d;
        static final double DEFAULT_GAMMA = 0.3d;
        static final double DEFAULT_DELTA = 0.2d;
        static final double DEFAULT_MIN_TRIGGER_SCORE = 0.15d;
        static final long DEFAULT_TTL_HORIZON_SECONDS = TimeUnit.HOURS.toSeconds(6);
        static final long DEFAULT_AGE_HORIZON_SECONDS = TimeUnit.HOURS.toSeconds(24);
        static final long DEFAULT_SIZE_PENALTY_BYTES = 512L * 1024L * 1024L;
        static final long DEFAULT_MAX_CANDIDATE_BYTES = 2L * 1024L * 1024L * 1024L;

        final double alpha;
        final double beta;
        final double gamma;
        final double delta;
        final double minimumTriggerScore;
        final long ttlHorizonSeconds;
        final long ageHorizonMicros;
        final long sizePenaltyBytes;
        final long maxCandidateBytes;

        TTLAwareCompactionStrategyOptions(Map<String, String> options)
        {
            alpha = parsePositiveDouble(options, ALPHA_OPTION, DEFAULT_ALPHA);
            beta = parsePositiveDouble(options, BETA_OPTION, DEFAULT_BETA);
            gamma = parsePositiveDouble(options, GAMMA_OPTION, DEFAULT_GAMMA);
            delta = parsePositiveDouble(options, DELTA_OPTION, DEFAULT_DELTA);
            if (!(alpha > beta && beta > gamma))
                throw new ConfigurationException("Expected alpha > beta > gamma for TTLAwareCompactionStrategy");

            minimumTriggerScore = clampScore(parsePositiveDouble(options, MIN_TRIGGER_SCORE_OPTION, DEFAULT_MIN_TRIGGER_SCORE));
            ttlHorizonSeconds = parsePositiveLong(options, TTL_HORIZON_SECONDS_OPTION, DEFAULT_TTL_HORIZON_SECONDS);
            long ageHorizonSeconds = parsePositiveLong(options, AGE_HORIZON_SECONDS_OPTION, DEFAULT_AGE_HORIZON_SECONDS);
            ageHorizonMicros = TimeUnit.SECONDS.toMicros(ageHorizonSeconds);

            long sizePenaltyMb = parsePositiveLong(options, SIZE_PENALTY_MB_OPTION, bytesToMb(DEFAULT_SIZE_PENALTY_BYTES));
            sizePenaltyBytes = megabytesToBytes(sizePenaltyMb);

            long maxCandidateMb = parsePositiveLong(options, MAX_CANDIDATE_MB_OPTION, bytesToMb(DEFAULT_MAX_CANDIDATE_BYTES));
            maxCandidateBytes = Math.max(megabytesToBytes(maxCandidateMb), sizePenaltyBytes);
        }

        static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> unchecked) throws ConfigurationException
        {
            validateDouble(options, ALPHA_OPTION);
            validateDouble(options, BETA_OPTION);
            validateDouble(options, GAMMA_OPTION);
            validateDouble(options, DELTA_OPTION);
            validateDouble(options, MIN_TRIGGER_SCORE_OPTION);
            validateLong(options, TTL_HORIZON_SECONDS_OPTION);
            validateLong(options, AGE_HORIZON_SECONDS_OPTION);
            validateLong(options, SIZE_PENALTY_MB_OPTION);
            validateLong(options, MAX_CANDIDATE_MB_OPTION);

            unchecked.remove(ALPHA_OPTION);
            unchecked.remove(BETA_OPTION);
            unchecked.remove(GAMMA_OPTION);
            unchecked.remove(DELTA_OPTION);
            unchecked.remove(MIN_TRIGGER_SCORE_OPTION);
            unchecked.remove(TTL_HORIZON_SECONDS_OPTION);
            unchecked.remove(AGE_HORIZON_SECONDS_OPTION);
            unchecked.remove(SIZE_PENALTY_MB_OPTION);
            unchecked.remove(MAX_CANDIDATE_MB_OPTION);
            return unchecked;
        }

        private static double parsePositiveDouble(Map<String, String> options, String key, double defaultValue)
        {
            String value = options.get(key);
            if (value == null)
                return defaultValue;
            try
            {
                double parsed = Double.parseDouble(value);
                if (parsed <= 0)
                    throw new ConfigurationException(String.format("%s must be > 0", key));
                return parsed;
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a valid double", key), e);
            }
        }

        private static long parsePositiveLong(Map<String, String> options, String key, long defaultValue)
        {
            String value = options.get(key);
            if (value == null)
                return defaultValue;
            try
            {
                long parsed = Long.parseLong(value);
                if (parsed <= 0)
                    throw new ConfigurationException(String.format("%s must be > 0", key));
                return parsed;
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a valid long", key), e);
            }
        }

        private static void validateDouble(Map<String, String> options, String key) throws ConfigurationException
        {
            if (options.containsKey(key))
                parsePositiveDouble(options, key, 1.0d);
        }

        private static void validateLong(Map<String, String> options, String key) throws ConfigurationException
        {
            if (options.containsKey(key))
                parsePositiveLong(options, key, 1L);
        }

        private static double clampScore(double score)
        {
            if (score < 0.0d)
                return 0.0d;
            if (score > 1.0d)
                return 1.0d;
            return score;
        }

        private static long bytesToMb(long bytes)
        {
            return bytes / (1024L * 1024L);
        }

        private static long megabytesToBytes(long megabytes)
        {
            return megabytes * 1024L * 1024L;
        }
    }

    private static class SplittingCompactionTask extends CompactionTask
    {
        SplittingCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, long gcBefore)
        {
            super(cfs, txn, gcBefore);
        }

        @Override
        public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                              Directories directories,
                                                              LifecycleTransaction txn,
                                                              Set<SSTableReader> nonExpiredSSTables)
        {
            return new SplittingSizeTieredCompactionWriter(cfs, directories, txn, nonExpiredSSTables);
        }
    }
}
