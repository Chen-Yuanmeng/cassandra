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
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class TTLAwareCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(TTLAwareCompactionStrategy.class);

    private static final Comparator<ScoredSSTable> SCORE_COMPARATOR =
    Comparator.comparingDouble((ScoredSSTable s) -> s.score).reversed()
              .thenComparingLong(s -> s.sstable.bytesOnDisk());

    private final Set<SSTableReader> sstables = new HashSet<>();
    private final TTLAwareCompactionStrategyOptions options;
    private volatile int estimatedRemainingTasks;

    public TTLAwareCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.options = new TTLAwareCompactionStrategyOptions(options);
        this.estimatedRemainingTasks = 0;
    }

    @Override
    public AbstractCompactionTask getNextBackgroundTask(long gcBefore)
    {
        if (!isActive)
            return null;

        List<SSTableReader> previous = null;
        while (true)
        {
            List<SSTableReader> next = getNextBackgroundSSTables(gcBefore);
            if (next.isEmpty())
                return null;

            if (next.equals(previous))
            {
                logger.warn("Could not acquire references for {}. Will retry later.", next);
                return null;
            }

            LifecycleTransaction txn = cfs.getTracker().tryModify(next, OperationType.COMPACTION);
            if (txn != null)
                return new CompactionTask(cfs, txn, gcBefore);

            previous = next;
        }
    }

    private synchronized List<SSTableReader> getNextBackgroundSSTables(long gcBefore)
    {
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        if (minThreshold <= 1 || maxThreshold <= 0)
            return Collections.emptyList();

        Iterable<SSTableReader> uncompacting = cfs.getUncompactingSSTables();
        List<SSTableReader> candidates = new ArrayList<>();
        for (SSTableReader sstable : filterSuspectSSTables(uncompacting))
        {
            if (sstables.contains(sstable))
                candidates.add(sstable);
        }

        if (candidates.isEmpty())
        {
            estimatedRemainingTasks = 0;
            cfs.getCompactionStrategyManager().compactionLogger.pending(this, estimatedRemainingTasks);
            return Collections.emptyList();
        }

        List<ScoredSSTable> scored = scoreCandidates(candidates, gcBefore);
        estimatedRemainingTasks = estimateRemaining(scored, minThreshold, maxThreshold);
        cfs.getCompactionStrategyManager().compactionLogger.pending(this, estimatedRemainingTasks);

        List<SSTableReader> prioritized = selectBest(scored, minThreshold, maxThreshold);
        if (!prioritized.isEmpty())
            return prioritized;

        List<SSTableReader> tombstoneTargets = new ArrayList<>();
        for (SSTableReader sstable : candidates)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                tombstoneTargets.add(sstable);
        }

        if (tombstoneTargets.isEmpty())
            return Collections.emptyList();

        SSTableReader largest = Collections.max(tombstoneTargets, SSTableReader.sizeComparator);
        return Collections.singletonList(largest);
    }

    private int estimateRemaining(List<ScoredSSTable> scored, int minThreshold, int maxThreshold)
    {
        if (maxThreshold <= 0)
            return 0;

        int eligible = 0;
        for (ScoredSSTable entry : scored)
        {
            if (entry.score >= options.minScoreThreshold)
                eligible++;
        }

        if (eligible < minThreshold)
            return 0;

        return (int) Math.ceil((double) eligible / Math.max(1, maxThreshold));
    }

    private List<ScoredSSTable> scoreCandidates(Collection<SSTableReader> candidates, long gcBefore)
    {
        List<ScoredSSTable> scored = new ArrayList<>(candidates.size());
        long nowInSec = FBUtilities.nowInSeconds();
        long nowMillis = currentTimeMillis();

        for (SSTableReader sstable : candidates)
        {
            double tombstoneWeight = clamp01(sstable.getEstimatedDroppableTombstoneRatio(gcBefore));
            double ttlUrgency = ttlUrgency(sstable, nowInSec);
            double ageFactor = ageFactor(sstable, nowMillis);
            double sizePenalty = sizePenalty(sstable);

            double score = options.alpha * tombstoneWeight
                         + options.beta * ttlUrgency
                         + options.gamma * ageFactor
                         - options.delta * sizePenalty;

            if (!Double.isFinite(score))
                score = Double.NEGATIVE_INFINITY;

            scored.add(new ScoredSSTable(sstable, score, tombstoneWeight, ttlUrgency, ageFactor, sizePenalty));
        }

        if (logger.isTraceEnabled())
        {
            for (ScoredSSTable entry : scored)
            {
                logger.trace("Score for {} => score={}, tombstones={}, ttl={}, age={}, sizePenalty={}",
                             entry.sstable,
                             entry.score,
                             entry.tombstoneDensity,
                             entry.ttlUrgency,
                             entry.ageFactor,
                             entry.sizePenalty);
            }
        }

        return scored;
    }

    private List<SSTableReader> selectBest(List<ScoredSSTable> scored, int minThreshold, int maxThreshold)
    {
        int target = Math.min(maxThreshold, Math.max(2, minThreshold));
        if (scored.isEmpty() || target <= 1)
            return Collections.emptyList();

        scored.sort(SCORE_COMPARATOR);
        List<SSTableReader> selected = new ArrayList<>();
        long totalBytes = 0L;

        for (ScoredSSTable candidate : scored)
        {
            if (candidate.score < options.minScoreThreshold)
                continue;

            long size = Math.max(0L, candidate.sstable.bytesOnDisk());
            if (size > options.maxCompactionBytes)
                continue;

            if (totalBytes + size > options.maxCompactionBytes)
                continue;

            selected.add(candidate.sstable);
            totalBytes += size;

            if (selected.size() == target)
                break;
        }

        return selected.size() == target ? selected : Collections.emptyList();
    }

    private double ttlUrgency(SSTableReader sstable, long nowInSec)
    {
        long minDeletion = sstable.getMinLocalDeletionTime();
        if (minDeletion == Integer.MAX_VALUE || minDeletion == Cell.NO_DELETION_TIME)
            return 0d;

        long secondsUntilPurge = (long) minDeletion - nowInSec;
        if (secondsUntilPurge <= 0)
            return 1d;
        if (secondsUntilPurge >= options.ttlHorizonSeconds)
            return 0d;

        return 1d - (double) secondsUntilPurge / options.ttlHorizonSeconds;
    }

    private double ageFactor(SSTableReader sstable, long nowMillis)
    {
        long birthMillis = sstable.getDataCreationTime();
        if (birthMillis <= 0L)
        {
            long timestampMicros = sstable.getMaxTimestamp();
            if (timestampMicros > 0L)
                birthMillis = TimeUnit.MICROSECONDS.toMillis(timestampMicros);
        }

        if (birthMillis <= 0L)
            return 0d;

        long ageSeconds = Math.max(0L, (nowMillis - birthMillis) / 1000L);
        if (ageSeconds <= 0L)
            return 0d;
        if (ageSeconds >= options.ageHorizonSeconds)
            return 1d;

        return (double) ageSeconds / options.ageHorizonSeconds;
    }

    private double sizePenalty(SSTableReader sstable)
    {
        long size = Math.max(0L, sstable.bytesOnDisk());
        double normalized = (double) size / options.maxCompactionBytes;
        return clamp01(normalized);
    }

    private static double clamp01(double value)
    {
        if (!Double.isFinite(value) || value <= 0d)
            return 0d;
        return value >= 1d ? 1d : value;
    }

    @Override
    public synchronized void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

    @Override
    public synchronized void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }

    @Override
    protected synchronized Set<SSTableReader> getSSTables()
    {
        return ImmutableSet.copyOf(sstables);
    }

    @Override
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(long gcBefore, boolean splitOutput)
    {
        Iterable<SSTableReader> filtered = filterSuspectSSTables(sstables);
        if (Iterables.isEmpty(filtered))
            return null;

        LifecycleTransaction txn = cfs.getTracker().tryModify(filtered, OperationType.COMPACTION);
        if (txn == null)
            return null;

        return Collections.singletonList(new CompactionTask(cfs, txn, gcBefore));
    }

    @Override
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, long gcBefore)
    {
        assert !sstables.isEmpty();
        LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (txn == null)
            return null;
        return new CompactionTask(cfs, txn, gcBefore).setUserDefined(true);
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return options.maxCompactionBytes;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> unchecked = AbstractCompactionStrategy.validateOptions(options);
        unchecked = TTLAwareCompactionStrategyOptions.validateOptions(options, unchecked);
        unchecked.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        unchecked.remove(CompactionParams.Option.MAX_THRESHOLD.toString());
        return unchecked;
    }

    @VisibleForTesting
    List<SSTableReader> pickForTesting(Collection<SSTableReader> candidates, long gcBefore, int minThreshold, int maxThreshold)
    {
        return selectBest(scoreCandidates(candidates, gcBefore), minThreshold, maxThreshold);
    }

    private static final class ScoredSSTable
    {
        final SSTableReader sstable;
        final double score;
        final double tombstoneDensity;
        final double ttlUrgency;
        final double ageFactor;
        final double sizePenalty;

        ScoredSSTable(SSTableReader sstable,
                      double score,
                      double tombstoneDensity,
                      double ttlUrgency,
                      double ageFactor,
                      double sizePenalty)
        {
            this.sstable = sstable;
            this.score = score;
            this.tombstoneDensity = tombstoneDensity;
            this.ttlUrgency = ttlUrgency;
            this.ageFactor = ageFactor;
            this.sizePenalty = sizePenalty;
        }
    }

    @Override
    public String toString()
    {
        return String.format("TTLAwareCompactionStrategy[tasks=%d]", estimatedRemainingTasks);
    }
}
