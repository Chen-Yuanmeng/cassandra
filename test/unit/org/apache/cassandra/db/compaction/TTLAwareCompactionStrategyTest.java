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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.when;

public class TTLAwareCompactionStrategyTest
{
    private static final long ONE_MB = 1L << 20;

    private ColumnFamilyStore cfs;

    @BeforeClass
    public static void initClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        cfs = Mockito.mock(ColumnFamilyStore.class, Mockito.withSettings().defaultAnswer(RETURNS_SMART_NULLS));
        when(cfs.getDirectories()).thenReturn(Mockito.mock(Directories.class));
    }

    @Test
    public void testOptionsValidation() throws ConfigurationException
    {
        Map<String, String> valid = new HashMap<>();
        valid.put(TTLAwareCompactionStrategyOptions.ALPHA_KEY, "1.2");
        valid.put(TTLAwareCompactionStrategyOptions.BETA_KEY, "0.7");
        valid.put(TTLAwareCompactionStrategyOptions.GAMMA_KEY, "0.4");
        valid.put(TTLAwareCompactionStrategyOptions.DELTA_KEY, "0.2");
        assertTrue(TTLAwareCompactionStrategy.validateOptions(valid).isEmpty());

        Map<String, String> invalid = new HashMap<>(valid);
        invalid.put(TTLAwareCompactionStrategyOptions.BETA_KEY, "1.3");
        try
        {
            TTLAwareCompactionStrategy.validateOptions(invalid);
            fail("beta >= alpha should be rejected");
        }
        catch (ConfigurationException ignored)
        {
        }

        invalid.clear();
        invalid.put(TTLAwareCompactionStrategyOptions.MAX_COMPACTION_BYTES_KEY, "0");
        try
        {
            TTLAwareCompactionStrategy.validateOptions(invalid);
            fail("non positive max_compaction_bytes should be rejected");
        }
        catch (ConfigurationException ignored)
        {
        }
    }

    @Test
    public void testTombstonePriorityDominates()
    {
        TTLAwareCompactionStrategy strategy = newStrategy(Collections.emptyMap());
        long gcBefore = FBUtilities.nowInSeconds() - 10;

        SSTableReader tombHeavy = mockReader("tombHeavy", 0.9d, 64 * ONE_MB, 600, 1800);
        SSTableReader ttlSoon = mockReader("ttlSoon", 0.2d, 32 * ONE_MB, 5, 600);
        SSTableReader young = mockReader("young", 0.1d, 16 * ONE_MB, 3600, 60);

        List<SSTableReader> pick = strategy.pickForTesting(Arrays.asList(tombHeavy, ttlSoon, young), gcBefore, 2, 4);
        assertEquals(2, pick.size());
        assertEquals(tombHeavy, pick.get(0));
        assertTrue(pick.contains(ttlSoon));
    }

    @Test
    public void testTtlUrgencyElevatesExpiringTable()
    {
        TTLAwareCompactionStrategy strategy = newStrategy(Collections.emptyMap());
        long gcBefore = FBUtilities.nowInSeconds() - 5;

        SSTableReader expiringSoon = mockReader("expiringSoon", 0.15d, 8 * ONE_MB, 2, 1200);
        SSTableReader longLived = mockReader("longLived", 0.15d, 8 * ONE_MB, 7200, 3600);
        SSTableReader aged = mockReader("aged", 0.1d, 8 * ONE_MB, Long.MAX_VALUE, 7200);

        List<SSTableReader> pick = strategy.pickForTesting(Arrays.asList(expiringSoon, longLived, aged), gcBefore, 2, 3);
        assertEquals(expiringSoon, pick.get(0));
        assertTrue(pick.contains(longLived));
    }

    @Test
    public void testSizePenaltySkipsOversizedCandidates()
    {
        Map<String, String> opts = new HashMap<>();
        opts.put(TTLAwareCompactionStrategyOptions.MAX_COMPACTION_BYTES_KEY, Long.toString(64 * ONE_MB));
        TTLAwareCompactionStrategy strategy = newStrategy(opts);
        long gcBefore = FBUtilities.nowInSeconds() - 5;

        SSTableReader huge = mockReader("huge", 0.95d, 80 * ONE_MB, 10, 7200);
        SSTableReader huge2 = mockReader("huge2", 0.9d, 80 * ONE_MB, 10, 4200);
        SSTableReader compact = mockReader("compact", 0.6d, 16 * ONE_MB, 10, 3600);
        SSTableReader compact2 = mockReader("compact2", 0.55d, 12 * ONE_MB, 15, 3000);

        List<SSTableReader> pick = strategy.pickForTesting(Arrays.asList(huge, huge2, compact, compact2), gcBefore, 2, 4);
        assertEquals(Arrays.asList(compact, compact2), pick);
    }

    private TTLAwareCompactionStrategy newStrategy(Map<String, String> options)
    {
        return new TTLAwareCompactionStrategy(cfs, options);
    }

    private SSTableReader mockReader(String name,
                                     double droppableRatio,
                                     long sizeBytes,
                                     long secondsUntilExpiry,
                                     long ageSeconds)
    {
        long nowMillis = System.currentTimeMillis();
        long nowInSec = FBUtilities.nowInSeconds();

        SSTableReader reader = Mockito.mock(SSTableReader.class,
                                            Mockito.withSettings().name(name).defaultAnswer(RETURNS_SMART_NULLS));
        when(reader.bytesOnDisk()).thenReturn(sizeBytes);
        when(reader.getEstimatedDroppableTombstoneRatio(anyLong())).thenReturn(droppableRatio);
        long expiry = secondsUntilExpiry == Long.MAX_VALUE ? Long.MAX_VALUE : nowInSec + secondsUntilExpiry;
        when(reader.getMinLocalDeletionTime()).thenReturn(expiry);
        long creationMillis = nowMillis - TimeUnit.SECONDS.toMillis(ageSeconds);
        when(reader.getDataCreationTime()).thenReturn(creationMillis);
        when(reader.getMaxTimestamp()).thenReturn(TimeUnit.MILLISECONDS.toMicros(Math.max(0L, creationMillis)));
        when(reader.isMarkedSuspect()).thenReturn(false);
        return reader;
    }
}
