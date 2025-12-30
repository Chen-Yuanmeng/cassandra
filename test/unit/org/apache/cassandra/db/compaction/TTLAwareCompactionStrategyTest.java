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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.MockSchema;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TTLAwareCompactionStrategyTest
{
    @BeforeClass
    public static void setup() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void testOptionsValidation() throws ConfigurationException
    {
        Map<String, String> valid = new HashMap<>();
        valid.put("alpha", "1.2");
        valid.put("beta", "0.8");
        valid.put("gamma", "0.3");
        Map<String, String> unchecked = TTLAwareCompactionStrategy.validateOptions(valid);
        assertTrue(unchecked.isEmpty());

        Map<String, String> invalid = new HashMap<>();
        invalid.put("alpha", "0.1");
        invalid.put("beta", "0.9");
        invalid.put("gamma", "0.2");
        ColumnFamilyStore cfs = MockSchema.newCFS(builder -> builder.compaction(CompactionParams.create(TTLAwareCompactionStrategy.class, Collections.emptyMap())));
        try
        {
            new TTLAwareCompactionStrategy(cfs, invalid);
            fail("Expected alpha > beta > gamma validation to trigger");
        }
        catch (ConfigurationException ignore)
        {
            // expected
        }

        valid.put("bogus", "123");
        unchecked = TTLAwareCompactionStrategy.validateOptions(valid);
        assertTrue(unchecked.containsKey("bogus"));
    }

    @Test
    public void testScorePrefersExpiringAndPenalizesLargeTables() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put("ttl_horizon_seconds", Long.toString(TimeUnit.MINUTES.toSeconds(30)));
        options.put("age_horizon_seconds", Long.toString(TimeUnit.HOURS.toSeconds(6)));
        options.put("size_penalty_target_mb", "1");
        options.put("max_candidate_bytes_mb", "4");

        ColumnFamilyStore cfs = MockSchema.newCFS(builder -> builder.compaction(CompactionParams.create(TTLAwareCompactionStrategy.class, options)));
        TTLAwareCompactionStrategy strategy = new TTLAwareCompactionStrategy(cfs, options);

        long nowMillis = currentTimeMillis();
        long nowMicros = TimeUnit.MILLISECONDS.toMicros(nowMillis);
        long gcBefore = TimeUnit.MILLISECONDS.toSeconds(nowMillis);

        int urgentExpiry = (int) TimeUnit.MILLISECONDS.toSeconds(nowMillis + TimeUnit.MINUTES.toMillis(1));
        int relaxedExpiry = (int) TimeUnit.MILLISECONDS.toSeconds(nowMillis + TimeUnit.HOURS.toMillis(1));

        SSTableReader urgent = MockSchema.sstable(1,
                                                  256 * 1024,
                                                  false,
                                                  0,
                                                  100,
                                                  0,
                                                  cfs,
                                                  urgentExpiry,
                                                  nowMicros - TimeUnit.SECONDS.toMicros(5));

        SSTableReader relaxed = MockSchema.sstable(2,
                                                   256 * 1024,
                                                   false,
                                                   101,
                                                   200,
                                                   0,
                                                   cfs,
                                                   relaxedExpiry,
                                                   nowMicros - TimeUnit.MINUTES.toMicros(30));

        double urgentScore = invokeScore(strategy, urgent, gcBefore);
        double relaxedScore = invokeScore(strategy, relaxed, gcBefore);
        assertTrue("SSTable nearing TTL expiry should score higher", urgentScore > relaxedScore);

        SSTableReader large = MockSchema.sstable(3,
                                                 4 * 1024 * 1024,
                                                 false,
                                                 201,
                                                 400,
                                                 0,
                                                 cfs,
                                                 relaxedExpiry,
                                                 nowMicros - TimeUnit.MINUTES.toMicros(30));
        double largeScore = invokeScore(strategy, large, gcBefore);
        assertTrue("Large SSTable should incur a higher size penalty", relaxedScore > largeScore);
    }

    private static double invokeScore(TTLAwareCompactionStrategy strategy, SSTableReader reader, long gcBefore)
    {
        try
        {
            Method score = TTLAwareCompactionStrategy.class.getDeclaredMethod("score", SSTableReader.class, long.class);
            score.setAccessible(true);
            return (double) score.invoke(strategy, reader, gcBefore);
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e)
        {
            throw new AssertionError("Unable to invoke TTLAwareCompactionStrategy.score", e);
        }
    }
}
