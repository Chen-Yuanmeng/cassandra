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

import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;

final class TTLAwareCompactionStrategyOptions
{
    static final String ALPHA_KEY = "alpha";
    static final String BETA_KEY = "beta";
    static final String GAMMA_KEY = "gamma";
    static final String DELTA_KEY = "delta";
    static final String TTL_HORIZON_SECONDS_KEY = "ttl_horizon_seconds";
    static final String AGE_HORIZON_SECONDS_KEY = "age_horizon_seconds";
    static final String MAX_COMPACTION_BYTES_KEY = "max_compaction_bytes";
    static final String MIN_SCORE_THRESHOLD_KEY = "min_score_threshold";

    static final double DEFAULT_ALPHA = 1.0d;
    static final double DEFAULT_BETA = 0.6d;
    static final double DEFAULT_GAMMA = 0.35d;
    static final double DEFAULT_DELTA = 0.25d;
    static final long DEFAULT_TTL_HORIZON_SECONDS = 24 * 3600L;    // 1 day
    static final long DEFAULT_AGE_HORIZON_SECONDS = 7 * 24 * 3600L; // 1 week
    static final long DEFAULT_MAX_COMPACTION_BYTES = 8L << 30;       // 8 GiB
    static final double DEFAULT_MIN_SCORE_THRESHOLD = 0.05d;

    final double alpha;
    final double beta;
    final double gamma;
    final double delta;
    final long ttlHorizonSeconds;
    final long ageHorizonSeconds;
    final long maxCompactionBytes;
    final double minScoreThreshold;

    TTLAwareCompactionStrategyOptions(Map<String, String> options)
    {
        this.alpha = parsePositiveDouble(options, ALPHA_KEY, DEFAULT_ALPHA);
        this.beta = parsePositiveDouble(options, BETA_KEY, DEFAULT_BETA);
        this.gamma = parsePositiveDouble(options, GAMMA_KEY, DEFAULT_GAMMA);
        this.delta = parsePositiveDouble(options, DELTA_KEY, DEFAULT_DELTA);
        validateWeightOrdering(alpha, beta, gamma);

        this.ttlHorizonSeconds = parsePositiveLong(options, TTL_HORIZON_SECONDS_KEY, DEFAULT_TTL_HORIZON_SECONDS);
        this.ageHorizonSeconds = parsePositiveLong(options, AGE_HORIZON_SECONDS_KEY, DEFAULT_AGE_HORIZON_SECONDS);
        this.maxCompactionBytes = parsePositiveLong(options, MAX_COMPACTION_BYTES_KEY, DEFAULT_MAX_COMPACTION_BYTES);
        this.minScoreThreshold = parseNonNegativeDouble(options, MIN_SCORE_THRESHOLD_KEY, DEFAULT_MIN_SCORE_THRESHOLD);
    }

    static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws ConfigurationException
    {
        parsePositiveDouble(options, ALPHA_KEY, DEFAULT_ALPHA);
        parsePositiveDouble(options, BETA_KEY, DEFAULT_BETA);
        parsePositiveDouble(options, GAMMA_KEY, DEFAULT_GAMMA);
        parsePositiveDouble(options, DELTA_KEY, DEFAULT_DELTA);
        validateWeightOrdering(parseValue(options, ALPHA_KEY, DEFAULT_ALPHA),
                               parseValue(options, BETA_KEY, DEFAULT_BETA),
                               parseValue(options, GAMMA_KEY, DEFAULT_GAMMA));

        parsePositiveLong(options, TTL_HORIZON_SECONDS_KEY, DEFAULT_TTL_HORIZON_SECONDS);
        parsePositiveLong(options, AGE_HORIZON_SECONDS_KEY, DEFAULT_AGE_HORIZON_SECONDS);
        parsePositiveLong(options, MAX_COMPACTION_BYTES_KEY, DEFAULT_MAX_COMPACTION_BYTES);
        parseNonNegativeDouble(options, MIN_SCORE_THRESHOLD_KEY, DEFAULT_MIN_SCORE_THRESHOLD);

        uncheckedOptions.remove(ALPHA_KEY);
        uncheckedOptions.remove(BETA_KEY);
        uncheckedOptions.remove(GAMMA_KEY);
        uncheckedOptions.remove(DELTA_KEY);
        uncheckedOptions.remove(TTL_HORIZON_SECONDS_KEY);
        uncheckedOptions.remove(AGE_HORIZON_SECONDS_KEY);
        uncheckedOptions.remove(MAX_COMPACTION_BYTES_KEY);
        uncheckedOptions.remove(MIN_SCORE_THRESHOLD_KEY);
        return uncheckedOptions;
    }

    private static void validateWeightOrdering(double alpha, double beta, double gamma) throws ConfigurationException
    {
        if (!(alpha > beta && beta > gamma))
            throw new ConfigurationException(String.format("Weights must satisfy alpha > beta > gamma (got alpha=%s, beta=%s, gamma=%s)", alpha, beta, gamma));
    }

    private static double parsePositiveDouble(Map<String, String> options, String key, double defaultValue) throws ConfigurationException
    {
        double value = parseValue(options, key, defaultValue);
        if (value <= 0d || Double.isNaN(value))
            throw new ConfigurationException(String.format("%s must be > 0", key));
        return value;
    }

    private static double parseNonNegativeDouble(Map<String, String> options, String key, double defaultValue) throws ConfigurationException
    {
        double value = parseValue(options, key, defaultValue);
        if (value < 0d || Double.isNaN(value))
            throw new ConfigurationException(String.format("%s must be >= 0", key));
        return value;
    }

    private static double parseValue(Map<String, String> options, String key, double defaultValue) throws ConfigurationException
    {
        String raw = options.get(key);
        if (raw == null)
            return defaultValue;
        try
        {
            return Double.parseDouble(raw);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a valid double for %s", raw, key), e);
        }
    }

    private static long parsePositiveLong(Map<String, String> options, String key, long defaultValue) throws ConfigurationException
    {
        String raw = options.get(key);
        long value = defaultValue;
        if (raw != null)
        {
            try
            {
                value = Long.parseLong(raw);
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a valid long for %s", raw, key), e);
            }
        }

        if (value <= 0L)
            throw new ConfigurationException(String.format("%s must be > 0", key));
        return value;
    }
}
