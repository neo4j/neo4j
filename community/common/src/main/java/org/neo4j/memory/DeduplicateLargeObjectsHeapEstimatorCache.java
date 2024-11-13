/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.memory;

import java.util.Arrays;
import org.neo4j.util.VisibleForTesting;

public class DeduplicateLargeObjectsHeapEstimatorCache implements HeapEstimatorCache {
    public static final int MAX_CACHE_HITS = 1_000_000;
    public static final boolean DEBUG_LOG_ENABLED = false;

    private final int sizeLimit;
    private final long largeObjectThreshold;
    private final Measurable[] cacheRefs;
    private final long[] cacheEstimates;
    private final int[] cacheHits;
    private int currentSize;

    DeduplicateLargeObjectsHeapEstimatorCache(HeapEstimatorCacheConfig config) {
        this(config.sizeLimit(), config.largeObjectThreshold());
    }

    DeduplicateLargeObjectsHeapEstimatorCache(int sizeLimit, long largeObjectThreshold) {
        this.sizeLimit = sizeLimit;
        this.largeObjectThreshold = largeObjectThreshold;
        this.cacheRefs = new Measurable[sizeLimit];
        this.cacheEstimates = new long[sizeLimit];
        this.cacheHits = new int[sizeLimit];
    }

    public DeduplicateLargeObjectsHeapEstimatorCache() {
        this(HeapEstimatorCacheConfig.DEFAULT);
    }

    @Override
    public long estimatedHeapUsage(Measurable measurable, long estimate) {
        if (estimate < largeObjectThreshold) {
            return estimate;
        }

        var index = find(measurable, estimate);
        if (index >= 0) {
            // We have already measured this object
            return 0L;
        }

        insert(measurable, estimate);
        return estimate;
    }

    @Override
    public void fastReset() {
        currentSize = 0;
    }

    @Override
    public void fullReset() {
        Arrays.fill(cacheRefs, 0, currentSize, null);
        currentSize = 0;
    }

    @Override
    public HeapEstimatorCache newWithSameSettings() {
        return new DeduplicateLargeObjectsHeapEstimatorCache(sizeLimit, largeObjectThreshold);
    }

    @Override
    public String toString() {
        return String.format(
                "%s:%08x,size:%d,thres:%d",
                getClass().getSimpleName(), System.identityHashCode(this), sizeLimit, largeObjectThreshold);
    }

    private int find(Measurable measurable, long estimate) {
        // Linear search
        for (int i = 0; i < currentSize; i++) {
            if (cacheRefs[i] == measurable) {
                var newHits = cacheHits[i] + 1;
                if (newHits < MAX_CACHE_HITS) {
                    cacheHits[i] = newHits;
                }
                if (DEBUG_LOG_ENABLED) {
                    log(
                            "[%s] Cache hit %s at %d with estimate %d: %d",
                            this, refString(measurable), i, estimate, newHits);
                }
                return i;
            }
        }
        return -1;
    }

    private void insert(Measurable measurable, long estimate) {
        var size = currentSize;
        if (size < sizeLimit) {
            cacheRefs[size] = measurable;
            cacheEstimates[size] = estimate;
            cacheHits[size] = 1;
            currentSize++;
            if (DEBUG_LOG_ENABLED) {
                log("[%s] Inserted  %s at %d with estimate %d", this, refString(measurable), size, estimate);
            }
        } else {
            evictAndReplace(measurable, estimate, size);
        }
    }

    private void evictAndReplace(Measurable measurable, long estimate, int size) {
        // Find eviction candidate, first by hits, then by largest estimate if tied
        // (We always insert the new element)
        // TODO: Consider size buckets by orders of magnitude to prioritize keeping very large object
        int minHitsIndex = 0;
        int minHits = cacheHits[0];
        long minHitsEstimate = cacheEstimates[0];
        for (int i = 1; i < size; i++) {
            var hits = cacheHits[i];
            if (hits < minHits) {
                minHitsIndex = i;
                minHits = hits;
                minHitsEstimate = cacheEstimates[i];
            } else if (hits == minHits) {
                var cachedEstimate = cacheEstimates[i];
                if (cachedEstimate < minHitsEstimate) {
                    minHitsIndex = i;
                    minHitsEstimate = cachedEstimate;
                }
            }
        }
        if (DEBUG_LOG_ENABLED) {
            log(
                    "[%s] Evicted   %s at %d with estimate %d",
                    this, refString(cacheRefs[minHitsIndex]), minHitsIndex, minHitsEstimate);
            log("[%s] Inserted  %s at %d with estimate %d", this, refString(measurable), minHitsIndex, estimate);
            if (estimate < minHitsEstimate) {
                log("[%s] ! Evicted object had larger estimate by %d", this, minHitsEstimate - estimate);
            }
        }
        // Replace the candidate
        cacheRefs[minHitsIndex] = measurable;
        cacheEstimates[minHitsIndex] = estimate;
        cacheHits[minHitsIndex] = 1;
    }

    @VisibleForTesting
    public static String refString(Measurable measurable) {
        return String.format("%s:%08x", measurable.getClass().getName(), System.identityHashCode(measurable));
    }

    private static void log(String format, Object... args) {
        if (DEBUG_LOG_ENABLED) {
            System.out.printf(format + System.lineSeparator(), args);
        }
    }
}
