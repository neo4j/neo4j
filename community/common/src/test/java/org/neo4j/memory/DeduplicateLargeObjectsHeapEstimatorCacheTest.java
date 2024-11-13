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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeduplicateLargeObjectsHeapEstimatorCacheTest {

    private DeduplicateLargeObjectsHeapEstimatorCache cache;
    private static final long LARGE_OBJECT_THRESHOLD = HeapEstimatorCacheConfig.DEFAULT_LARGE_OBJECT_THRESHOLD;

    @BeforeEach
    void setUp() {
        cache = new DeduplicateLargeObjectsHeapEstimatorCache();
    }

    @Test
    void testEstimateBelowThreshold() {
        Measurable smallObject = new TestMeasurable();
        long estimate = 13L;
        long result1 = cache.estimatedHeapUsage(smallObject, estimate);
        assertEquals(estimate, result1, "Estimate below threshold should be returned as is");
        long result2 = cache.estimatedHeapUsage(smallObject, estimate);
        assertEquals(estimate, result2, "Estimate below threshold should be returned as is");
    }

    @Test
    void testEstimateAboveThresholdCacheHit() {
        Measurable largeObject = new TestMeasurable();
        long estimate = LARGE_OBJECT_THRESHOLD;
        long result1 = cache.estimatedHeapUsage(largeObject, estimate);
        assertEquals(estimate, result1, "Estimate above threshold should be returned as is on first insertion");
        long result2 = cache.estimatedHeapUsage(largeObject, estimate);
        assertEquals(0L, result2, "Estimate above threshold should be returned as 0 on cache hit");
        long result3 = cache.estimatedHeapUsage(largeObject, estimate);
        assertEquals(0L, result3, "Estimate above threshold should be returned as 0 on cache hit");
    }

    @Test
    void testEvictionOfSmallestEstimatedObjectWithLeastCacheHits() {
        int sizeLimit = 3;
        DeduplicateLargeObjectsHeapEstimatorCache smallCache =
                new DeduplicateLargeObjectsHeapEstimatorCache(sizeLimit, LARGE_OBJECT_THRESHOLD);
        Measurable object1 = new TestMeasurable(30L * LARGE_OBJECT_THRESHOLD);
        Measurable object2 = new TestMeasurable(10L * LARGE_OBJECT_THRESHOLD);
        Measurable object3 = new TestMeasurable(20L * LARGE_OBJECT_THRESHOLD);
        Measurable object4 = new TestMeasurable(25L * LARGE_OBJECT_THRESHOLD);

        smallCache.estimatedHeapUsage(object1, object1.estimatedHeapUsage());
        smallCache.estimatedHeapUsage(object2, object2.estimatedHeapUsage());
        smallCache.estimatedHeapUsage(object3, object3.estimatedHeapUsage());

        // All objects have the same number of cache hits
        smallCache.estimatedHeapUsage(object1, object1.estimatedHeapUsage());
        smallCache.estimatedHeapUsage(object2, object2.estimatedHeapUsage());
        smallCache.estimatedHeapUsage(object3, object3.estimatedHeapUsage());

        // Insert a new object which should cause eviction of the smallest estimated object (object2)
        smallCache.estimatedHeapUsage(object4, object4.estimatedHeapUsage());

        long result1 = smallCache.estimatedHeapUsage(object1, object1.estimatedHeapUsage());
        assertEquals(0L, result1, "Object1 should still be in cache");
        long result2 = smallCache.estimatedHeapUsage(object2, object2.estimatedHeapUsage());
        assertNotEquals(0L, result2, "Object2 should have been evicted as it had the smallest estimate");
        long result3 = smallCache.estimatedHeapUsage(object3, object3.estimatedHeapUsage());
        assertEquals(0L, result3, "Object3 should still be in cache");
        long result4 = smallCache.estimatedHeapUsage(object4, object4.estimatedHeapUsage());
        // NOTE: Cache hits takes precedence, so even though object4 is larger than object3 it has fewer hits
        assertNotEquals(0L, result4, "Object4 should have been evicted as it had lower hits");

        // Add 2 hits to object4 to make it eaual
        smallCache.estimatedHeapUsage(object4, object4.estimatedHeapUsage());
        smallCache.estimatedHeapUsage(object4, object4.estimatedHeapUsage());

        // Re-insert object 2 => object3 should be evicted as it has a smaller estimate than object4
        long result5 = smallCache.estimatedHeapUsage(object2, object2.estimatedHeapUsage());
        assertNotEquals(0L, result5, "Object2 should already have been evicted");

        long result6 = smallCache.estimatedHeapUsage(object3, object3.estimatedHeapUsage());
        assertNotEquals(0L, result6, "Object3 should have been evicted as it had the smallest estimate");
    }

    @Test
    void testCacheEvictionOrderOfEqualSizedObjects() {
        // NOTE: The eviction order here is not important, but this codifies the current eviction order
        int sizeLimit = 2;
        DeduplicateLargeObjectsHeapEstimatorCache smallCache =
                new DeduplicateLargeObjectsHeapEstimatorCache(sizeLimit, LARGE_OBJECT_THRESHOLD);
        Measurable object1 = new TestMeasurable();
        Measurable object2 = new TestMeasurable();
        Measurable object3 = new TestMeasurable();
        long estimate = LARGE_OBJECT_THRESHOLD;

        smallCache.estimatedHeapUsage(object1, estimate);
        smallCache.estimatedHeapUsage(object2, estimate);
        smallCache.estimatedHeapUsage(object3, estimate); // Evicts object 1, as eviction picks the first slot first

        long result1 = smallCache.estimatedHeapUsage(object1, estimate); // Evicts object 3, as it replaced object 1
        assertNotEquals(0L, result1, "Object1 should have been evicted before");

        long result2 = smallCache.estimatedHeapUsage(object2, estimate);
        assertEquals(0L, result2, "Object2 should still be in cache");

        long result3 = smallCache.estimatedHeapUsage(object3, estimate); // Evicts object 1, as it replaced object 3
        assertNotEquals(0L, result3, "Object3 should have been evicted before");
    }

    private static class TestMeasurable implements Measurable {
        private final long estimatedHeapUsage;

        public TestMeasurable() {
            this.estimatedHeapUsage = 1337L;
        }

        public TestMeasurable(long estimatedHeapUsage) {
            this.estimatedHeapUsage = estimatedHeapUsage;
        }

        @Override
        public long estimatedHeapUsage() {
            return estimatedHeapUsage;
        }
    }
}
