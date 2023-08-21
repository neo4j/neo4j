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
package org.neo4j.consistency;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ConsistencyCheckMemoryCalculationTest {
    @Test
    void shouldKeepPageCacheMemoryIfEnoughMaxMemory() {
        // given
        var desiredPageCacheMemory = 1_000;
        var desiredOffHeapCachingMemory = 100;

        // when
        var distribution = ConsistencyCheckMemoryCalculation.calculate(
                10_000, desiredPageCacheMemory, desiredOffHeapCachingMemory);

        // then
        assertThat(distribution.pageCacheMemory()).isEqualTo(desiredPageCacheMemory);
        assertThat(distribution.offHeapCachingMemory()).isGreaterThanOrEqualTo(desiredOffHeapCachingMemory);
    }

    @Test
    void shouldConstrainPageCacheBarelyAboveLimit() {
        // given
        var desiredPageCacheMemory = 1_000;
        var desiredOffHeapCachingMemory = 100;
        var maxOffHeapMemory = 900;

        // when
        var distribution = ConsistencyCheckMemoryCalculation.calculate(
                maxOffHeapMemory, desiredPageCacheMemory, desiredOffHeapCachingMemory);

        // then
        assertThat(distribution.pageCacheMemory()).isEqualTo(maxOffHeapMemory - desiredOffHeapCachingMemory);
        assertThat(distribution.offHeapCachingMemory()).isEqualTo(desiredOffHeapCachingMemory);
    }

    @Test
    void shouldConstrainPageCacheMoreOnWayAboveLimit() {
        // given
        var desiredPageCacheMemory = 1_000;
        var desiredOffHeapCachingMemory = 100;
        var maxOffHeapMemory = 150;

        // when
        var distribution = ConsistencyCheckMemoryCalculation.calculate(
                maxOffHeapMemory, desiredPageCacheMemory, desiredOffHeapCachingMemory);

        // then
        assertThat(distribution.pageCacheMemory()).isLessThan(desiredPageCacheMemory);
        assertThat(distribution.offHeapCachingMemory()).isLessThan(desiredOffHeapCachingMemory);
    }
}
