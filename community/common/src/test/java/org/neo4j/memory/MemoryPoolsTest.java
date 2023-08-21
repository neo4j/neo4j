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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.memory.MemoryGroup.OTHER;
import static org.neo4j.memory.MemoryGroup.TRANSACTION;

import org.junit.jupiter.api.Test;

class MemoryPoolsTest {

    @Test
    void createdPoolRegisteredInListOfPools() {
        var pools = new MemoryPools();
        var pool1 = pools.pool(OTHER, 2, true, null);
        var pool2 = pools.pool(TRANSACTION, 2, true, null);
        assertThat(pools.getPools()).contains(pool1, pool2);
    }

    @Test
    void poolsWithDisabledMemoryTracking() {
        var pools = new MemoryPools(false);
        var pool = pools.pool(TRANSACTION, 2, true, null);

        assertEquals(0, pool.usedNative());
        assertEquals(0, pool.usedHeap());
        assertEquals(0, pool.totalUsed());

        var memoryTracker = pool.getPoolMemoryTracker();
        memoryTracker.allocateHeap(100);
        memoryTracker.allocateNative(1000);

        assertEquals(0, pool.usedNative());
        assertEquals(0, pool.usedHeap());
        assertEquals(0, pool.totalUsed());
    }

    @Test
    void subpoolWithDisabledMemoryTracking() {
        var pools = new MemoryPools(false);
        var pool = pools.pool(TRANSACTION, 2, true, null).newDatabasePool("test", 1, null);

        assertEquals(0, pool.usedNative());
        assertEquals(0, pool.usedHeap());
        assertEquals(0, pool.totalUsed());

        var memoryTracker = pool.getPoolMemoryTracker();
        memoryTracker.allocateHeap(100);
        memoryTracker.allocateNative(1000);

        assertEquals(0, pool.usedNative());
        assertEquals(0, pool.usedHeap());
        assertEquals(0, pool.totalUsed());
    }

    @Test
    void poolIsDeregisteredOnClose() {
        var pools = new MemoryPools();
        var pool = pools.pool(TRANSACTION, 2, true, null);

        assertThat(pools.getPools()).contains(pool);
        pool.close();
        assertThat(pools.getPools()).isEmpty();
    }

    @Test
    void registerAndDeregisterExternalPool() {
        var pools = new MemoryPools();
        assertThat(pools.getPools()).isEmpty();

        var externalPool = new GlobalMemoryGroupTracker(pools, MemoryGroup.NO_TRACKING, 0, true, true, null);
        pools.registerPool(externalPool);

        assertThat(pools.getPools()).hasSize(1);
        assertThat(pools.getPools()).contains(externalPool);

        pools.unregisterPool(externalPool);
        assertThat(pools.getPools()).isEmpty();
    }
}
