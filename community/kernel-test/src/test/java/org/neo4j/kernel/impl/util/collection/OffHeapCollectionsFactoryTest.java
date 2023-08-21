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
package org.neo4j.kernel.impl.util.collection;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.io.ByteUnit;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryLimitExceededException;

class OffHeapCollectionsFactoryTest {

    @Test
    void shouldNotLeakNativeMemoryWhenAllocatingCloseToLimit() {
        var memoryTracker = new LocalMemoryTracker();
        var factory = new OffHeapCollectionsFactory(new CachingOffHeapBlockAllocator());
        memoryTracker.setLimit(ByteUnit.kibiBytes(512) + 1);
        // when
        Assertions.assertThatThrownBy(() -> factory.newObjectMap(memoryTracker))
                .isInstanceOf(MemoryLimitExceededException.class);
        factory.release();

        // then
        Assertions.assertThat(memoryTracker.usedNativeMemory()).isZero();
    }
}
