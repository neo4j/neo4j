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
package org.neo4j.internal.batchimport.cache;

import org.neo4j.memory.MemoryTracker;

/**
 * Dynamically growing {@link LongArray}. Is given a chunk size and chunks are added as higher and higher
 * items are requested.
 *
 * @see NumberArrayFactory#newDynamicLongArray(long, long, MemoryTracker)
 */
public class DynamicLongArray extends DynamicNumberArray<LongArray> implements LongArray {
    private final long defaultValue;
    private final MemoryTracker memoryTracker;

    public DynamicLongArray(
            NumberArrayFactory factory, long chunkSize, long defaultValue, MemoryTracker memoryTracker) {
        super(factory, chunkSize, new LongArray[0]);
        this.defaultValue = defaultValue;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public long get(long index) {
        LongArray chunk = chunkOrNullAt(index);
        return chunk != null ? chunk.get(index) : defaultValue;
    }

    @Override
    public void set(long index, long value) {
        at(index).set(index, value);
    }

    @Override
    public boolean compareAndSwap(long index, long expectedValue, long updatedValue) {
        return at(index).compareAndSwap(index, expectedValue, updatedValue);
    }

    @Override
    protected LongArray addChunk(long chunkSize, long base) {
        return factory.newLongArray(chunkSize, defaultValue, base, memoryTracker);
    }
}
