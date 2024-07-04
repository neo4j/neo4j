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
 * @see NumberArrayFactory#newDynamicIntArray(long, int, org.neo4j.memory.MemoryTracker)
 */
public class DynamicIntArray extends DynamicNumberArray<IntArray> implements IntArray {
    private final int defaultValue;
    private final MemoryTracker memoryTracker;

    public DynamicIntArray(NumberArrayFactory factory, long chunkSize, int defaultValue, MemoryTracker memoryTracker) {
        super(factory, chunkSize, new IntArray[0]);
        this.defaultValue = defaultValue;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public int get(long index) {
        IntArray chunk = chunkOrNullAt(index);
        return chunk != null ? chunk.get(index) : defaultValue;
    }

    @Override
    public void set(long index, int value) {
        at(index).set(index, value);
    }

    @Override
    public boolean compareAndSwap(long index, int expectedValue, int updatedValue) {
        return at(index).compareAndSwap(index, expectedValue, updatedValue);
    }

    @Override
    protected IntArray addChunk(long chunkSize, long base) {
        return factory.newIntArray(chunkSize, defaultValue, base, memoryTracker);
    }
}
