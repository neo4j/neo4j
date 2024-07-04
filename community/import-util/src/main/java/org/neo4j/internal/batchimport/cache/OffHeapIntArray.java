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

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.MemoryTracker;

/**
 * Off-heap version of {@link IntArray} using {@code sun.misc.Unsafe}. Supports arrays with length beyond
 * Integer.MAX_VALUE.
 */
public class OffHeapIntArray extends OffHeapRegularNumberArray<IntArray> implements IntArray {
    private final int defaultValue;

    public OffHeapIntArray(long length, int defaultValue, long base, MemoryTracker memoryTracker) {
        super(length, 2, base, memoryTracker);
        this.defaultValue = defaultValue;
        clear();
    }

    @Override
    public int get(long index) {
        return UnsafeUtil.getInt(addressOf(index));
    }

    @Override
    public void set(long index, int value) {
        UnsafeUtil.putInt(addressOf(index), value);
    }

    @Override
    public boolean compareAndSwap(long index, int expectedValue, int updatedValue) {
        return UnsafeUtil.compareAndSwapInt(null, addressOf(index), expectedValue, updatedValue);
    }

    @Override
    public void clear() {
        if (isByteUniform(defaultValue)) {
            UnsafeUtil.setMemory(address, length << shift, (byte) defaultValue);
        } else {
            for (long i = 0, adr = address; i < length; i++, adr += itemSize) {
                UnsafeUtil.putInt(adr, defaultValue);
            }
        }
    }
}
