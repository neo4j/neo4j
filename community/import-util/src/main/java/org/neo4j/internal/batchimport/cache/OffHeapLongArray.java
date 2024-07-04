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
 * Off-heap version of {@link LongArray} using {@code sun.misc.Unsafe}. Supports arrays with length beyond
 * Integer.MAX_VALUE.
 */
public class OffHeapLongArray extends OffHeapRegularNumberArray<LongArray> implements LongArray {
    private final long defaultValue;

    public OffHeapLongArray(long length, long defaultValue, long base, MemoryTracker memoryTracker) {
        super(length, 3, base, memoryTracker);
        this.defaultValue = defaultValue;
        clear();
    }

    @Override
    public long get(long index) {
        return UnsafeUtil.getLong(addressOf(index));
    }

    @Override
    public void set(long index, long value) {
        UnsafeUtil.putLong(addressOf(index), value);
    }

    @Override
    public boolean compareAndSwap(long index, long expectedValue, long updatedValue) {
        return UnsafeUtil.compareAndSwapLong(null, addressOf(index), expectedValue, updatedValue);
    }

    @Override
    public void clear() {
        if (isByteUniform(defaultValue)) {
            UnsafeUtil.setMemory(address, length << shift, (byte) defaultValue);
        } else {
            for (long i = 0, adr = address; i < length; i++, adr += itemSize) {
                UnsafeUtil.putLong(adr, defaultValue);
            }
        }
    }

    public long getAndAdd(long index, long delta) {
        return UnsafeUtil.getAndAddLong(null, addressOf(index), delta);
    }
}
