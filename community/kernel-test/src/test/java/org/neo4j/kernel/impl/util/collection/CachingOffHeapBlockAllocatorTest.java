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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.kernel.impl.util.collection.OffHeapBlockAllocator.MemoryBlock;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;

class CachingOffHeapBlockAllocatorTest {
    private static final int CACHE_SIZE = 4;
    private static final int MAX_CACHEABLE_BLOCK_SIZE = 128;

    private final MemoryTracker memoryTracker = spy(new LocalMemoryTracker());
    private final CachingOffHeapBlockAllocator allocator =
            new CachingOffHeapBlockAllocator(MAX_CACHEABLE_BLOCK_SIZE, CACHE_SIZE);

    @AfterEach
    void afterEach() {
        allocator.release();
        assertEquals(0, memoryTracker.usedNativeMemory(), "Native memory is leaking");
    }

    @Test
    void allocateAfterRelease() {
        allocator.release();
        assertThrows(IllegalStateException.class, () -> allocator.allocate(128, memoryTracker));
    }

    @Test
    void freeAfterRelease() {
        final MemoryBlock block = allocator.allocate(128, memoryTracker);
        allocator.release();
        allocator.free(block, memoryTracker);
        verify(memoryTracker).allocateNative(eq(128L));
        verify(memoryTracker).releaseNative(eq(128L));
    }

    @Test
    void allocateAndFree() {
        final MemoryBlock block1 = allocator.allocate(128, memoryTracker);
        assertEquals(128, block1.size);
        assertEquals(128, block1.size);
        assertEquals(block1.size, memoryTracker.usedNativeMemory());

        final MemoryBlock block2 = allocator.allocate(256, memoryTracker);
        assertEquals(256, block2.size);
        assertEquals(256, block2.size);
        assertEquals(block1.size + block2.size, memoryTracker.usedNativeMemory());

        allocator.free(block1, memoryTracker);
        allocator.free(block2, memoryTracker);
        assertEquals(0, memoryTracker.usedNativeMemory());
    }

    @ParameterizedTest
    @ValueSource(longs = {10, 100, 256})
    void allocateNonCacheableSize(long bytes) {
        final MemoryBlock block1 = allocator.allocate(bytes, memoryTracker);
        allocator.free(block1, memoryTracker);

        final MemoryBlock block2 = allocator.allocate(bytes, memoryTracker);
        allocator.free(block2, memoryTracker);

        verify(memoryTracker, times(2)).allocateNative(eq(bytes));
        assertEquals(0, memoryTracker.usedNativeMemory());
    }

    @ParameterizedTest
    @ValueSource(longs = {8, 64, 128})
    void allocateCacheableSize(long bytes) {
        final MemoryBlock block1 = allocator.allocate(bytes, memoryTracker);
        allocator.free(block1, memoryTracker);

        final MemoryBlock block2 = allocator.allocate(bytes, memoryTracker);
        allocator.free(block2, memoryTracker);

        verify(memoryTracker, times(2)).allocateNative(eq(bytes));
        assertEquals(1, allocator.numberOfCachedBlocks());
        assertEquals(0, memoryTracker.usedNativeMemory());
    }

    @Test
    void cacheCapacityPerBlockSize() {
        final int EXTRA = 3;
        final List<MemoryBlock> blocks64 = new ArrayList<>();
        final List<MemoryBlock> blocks128 = new ArrayList<>();
        for (int i = 0; i < CACHE_SIZE + EXTRA; i++) {
            blocks64.add(allocator.allocate(64, memoryTracker));
            blocks128.add(allocator.allocate(128, memoryTracker));
        }

        verify(memoryTracker, times(CACHE_SIZE + EXTRA)).allocateNative(eq(64L));
        verify(memoryTracker, times(CACHE_SIZE + EXTRA)).allocateNative(eq(128L));
        assertEquals((CACHE_SIZE + EXTRA) * (64 + 128), memoryTracker.usedNativeMemory());

        blocks64.forEach(it -> allocator.free(it, memoryTracker));
        assertEquals((CACHE_SIZE + EXTRA) * 128, memoryTracker.usedNativeMemory());

        blocks128.forEach(it -> allocator.free(it, memoryTracker));
        assertEquals(0, memoryTracker.usedNativeMemory());

        assertEquals(CACHE_SIZE * 2, allocator.numberOfCachedBlocks());
        verify(memoryTracker, times((CACHE_SIZE + EXTRA) * 2)).releaseNative(anyLong());
    }
}
