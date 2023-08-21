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
package org.neo4j.io.mem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.ByteUnit.MebiByte;
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.kibiBytes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.memory.LocalMemoryTracker;

class MemoryAllocatorTest {
    private static final long ONE_PAGE = PageCache.PAGE_SIZE;
    private static final long EIGHT_PAGES = 8 * PageCache.PAGE_SIZE;

    private MemoryAllocator allocator;

    @AfterEach
    void tearDown() {
        closeAllocator();
    }

    @Test
    void allocatedPointerMustNotBeNull() {
        MemoryAllocator mman = createAllocator(EIGHT_PAGES);
        long address = mman.allocateAligned(PageCache.PAGE_SIZE, 8);
        assertThat(address).isNotEqualTo(0L);
    }

    @Test
    void allocatedPointerMustBePageAligned() {
        MemoryAllocator mman = createAllocator(EIGHT_PAGES);
        long address = mman.allocateAligned(PageCache.PAGE_SIZE, UnsafeUtil.pageSize());
        assertThat(address % UnsafeUtil.pageSize()).isEqualTo(0L);
    }

    @Test
    void allocatedPointerMustBeAlignedToArbitraryByte() {
        int pageSize = UnsafeUtil.pageSize();
        for (int initialOffset = 0; initialOffset < 8; initialOffset++) {
            for (int i = 0; i < pageSize - 1; i++) {
                MemoryAllocator mman = createAllocator(ONE_PAGE);
                mman.allocateAligned(initialOffset, 1);
                long alignment = 1 + i;
                long address = mman.allocateAligned(PageCache.PAGE_SIZE, alignment);
                assertThat(address % alignment)
                        .as("With initial offset " + initialOffset + ", iteration " + i + ", aligning to " + alignment
                                + " and got address " + address)
                        .isEqualTo(0L);
            }
        }
    }

    @Test
    void mustBeAbleToAllocatePastMemoryLimit() {
        MemoryAllocator mman = createAllocator(ONE_PAGE);
        for (int i = 0; i < 4100; i++) {
            assertThat(mman.allocateAligned(1, 2) % 2).isEqualTo(0L);
        }
        // Also asserts that no OutOfMemoryError is thrown.
    }

    @Test
    void allocatedPointersMustBeAlignedPastMemoryLimit() {
        MemoryAllocator mman = createAllocator(ONE_PAGE);
        for (int i = 0; i < 4100; i++) {
            assertThat(mman.allocateAligned(1, 2) % 2).isEqualTo(0L);
        }

        int pageSize = UnsafeUtil.pageSize();
        for (int i = 0; i < pageSize - 1; i++) {
            int alignment = pageSize - i;
            long address = mman.allocateAligned(PageCache.PAGE_SIZE, alignment);
            assertThat(address % alignment)
                    .as("iteration " + i + ", aligning to " + alignment)
                    .isEqualTo(0L);
        }
    }

    @Test
    void alignmentCannotBeZero() {
        assertThrows(
                IllegalArgumentException.class, () -> createAllocator(ONE_PAGE).allocateAligned(8, 0));
    }

    @Test
    void mustBeAbleToAllocateSlabsLargerThanGrabSize() {
        MemoryAllocator mman = createAllocator(MebiByte.toBytes(2));
        long page1 = mman.allocateAligned(UnsafeUtil.pageSize(), 1);
        long largeBlock = mman.allocateAligned(1024 * 1024, 1); // 1 MiB
        long page2 = mman.allocateAligned(UnsafeUtil.pageSize(), 1);
        assertThat(page1).isNotEqualTo(0L);
        assertThat(largeBlock).isNotEqualTo(0L);
        assertThat(page2).isNotEqualTo(0L);
    }

    @Test
    void allocatingMustIncreaseMemoryUsedAndDecreaseAvailableMemory() {
        MemoryAllocator mman = createAllocator(ONE_PAGE, ONE_PAGE);
        // We haven't allocated anything, so usedMemory should be zero, and the available memory should be the
        // initial capacity.
        assertThat(mman.usedMemory()).isEqualTo(0L);
        assertThat(mman.availableMemory()).isEqualTo(PageCache.PAGE_SIZE);

        // Allocate 32 bytes of unaligned memory. Ideally there would be no memory wasted on this allocation,
        // but in principle we cannot rule it out.
        mman.allocateAligned(32, 1);
        assertThat(mman.usedMemory()).isGreaterThanOrEqualTo(32L);
        assertThat(mman.availableMemory()).isLessThanOrEqualTo(PageCache.PAGE_SIZE - 32L);

        // Allocate another 32 bytes of unaligned memory.
        mman.allocateAligned(32, 1);
        assertThat(mman.usedMemory()).isGreaterThanOrEqualTo(64L);
        assertThat(mman.availableMemory()).isLessThanOrEqualTo(PageCache.PAGE_SIZE - 64L);

        // Allocate 1 byte to throw off any subsequent accidental alignment.
        mman.allocateAligned(1, 1);

        // Allocate 32 bytes memory, but this time it is aligned to a 16 byte boundary.
        mman.allocateAligned(32, 16);
        // Don't count the 16 byte alignment in our assertions since we might already be accidentally aligned.
        assertThat(mman.usedMemory()).isGreaterThanOrEqualTo(97L);
        assertThat(mman.availableMemory()).isLessThanOrEqualTo(PageCache.PAGE_SIZE - 97L);
    }

    @Test
    void trackMemoryAllocations() {
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        GrabAllocator allocator = (GrabAllocator) MemoryAllocator.createAllocator(MebiByte.toBytes(2), memoryTracker);

        assertEquals(0, memoryTracker.usedNativeMemory());

        allocator.allocateAligned(ByteUnit.mebiBytes(1), 1);

        assertEquals(ByteUnit.mebiBytes(1), memoryTracker.usedNativeMemory());

        allocator.close();
        assertEquals(0, memoryTracker.usedNativeMemory());
    }

    @Test
    void allAllocatedMemoryMustBeAccessibleForAllAlignments() throws Exception {
        // This test relies on the native access bounds checks that are enabled in Unsafeutil during tests.
        int k512 = (int) ByteUnit.kibiBytes(512);
        int maxAlign = PageCache.PAGE_SIZE >> 2;
        for (int align = 1; align <= maxAlign; align += Long.BYTES) {
            for (int alloc = PageCache.PAGE_SIZE; alloc <= k512; alloc += PageCache.PAGE_SIZE) {
                createAllocator(MebiByte.toBytes(2));
                long addr = allocator.allocateAligned(alloc, align);
                int i = 0;
                try {
                    // This must not throw any bad access exceptions.
                    UnsafeUtil.getLong(addr + i); // Start of allocation.
                    i = alloc - Long.BYTES;
                    UnsafeUtil.getLong(addr + i); // End of allocation.
                } catch (Throwable e) {
                    throw new Exception(
                            String.format(
                                    "Access failed at offset %s (%x) into allocated address %s (%x) of size %s (align %s).",
                                    i, i, addr, addr, alloc, align),
                            e);
                }
            }
        }
    }

    @Test
    void bufferCannotBeAlignedOutsideAllocatedSlot() {
        // This test relies on the native access bounds checks that are enabled in Unsafeutil during tests.
        MemoryAllocator mman = createAllocator(ONE_PAGE);
        // let's choose a really ridiculous alignment to make it very unlikely
        // that the allocated grab will be aligned with it
        long address = mman.allocateAligned(PageCache.PAGE_SIZE, PageCache.PAGE_SIZE - 1);
        assertThat(address).isNotEqualTo(0L);

        // This must not throw any bad access exceptions.
        UnsafeUtil.getLong(address); // Start of allocation.
        UnsafeUtil.getLong(address + ONE_PAGE - Long.BYTES); // End of allocation.
    }

    @ParameterizedTest
    @ValueSource(longs = {1L, 1024L, 512 * 1024L, 5 * 512 * 1024L})
    void canAllocateWithCustomGrabSize(long grabSize) {
        var mman = createAllocator(ONE_PAGE, grabSize);
        long address = mman.allocateAligned(ONE_PAGE, 3);
        assertThat(address).isNotEqualTo(0L);

        // This must not throw any bad access exceptions.
        UnsafeUtil.getLong(address); // Start of allocation.
        UnsafeUtil.getLong(address + ONE_PAGE - Long.BYTES); // End of allocation.
    }

    @Test
    void grabSizeCalculus() {
        assertThat(GrabAllocator.calculateGrabSize(null, 0)).isEqualTo(kibiBytes(512));
        assertThat(GrabAllocator.calculateGrabSize(null, gibiBytes(150))).isEqualTo(kibiBytes(1024));
        assertThat(GrabAllocator.calculateGrabSize(null, Long.MAX_VALUE)).isEqualTo(gibiBytes(1));
    }

    private void closeAllocator() {
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }

    private MemoryAllocator createAllocator(long expectedMaxMemory) {
        closeAllocator();
        allocator = MemoryAllocator.createAllocator(expectedMaxMemory, new LocalMemoryTracker());
        return allocator;
    }

    private MemoryAllocator createAllocator(long expectedMaxMemory, Long grabSize) {
        closeAllocator();
        allocator = MemoryAllocator.createAllocator(expectedMaxMemory, grabSize, new LocalMemoryTracker());
        return allocator;
    }
}
