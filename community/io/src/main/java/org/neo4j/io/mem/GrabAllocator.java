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

import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.kibiBytes;

import java.lang.ref.Cleaner;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

/**
 * This memory allocator allocates native memory in large segments, called "grabs". It returns portions of those segments applying requested alignment.
 */
public final class GrabAllocator implements MemoryAllocator {

    private static final long BASE_GRAB_SIZE = kibiBytes(512);
    private static final long MAX_GRAB_SIZE = gibiBytes(1);
    private static final long BASE_MEMORY_SIZE = gibiBytes(100);

    private static final Cleaner globalCleaner = globalCleaner();

    private final Grabs grabs;
    private final Cleaner.Cleanable cleanable;

    /**
     * Create a new GrabAllocator that will allocate the given amount of memory, to pointers that are aligned to the
     * given alignment size.
     *
     * @param expectedMaxMemory The maximum amount of memory that this memory manager is expected to allocate. The
     *                          actual amount of memory used can end up greater than this value, if some of it gets wasted on alignment padding.
     * @param memoryTracker     memory usage tracker
     */
    GrabAllocator(long expectedMaxMemory, Long grabSize, MemoryTracker memoryTracker) {
        Preconditions.requirePositive(expectedMaxMemory);
        this.grabs = new Grabs(expectedMaxMemory, calculateGrabSize(grabSize, expectedMaxMemory), memoryTracker);
        this.cleanable = globalCleaner.register(this, new GrabsDeallocator(grabs));
    }

    /**
     * Here we calculate allocation grab size, if it is not provided.
     * Grab size affects two things:
     * 1. Heap size used to record grabs:
     *      Each {@link Grab} takes 40 bytes (with compressed oops).
     *      At grab size 512KiB and 100GiB of expected max memory it will take 204800 grabs, or 7MiB of heap, to record all allocated memory.
     * 2. Amount of memory wasted for alignment:
     *      Expected alignment is 4K or 8K.
     *      With grab size 512KiB and alignment 4K average waste is about 0.78% or 0.78GiB of 100GiB expected max memory.
     * We take 512KiB grab as a base size and add another 512KiB for every 100GiB of expected max memory.
     * This way heap usage is kept around 7MiB and alignment waste is kept around 1GiB.
     */
    static long calculateGrabSize(Long grabSize, long expectedMaxMemory) {
        if (grabSize != null) {
            Preconditions.requirePositive(grabSize);
            return grabSize;
        }
        return Math.min(BASE_GRAB_SIZE + (expectedMaxMemory / BASE_MEMORY_SIZE) * BASE_GRAB_SIZE, MAX_GRAB_SIZE);
    }

    @Override
    public synchronized long usedMemory() {
        return grabs.usedMemory();
    }

    @Override
    public synchronized long availableMemory() {
        return grabs.availableMemory();
    }

    @Override
    public synchronized long allocateAligned(long bytes, long alignment) {
        return grabs.allocateAligned(bytes, alignment);
    }

    @Override
    public void close() {
        cleanable.clean();
    }

    private static class Grab {
        public final Grab next;
        private final long address;
        private final long limit;
        private long nextPointer;

        Grab(Grab next, long size, MemoryTracker memoryTracker) {
            this.next = next;
            this.address = UnsafeUtil.allocateMemory(size, memoryTracker);
            this.limit = address + size;
            nextPointer = address;
        }

        Grab(Grab next, long address, long limit, long nextPointer) {
            this.next = next;
            this.address = address;
            this.limit = limit;
            this.nextPointer = nextPointer;
        }

        private static long nextAligned(long pointer, long alignment) {
            if (alignment == 1) {
                return pointer;
            }
            long off = pointer % alignment;
            if (off == 0) {
                return pointer;
            }
            return pointer + (alignment - off);
        }

        long allocate(long bytes, long alignment) {
            long allocation = nextAligned(nextPointer, alignment);
            nextPointer = allocation + bytes;
            return allocation;
        }

        void free(MemoryTracker memoryTracker) {
            UnsafeUtil.free(address, limit - address, memoryTracker);
        }

        boolean canAllocate(long bytes, long alignment) {
            return nextAligned(nextPointer, alignment) + bytes <= limit;
        }

        Grab setNext(Grab grab) {
            return new Grab(grab, address, limit, nextPointer);
        }

        @Override
        public String toString() {
            long size = limit - address;
            long reserve = nextPointer > limit ? 0 : limit - nextPointer;
            double use = (1.0 - reserve / ((double) size)) * 100.0;
            return String.format("Grab[size = %d bytes, reserve = %d bytes, use = %5.2f %%]", size, reserve, use);
        }
    }

    private static final class Grabs {
        private final long grabSize;
        private final MemoryTracker memoryTracker;
        private long expectedMaxMemory;
        private Grab head;

        Grabs(long expectedMaxMemory, long grabSize, MemoryTracker memoryTracker) {
            this.expectedMaxMemory = expectedMaxMemory;
            this.grabSize = grabSize;
            this.memoryTracker = memoryTracker;
        }

        long usedMemory() {
            long sum = 0;
            Grab grab = head;
            while (grab != null) {
                sum += grab.nextPointer - grab.address;
                grab = grab.next;
            }
            return sum;
        }

        long availableMemory() {
            Grab grab = head;
            long availableInCurrentGrab = 0;
            if (grab != null) {
                availableInCurrentGrab = grab.limit - grab.nextPointer;
            }
            return Math.max(expectedMaxMemory, 0L) + availableInCurrentGrab;
        }

        public void close() {
            Grab current = head;

            while (current != null) {
                current.free(memoryTracker);
                current = current.next;
            }
            head = null;
        }

        long allocateAligned(long bytes, long alignment) {
            if (alignment <= 0) {
                throw new IllegalArgumentException("Invalid alignment: " + alignment + ". Alignment must be positive.");
            }
            long sizeWithAlignment = bytes + alignment - 1;
            if (sizeWithAlignment > grabSize) {
                // This is a huge allocation. Put it in its own grab and keep any existing grab at the head.
                Grab nextGrab = head == null ? null : head.next;
                Grab allocationGrab = new Grab(nextGrab, sizeWithAlignment, memoryTracker);
                long allocation = allocationGrab.allocate(bytes, alignment);
                head = head == null ? allocationGrab : head.setNext(allocationGrab);
                expectedMaxMemory -= sizeWithAlignment;
                return allocation;
            }

            if (head == null || !head.canAllocate(bytes, alignment)) {
                head = new Grab(head, grabSize, memoryTracker);
                expectedMaxMemory -= grabSize;
            }
            return head.allocate(bytes, alignment);
        }
    }

    private static Cleaner globalCleaner() {
        return Cleaner.create();
    }

    private static final class GrabsDeallocator implements Runnable {
        private final Grabs grabs;

        GrabsDeallocator(Grabs grabs) {
            this.grabs = grabs;
        }

        @Override
        public void run() {
            grabs.close();
        }
    }
}
