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

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.neo4j.internal.helpers.Exceptions.chain;
import static org.neo4j.io.ByteUnit.bytesToString;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.neo4j.internal.unsafe.NativeMemoryAllocationRefusedError;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.logging.InternalLog;
import org.neo4j.memory.MemoryTracker;

public class NumberArrayFactories {
    private NumberArrayFactories() {}

    public static final NumberArrayFactory.Monitor NO_MONITOR =
            (memory, successfulFactory, attemptedAllocationFailures) -> {
                /* no-op */
            };

    /**
     * Puts arrays inside the heap.
     */
    public static final NumberArrayFactory HEAP = new NumberArrayFactory.Adapter() {
        @Override
        public IntArray newIntArray(long length, int defaultValue, long base, MemoryTracker memoryTracker) {
            return new HeapIntArray(toIntExact(length), defaultValue, base, memoryTracker);
        }

        @Override
        public LongArray newLongArray(long length, long defaultValue, long base, MemoryTracker memoryTracker) {
            return new HeapLongArray(toIntExact(length), defaultValue, base, memoryTracker);
        }

        @Override
        public HeapByteArray newByteArray(long length, byte[] defaultValue, long base, MemoryTracker memoryTracker) {
            return new HeapByteArray(toIntExact(length), defaultValue, base, memoryTracker);
        }

        @Override
        public String toString() {
            return "HEAP";
        }
    };

    /**
     * Puts arrays off-heap, using unsafe calls.
     */
    public static final NumberArrayFactory OFF_HEAP = new NumberArrayFactory.Adapter() {
        @Override
        public IntArray newIntArray(long length, int defaultValue, long base, MemoryTracker memoryTracker) {
            return new OffHeapIntArray(length, defaultValue, base, memoryTracker);
        }

        @Override
        public LongArray newLongArray(long length, long defaultValue, long base, MemoryTracker memoryTracker) {
            return new OffHeapLongArray(length, defaultValue, base, memoryTracker);
        }

        @Override
        public ByteArray newByteArray(long length, byte[] defaultValue, long base, MemoryTracker memoryTracker) {
            return new OffHeapByteArray(length, defaultValue, base, memoryTracker);
        }

        @Override
        public String toString() {
            return "OFF_HEAP";
        }
    };

    /**
     * Used as part of the fallback strategy for {@link Auto}. Tries to split up fixed-size arrays ({@link NumberArrayFactory#newLongArray(long, long,
     * MemoryTracker)} and {@link NumberArrayFactory#newIntArray(long, int, MemoryTracker)} into smaller chunks where some can live on heap and some off heap.
     */
    public static final NumberArrayFactory CHUNKED_FIXED_SIZE =
            new ChunkedNumberArrayFactory(NO_MONITOR, OFF_HEAP, HEAP);

    /**
     * {@link Auto} factory which uses JVM stats for gathering information about available memory.
     */
    public static final NumberArrayFactory AUTO_WITHOUT_PAGECACHE =
            new Auto(NO_MONITOR, OFF_HEAP, HEAP, CHUNKED_FIXED_SIZE);

    /**
     * {@link Auto} factory which has a page cache backed number array as final fallback, in order to prevent OOM errors.
     *
     * @param pageCache           {@link PageCache} to fallback allocation into, if no more memory is available.
     * @param contextFactory      underlying page cache cursor context factory.
     * @param dir                 directory where cached files are placed.
     * @param allowHeapAllocation whether or not to allow allocation on heap. Otherwise allocation is restricted to off-heap and the page cache fallback. This
     *                            to be more in control of available space in the heap at all times.
     * @param monitor             for monitoring successful and failed allocations and which factory was selected.
     * @return a {@link NumberArrayFactory} which tries to allocation off-heap, then potentially on heap and lastly falls back to allocating inside the given
     * {@code pageCache}.
     */
    public static NumberArrayFactory auto(
            PageCache pageCache,
            CursorContextFactory contextFactory,
            Path dir,
            boolean allowHeapAllocation,
            NumberArrayFactory.Monitor monitor,
            InternalLog log,
            String databaseName) {
        PageCachedNumberArrayFactory pagedArrayFactory =
                new PageCachedNumberArrayFactory(pageCache, contextFactory, dir, log, databaseName);
        ChunkedNumberArrayFactory chunkedArrayFactory =
                new ChunkedNumberArrayFactory(monitor, allocationAlternatives(allowHeapAllocation, pagedArrayFactory));
        return new Auto(monitor, allocationAlternatives(allowHeapAllocation, chunkedArrayFactory));
    }

    /**
     * @param allowHeapAllocation whether or not to include heap allocation as an alternative.
     * @param additional          other means of allocation to try after the standard off/on heap alternatives.
     * @return an array of {@link NumberArrayFactory} with the desired alternatives.
     */
    private static NumberArrayFactory[] allocationAlternatives(
            boolean allowHeapAllocation, NumberArrayFactory... additional) {
        List<NumberArrayFactory> result = new ArrayList<>(Collections.singletonList(OFF_HEAP));
        if (allowHeapAllocation) {
            result.add(HEAP);
        }
        result.addAll(asList(additional));
        return result.toArray(new NumberArrayFactory[0]);
    }

    /**
     * Looks at available memory and decides where the requested array fits best. Tries to allocate the whole array with the first candidate, falling back to
     * others as needed.
     */
    static class Auto extends NumberArrayFactory.Adapter {
        private final Monitor monitor;
        private final NumberArrayFactory[] candidates;

        Auto(Monitor monitor, NumberArrayFactory... candidates) {
            Objects.requireNonNull(monitor);
            this.monitor = monitor;
            this.candidates = candidates;
        }

        @Override
        public LongArray newLongArray(long length, long defaultValue, long base, MemoryTracker memoryTracker) {
            return tryAllocate(length, 8, f -> f.newLongArray(length, defaultValue, base, memoryTracker));
        }

        @Override
        public IntArray newIntArray(long length, int defaultValue, long base, MemoryTracker memoryTracker) {
            return tryAllocate(length, 4, f -> f.newIntArray(length, defaultValue, base, memoryTracker));
        }

        @Override
        public ByteArray newByteArray(long length, byte[] defaultValue, long base, MemoryTracker memoryTracker) {
            return tryAllocate(
                    length, defaultValue.length, f -> f.newByteArray(length, defaultValue, base, memoryTracker));
        }

        private <T extends NumberArray<? extends T>> T tryAllocate(
                long length, int itemSize, Function<NumberArrayFactory, T> allocator) {
            List<AllocationFailure> failures = new ArrayList<>();
            Error error = null;
            for (NumberArrayFactory candidate : candidates) {
                try {
                    try {
                        T array = allocator.apply(candidate);
                        monitor.allocationSuccessful(length * itemSize, candidate, failures);
                        return array;
                    } catch (ArithmeticException e) {
                        throw new OutOfMemoryError(e.getMessage());
                    }
                } catch (OutOfMemoryError | NativeMemoryAllocationRefusedError e) { // Alright let's try the next one

                    error = chain(e, error);
                    failures.add(new AllocationFailure(e, candidate));
                }
            }
            throw chain(
                    new OutOfMemoryError(format(
                            "Not enough memory available for allocating %s, tried %s",
                            bytesToString(length * itemSize), Arrays.toString(candidates))),
                    error);
        }
    }
}
