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
 * Factory of {@link LongArray}, {@link IntArray} and {@link ByteArray} instances. Users can select in which type of memory the arrays will be placed, either in
 * {@link NumberArrayFactories#HEAP}, {@link NumberArrayFactories#OFF_HEAP}, or use an auto allocator which will have each instance placed where it fits best,
 * favoring the primary candidates.
 */
public interface NumberArrayFactory {
    interface Monitor {
        /**
         * Notifies about a successful allocation where information about both successful and failed attempts are included.
         *
         * @param memory amount of memory for this allocation.
         * @param successfulFactory the {@link NumberArrayFactory} which proved successful allocating this amount of memory.
         * @param attemptedAllocationFailures list of failed attempts to allocate this memory in other allocators.
         */
        void allocationSuccessful(
                long memory,
                NumberArrayFactory successfulFactory,
                Iterable<AllocationFailure> attemptedAllocationFailures);
    }

    class AllocationFailure {
        private final Throwable failure;
        private final NumberArrayFactory factory;

        AllocationFailure(Throwable failure, NumberArrayFactory factory) {
            this.failure = failure;
            this.factory = factory;
        }

        public Throwable getFailure() {
            return failure;
        }

        public NumberArrayFactory getFactory() {
            return factory;
        }
    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return a fixed size {@link IntArray}.
     */
    default IntArray newIntArray(long length, int defaultValue, MemoryTracker memoryTracker) {
        return newIntArray(length, defaultValue, 0, memoryTracker);
    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param base base index to rebase all requested indexes with.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return a fixed size {@link IntArray}.
     */
    IntArray newIntArray(long length, int defaultValue, long base, MemoryTracker memoryTracker);

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param defaultValue value which will represent unset values.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return dynamically growing {@link IntArray}.
     */
    IntArray newDynamicIntArray(long chunkSize, int defaultValue, MemoryTracker memoryTracker);

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return a fixed size {@link LongArray}.
     */
    default LongArray newLongArray(long length, long defaultValue, MemoryTracker memoryTracker) {
        return newLongArray(length, defaultValue, 0, memoryTracker);
    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param base base index to rebase all requested indexes with.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return a fixed size {@link LongArray}.
     */
    LongArray newLongArray(long length, long defaultValue, long base, MemoryTracker memoryTracker);

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param defaultValue value which will represent unset values.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return dynamically growing {@link LongArray}.
     */
    LongArray newDynamicLongArray(long chunkSize, long defaultValue, MemoryTracker memoryTracker);

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return a fixed size {@link ByteArray}.
     */
    default ByteArray newByteArray(long length, byte[] defaultValue, MemoryTracker memoryTracker) {
        return newByteArray(length, defaultValue, 0, memoryTracker);
    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param base base index to rebase all requested indexes with.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return a fixed size {@link ByteArray}.
     */
    ByteArray newByteArray(long length, byte[] defaultValue, long base, MemoryTracker memoryTracker);

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param defaultValue value which will represent unset values.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return dynamically growing {@link ByteArray}.
     */
    ByteArray newDynamicByteArray(long chunkSize, byte[] defaultValue, MemoryTracker memoryTracker);

    /**
     * Implements the dynamic array methods, because they are the same in most implementations.
     */
    abstract class Adapter implements NumberArrayFactory {
        @Override
        public IntArray newDynamicIntArray(long chunkSize, int defaultValue, MemoryTracker memoryTracker) {
            return new DynamicIntArray(this, chunkSize, defaultValue, memoryTracker);
        }

        @Override
        public LongArray newDynamicLongArray(long chunkSize, long defaultValue, MemoryTracker memoryTracker) {
            return new DynamicLongArray(this, chunkSize, defaultValue, memoryTracker);
        }

        @Override
        public ByteArray newDynamicByteArray(long chunkSize, byte[] defaultValue, MemoryTracker memoryTracker) {
            return new DynamicByteArray(this, chunkSize, defaultValue, memoryTracker);
        }
    }
}
