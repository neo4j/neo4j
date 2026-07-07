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
package org.neo4j.memory;

public class SharedMemoryPool implements ScopedMemoryPool {
    private final MemoryPool shared;
    private final MemoryGroup memoryGroup;

    public SharedMemoryPool(MemoryGroup memoryGroup, long globalBytes, String globalBytesSettingName) {
        this.shared = new NonValidatingSizeMemoryPoolImpl(globalBytes, true, globalBytesSettingName);
        this.memoryGroup = memoryGroup;
    }

    @Override
    public MemoryGroup group() {
        return memoryGroup;
    }

    @Override
    public void close() {}

    @Override
    public MemoryTracker getPoolMemoryTracker() {
        // interact with pool via reserved pool's memory tracker
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void reserveHeap(long bytes) {
        shared.reserveHeap(bytes);
    }

    @Override
    public synchronized void reserveHeapNoThrow(long bytes) {
        shared.reserveHeapNoThrow(bytes);
    }

    @Override
    public synchronized void reserveNative(long bytes) {
        shared.reserveNative(bytes);
    }

    @Override
    public synchronized void reserveNativeNoThrow(long bytes) {
        shared.reserveNativeNoThrow(bytes);
    }

    @Override
    public synchronized void releaseHeap(long bytes) {
        shared.releaseHeap(bytes);
    }

    @Override
    public synchronized void releaseNative(long bytes) {
        shared.releaseNative(bytes);
    }

    @Override
    public synchronized long totalSize() {
        return shared.totalSize();
    }

    @Override
    public synchronized long usedHeap() {
        return shared.usedHeap();
    }

    @Override
    public synchronized long usedNative() {
        return shared.usedNative();
    }

    @Override
    public synchronized long free() {
        return shared.free();
    }

    @Override
    public synchronized void setSize(long size) {
        shared.setSize(size);
    }

    public synchronized void resize(long size) {
        long current = shared.totalSize();
        setSize(size + current);
    }

    /**
     * A pool of memory that performs no validation on the size it can be set , i.e., it can go negative in size.
     */
    private static class NonValidatingSizeMemoryPoolImpl extends MemoryPoolImpl {

        private NonValidatingSizeMemoryPoolImpl(long limit, boolean strict, String limitSettingName) {
            super(limit, strict, limitSettingName);
        }

        @Override
        public long free() {
            return super.totalSize() - super.totalUsed();
        }

        @Override
        long validateSize(long size) {
            return size;
        }
    }
}
