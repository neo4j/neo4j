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

/**
 * A memory pool that first attempts to allocate into its local pool, if this is full it then attempts to
 * allocate into a (shared) global pool.
 */
public class OverspillingMemoryPool implements ScopedMemoryPool {

    private final String databaseName;
    private final MemoryPool local;
    private final MemoryPool global;
    private final MemoryGroup memoryGroup;
    private final MemoryTracker memoryTracker;
    private long reservedHeapInGlobal;
    private long reservedNativeInGlobal;

    public OverspillingMemoryPool(
            MemoryGroup memoryGroup,
            MemoryPool global,
            String databaseName,
            long reservedBytes,
            String limitSettingName) {
        this.memoryGroup = memoryGroup;
        this.global = global;
        this.local = new MemoryPoolImpl(reservedBytes, true, limitSettingName);
        this.databaseName = databaseName;
        this.memoryTracker = new MemoryPoolTracker(this);
        this.reservedHeapInGlobal = 0;
        this.reservedNativeInGlobal = 0;
    }

    @Override
    public String databaseName() {
        return databaseName;
    }

    @Override
    public MemoryGroup group() {
        return memoryGroup;
    }

    @Override
    public void close() {
        global.releaseHeap(reservedHeapInGlobal);
        global.releaseNative(reservedNativeInGlobal);
        memoryTracker.close();
    }

    @Override
    public MemoryTracker getPoolMemoryTracker() {
        return memoryTracker;
    }

    @Override
    public void reserveHeap(long bytes) {
        long free = local.free();
        if (free == 0) {
            global.reserveHeap(bytes);
            reservedHeapInGlobal += bytes;
        } else if (free >= bytes) {
            local.reserveHeap(bytes);
        } else {
            long additional = bytes - free;
            global.reserveHeap(additional);
            local.reserveHeap(free);
            reservedHeapInGlobal += additional;
        }
    }

    @Override
    public void reserveHeapNoThrow(long bytes) {
        long free = local.free();
        if (free == 0) {
            global.reserveHeapNoThrow(bytes);
            reservedHeapInGlobal += bytes;
        } else if (free >= bytes) {
            local.reserveHeapNoThrow(bytes);
        } else {
            long additional = bytes - free;
            global.reserveHeapNoThrow(additional);
            local.reserveHeapNoThrow(free);
            reservedHeapInGlobal += additional;
        }
    }

    @Override
    public void reserveNative(long bytes) {
        long free = local.free();
        if (free == 0) {
            global.reserveNative(bytes);
            reservedNativeInGlobal += bytes;
        } else if (free >= bytes) {
            local.reserveNative(bytes);
        } else {
            long additional = bytes - free;
            global.reserveNative(additional);
            local.reserveNative(free);
            reservedNativeInGlobal += additional;
        }
    }

    @Override
    public void reserveNativeNoThrow(long bytes) {
        long free = local.free();
        if (free == 0) {
            global.reserveNativeNoThrow(bytes);
            reservedNativeInGlobal += bytes;
        } else if (free >= bytes) {
            local.reserveNativeNoThrow(bytes);
        } else {
            long additional = bytes - free;
            global.reserveNativeNoThrow(additional);
            local.reserveNativeNoThrow(free);
            reservedNativeInGlobal += additional;
        }
    }

    @Override
    public void releaseHeap(long bytes) {
        if (reservedHeapInGlobal >= bytes) {
            global.releaseHeap(bytes);
            reservedHeapInGlobal -= bytes;
        } else if (reservedHeapInGlobal == 0) {
            local.releaseHeap(bytes);
        } else {
            long additional = bytes - reservedHeapInGlobal;
            global.releaseHeap(reservedHeapInGlobal);
            local.releaseHeap(additional);
            reservedHeapInGlobal = 0;
        }
    }

    @Override
    public void releaseNative(long bytes) {
        if (reservedNativeInGlobal >= bytes) {
            global.releaseNative(bytes);
            reservedNativeInGlobal -= bytes;
        } else if (reservedNativeInGlobal == 0) {
            local.releaseNative(bytes);
        } else {
            long additional = bytes - reservedNativeInGlobal;
            global.releaseNative(reservedNativeInGlobal);
            local.releaseNative(additional);
            reservedNativeInGlobal = 0;
        }
    }

    @Override
    public long totalSize() {
        return local.totalSize();
    }

    @Override
    public long usedHeap() {
        return local.usedHeap();
    }

    @Override
    public long usedNative() {
        return local.usedNative();
    }

    @Override
    public long free() {
        return local.free();
    }

    @Override
    public void setSize(long size) {
        local.setSize(size);
    }
}
