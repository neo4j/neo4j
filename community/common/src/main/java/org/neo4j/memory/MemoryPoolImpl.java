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

import static org.neo4j.kernel.api.exceptions.Status.General.MemoryPoolOutOfMemoryError;
import static org.neo4j.util.Preconditions.requirePositive;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A pool of memory that can be limited. The implementation is thread-safe.
 */
public class MemoryPoolImpl implements MemoryPool {
    private final AtomicLong maxMemory = new AtomicLong();
    private final AtomicLong usedHeapBytes = new AtomicLong();
    private final AtomicLong usedNativeBytes = new AtomicLong();
    private final boolean strict;
    private final String limitSettingName;

    /**
     * @param limit of the pool, passing 0 will result in an unbounded pool
     * @param strict if true enforce limit by throwing exception
     */
    public MemoryPoolImpl(long limit, boolean strict, String limitSettingName) {
        this.limitSettingName = limitSettingName;
        this.maxMemory.setRelease(validateSize(limit));
        this.strict = strict;
    }

    @Override
    public long usedHeap() {
        return usedHeapBytes.getAcquire();
    }

    @Override
    public long usedNative() {
        return usedNativeBytes.getAcquire();
    }

    @Override
    public long free() {
        return Math.max(0, totalSize() - totalUsed());
    }

    @Override
    public void releaseHeap(long bytes) {
        usedHeapBytes.addAndGet(-bytes);
    }

    @Override
    public void releaseNative(long bytes) {
        usedNativeBytes.addAndGet(-bytes);
    }

    @Override
    public void reserveHeap(long bytes) {
        reserveMemory(bytes, usedHeapBytes, usedNativeBytes);
    }

    @Override
    public void reserveNative(long bytes) {
        reserveMemory(bytes, usedNativeBytes, usedHeapBytes);
    }

    private void reserveMemory(long bytes, AtomicLong poolCounter, AtomicLong complementPoolValue) {
        long max = strict ? maxMemory.getAcquire() : Long.MAX_VALUE;

        long newCounterValue = poolCounter.addAndGet(bytes);
        if (strict) {
            long localTotal = newCounterValue + complementPoolValue.getAcquire();
            if (localTotal > max) {
                poolCounter.addAndGet(-bytes);
                throw new MemoryLimitExceededException(
                        bytes, max, localTotal - bytes, MemoryPoolOutOfMemoryError, limitSettingName);
            }
        }
    }

    @Override
    public long totalSize() {
        return maxMemory.getAcquire();
    }

    @Override
    public void setSize(long size) {
        maxMemory.setRelease(validateSize(size));
    }

    private static long validateSize(long size) {
        if (size == 0) {
            return Long.MAX_VALUE;
        }
        return requirePositive(size);
    }
}
