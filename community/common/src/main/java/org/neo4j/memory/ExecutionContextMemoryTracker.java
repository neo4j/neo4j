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

import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static org.neo4j.internal.helpers.Numbers.ceilingPowerOfTwo;
import static org.neo4j.kernel.api.exceptions.Status.General.TransactionOutOfMemoryError;
import static org.neo4j.memory.HighWaterMarkMemoryPool.NO_TRACKING;
import static org.neo4j.util.Preconditions.requireNonNegative;
import static org.neo4j.util.Preconditions.requirePositive;

import java.util.function.BooleanSupplier;
import org.neo4j.util.VisibleForTesting;

/**
 * Memory allocation tracker that can be used as an alternative to LocalMemoryTracker for a set of execution contexts with
 * a shared parent memory pool, where corresponding allocation and release calls are allowed to occur on different child ExecutionContextMemoryTrackers.
 * Allocated bytes on individual ExecutionContextMemoryTrackers are allowed to go both positive and _negative_.
 * The only requirement is that it all eventually adds upp correctly in the parent memory pool.
 *
 * <p>
 * You could impose a limit on the total number of allocated bytes, but typically the limit is controlled by the parent memory pool.
 * <p>
 * To reduce contention on the parent tracker, locally reserved bytes are batched from the parent to a local pool. Once the pool is used up, new bytes will be
 * reserved. Calling {@link #reset()} will give back all the reserved bytes to the parent. Forgetting to call this will "leak" bytes and starve the database of
 * allocations.
 * <p>
 * To allow for smaller initial grab sizes and still reduce contention, the grab size will adaptively grow (up to the given maximum grab size)
 * based on the number of calls to allocate or release heap since the last reservation or release to the parent memory pool.
 */
public class ExecutionContextMemoryTracker implements LimitedMemoryTracker {
    public static final long NO_LIMIT = 0;
    private static final long INFINITY = Long.MAX_VALUE;
    private static final long DEFAULT_GRAB_SIZE = 8192;

    /**
     * If an allocation call triggers a reservation from the memory pool,
     * and the number of allocation calls since the last reservation is less
     * than this threshold, we will increase the grab size in an attempt to
     * reduce the number of reservation calls made to the memory pool.
     */
    private static final int CLIENT_CALLS_PER_POOL_INTERACTION_THRESHOLD = 10;

    /**
     * Imposes limits on a {@link MemoryGroup} level, e.g. global maximum transactions size
     */
    private final HighWaterMarkMemoryPool memoryPool;

    /**
     * The chunk size to reserve from the memory pool
     */
    private long grabSize;

    /**
     * The maximum chunk size to reserve from the memory pool
     */
    private final long maxGrabSize;

    /**
     * The number of allocation or release calls since the last interaction with the memory pool
     */
    private int clientCallsSinceLastPoolInteraction;

    /**
     * Name of the setting that imposes the limit.
     */
    private final String limitSettingName;

    /**
     * Check if current memory tracker is open and any operations are allowed
     */
    private final BooleanSupplier openCheck;

    /**
     * A per tracker limit.
     */
    private long localBytesLimit;

    /**
     * Number of bytes we are allowed to use on the heap. If this run out, we need to reserve more from the parent.
     */
    private long localHeapPool;

    /**
     * The current size of the tracked heap
     */
    private long allocatedBytesHeap;

    /**
     * The currently allocated off heap
     */
    private long allocatedBytesNative;

    public ExecutionContextMemoryTracker() {
        this(NO_TRACKING);
    }

    public ExecutionContextMemoryTracker(HighWaterMarkMemoryPool memoryPool) {
        this(memoryPool, INFINITY, DEFAULT_GRAB_SIZE, DEFAULT_GRAB_SIZE, null);
    }

    public ExecutionContextMemoryTracker(
            HighWaterMarkMemoryPool memoryPool,
            long localBytesLimit,
            long grabSize,
            long maxGrabSize,
            String limitSettingName) {
        this(memoryPool, localBytesLimit, grabSize, maxGrabSize, limitSettingName, () -> true);
    }

    public ExecutionContextMemoryTracker(
            HighWaterMarkMemoryPool memoryPool,
            long localBytesLimit,
            long grabSize,
            long maxGrabSize,
            String limitSettingName,
            BooleanSupplier openCheck) {
        this.memoryPool = requireNonNull(memoryPool);

        this.localBytesLimit = validateLimit(localBytesLimit);
        this.grabSize = requireNonNegative(grabSize);
        this.maxGrabSize = requireNonNegative(maxGrabSize);

        this.limitSettingName = limitSettingName;
        this.openCheck = openCheck;

        // NOTE: We do not want the threshold to apply on the first grab
        this.clientCallsSinceLastPoolInteraction = CLIENT_CALLS_PER_POOL_INTERACTION_THRESHOLD;
    }

    @Override
    public void allocateNative(long bytes) {
        assert openCheck.getAsBoolean() : "Tracker should be open to allow new allocations.";
        if (bytes == 0) {
            return;
        }
        requirePositive(bytes);

        allocatedBytesNative += bytes;

        if (allocatedBytesHeap + allocatedBytesNative > localBytesLimit) {
            allocatedBytesNative -= bytes;
            throw new MemoryLimitExceededException(
                    bytes,
                    localBytesLimit,
                    allocatedBytesHeap + allocatedBytesNative,
                    TransactionOutOfMemoryError,
                    limitSettingName);
        }

        try {
            this.memoryPool.reserveNative(bytes);
        } catch (MemoryLimitExceededException t) {
            allocatedBytesNative -= bytes;
            throw t;
        }
    }

    @Override
    public void releaseNative(long bytes) {
        if (bytes == 0) {
            return;
        }
        assert openCheck.getAsBoolean() : "Tracker should be open to allow releasing native memory.";
        this.allocatedBytesNative -= bytes;
        this.memoryPool.releaseNative(bytes);
    }

    @Override
    public void allocateHeap(long bytes) {
        if (bytes == 0) {
            return;
        }
        requirePositive(bytes);

        allocatedBytesHeap += bytes;
        clientCallsSinceLastPoolInteraction++;

        if (allocatedBytesHeap + allocatedBytesNative > localBytesLimit) {
            allocatedBytesHeap -= bytes;
            throw new MemoryLimitExceededException(
                    bytes,
                    localBytesLimit,
                    allocatedBytesHeap + allocatedBytesNative,
                    TransactionOutOfMemoryError,
                    limitSettingName);
        }

        localHeapPool -= bytes;
        if (localHeapPool < 0) {
            long grab = max(bytes, grabSize);
            try {
                reserveHeapFromPool(grab);
            } catch (MemoryLimitExceededException t) {
                allocatedBytesHeap -= bytes;
                throw t;
            }
        }
    }

    @Override
    public void releaseHeap(long bytes) {
        if (bytes == 0) {
            return;
        }
        requireNonNegative(bytes);
        allocatedBytesHeap -= bytes;
        localHeapPool += bytes;
        clientCallsSinceLastPoolInteraction++;

        // If the localHeapPool has reserved a lot more memory than is being used release part of it again.
        // The threshold for releasing memory back to the pool is double that of the current grab size
        if (localHeapPool > grabSize << 1) {
            long memoryToRelease = max(grabSize, localHeapPool - (grabSize << 1));
            releaseHeapToPool(memoryToRelease);
        }
    }

    @Override
    public long heapHighWaterMark() {
        return memoryPool.heapHighWaterMark();
    }

    /**
     * @return number of used bytes.
     */
    @Override
    public long usedNativeMemory() {
        return allocatedBytesNative;
    }

    @Override
    public long estimatedHeapMemory() {
        return allocatedBytesHeap;
    }

    @Override
    public void reset() {
        // Only release or reserve heap if the transaction is still open
        if (openCheck.getAsBoolean()) {
            long localHeapToRelease = localHeapPool;
            if (localHeapToRelease > 0L) {
                memoryPool.releaseHeap(localHeapToRelease);
            }
        }
        localHeapPool = 0;
        allocatedBytesHeap = 0;
        allocatedBytesNative = 0;
    }

    @Override
    public MemoryTracker getScopedMemoryTracker() {
        return new DefaultScopedMemoryTracker(this);
    }

    @Override
    public void setLimit(long localBytesLimit) {
        this.localBytesLimit = validateLimit(localBytesLimit);
    }

    @VisibleForTesting
    public long localHeapPool() {
        return localHeapPool;
    }

    /**
     * Will reserve heap in the provided pool.
     *
     * @param size heap space to reserve for the local pool
     * @throws MemoryLimitExceededException if not enough free memory
     */
    private void reserveHeapFromPool(long size) {
        // Only reserve heap if the transaction is still open
        if (openCheck.getAsBoolean()) {
            if (clientCallsSinceLastPoolInteraction < CLIENT_CALLS_PER_POOL_INTERACTION_THRESHOLD) {
                increaseGrabSize(size);
            }
            memoryPool.reserveHeap(size);
            localHeapPool += size;
            clientCallsSinceLastPoolInteraction = 0;
        }
    }

    private void releaseHeapToPool(long size) {
        // Only release heap if the transaction is still open
        if (openCheck.getAsBoolean()) {
            if (clientCallsSinceLastPoolInteraction < CLIENT_CALLS_PER_POOL_INTERACTION_THRESHOLD) {
                increaseGrabSize(size);
            }
            memoryPool.releaseHeap(size);
            localHeapPool -= size;
            clientCallsSinceLastPoolInteraction = 0;
        }
    }

    private void increaseGrabSize(long size) {
        var newGrabSize = Math.min(ceilingPowerOfTwo(size + 1), maxGrabSize);
        grabSize = newGrabSize;
    }

    private static long validateLimit(long localBytesLimit) {
        return localBytesLimit == NO_LIMIT ? INFINITY : requireNonNegative(localBytesLimit);
    }
}
