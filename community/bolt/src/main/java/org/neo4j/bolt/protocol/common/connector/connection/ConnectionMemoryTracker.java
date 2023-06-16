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
package org.neo4j.bolt.protocol.common.connector.connection;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import org.neo4j.memory.DefaultScopedMemoryTracker;
import org.neo4j.memory.MemoryPool;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

/**
 * Provides a thread safe connection scoped memory tracker implementation.
 */
public class ConnectionMemoryTracker implements MemoryTracker {
    @VisibleForTesting
    static final long CHUNK_SIZE = 64;

    private final MemoryPool parent;

    private final AtomicLong pooled = new AtomicLong(CHUNK_SIZE);
    private final AtomicLong allocated = new AtomicLong();

    /**
     * Accumulates the maximum allocation seen by this memory tracker within its respective
     * lifetime.
     * <p />
     * This field makes use of {@link LongAccumulator} in order to find the largest observed
     * allocation in a thread safe yet fast manner. Note, however, that this sacrifices some memory
     * over {@link AtomicLong} based solutions in favor of performance.
     */
    private final LongAccumulator watermark = new LongAccumulator(Long::max, 0);

    private ConnectionMemoryTracker(MemoryPool parent) {
        this.parent = parent;
    }

    public static ConnectionMemoryTracker createForPool(MemoryPool parent) {
        // reserve the designated chunk size initially in order to bootstrap the pool with a bit of
        // usable memory
        parent.reserveHeap(CHUNK_SIZE);

        return new ConnectionMemoryTracker(parent);
    }

    @Override
    public long usedNativeMemory() {
        return 0;
    }

    @Override
    public long estimatedHeapMemory() {
        return this.allocated.get();
    }

    @Override
    public void allocateNative(long bytes) {
        throw new UnsupportedOperationException("Reporting per-connection native allocation is not supported");
    }

    @Override
    public void releaseNative(long bytes) {
        // allocation is not permitted thus making any release logic unnecessary
        // NOOP implementation is still required, however, as scoped trackers will try to release
        // zero byte allocations
    }

    private long requestFromLocalPool(long expected, long request) {
        if (this.pooled.compareAndSet(expected, expected - request)) {
            return request;
        }

        return 0;
    }

    private void request(long requested) {
        var remaining = requested;

        long allocation;
        long previousAvailable;
        do {
            previousAvailable = this.pooled.get();

            if (previousAvailable > remaining) {
                // when there is a sufficient amount of memory available within our local pool,
                // we'll simply subtract the requested amount and attempt to claim it
                allocation = remaining;
            } else if (previousAvailable != 0) {
                // otherwise, we'll perform a partial allocation in which we'll attempt to take the
                // remaining amount of available bytes for ourselves - this reduces contention on
                // the pool
                allocation = previousAvailable;
            } else {
                // when there is nothing left to allocate from the local pool, we'll request n
                // chunks from the parent and add the reminder to the pool
                // note that we always allocate an additional chunk - this covers the reminder (if
                // the requested amount is not divisible by CHUNK_SIZE) and further reduces
                // contention on the parent pool as we always retain a bit of additional memory
                var requiredChunks = (remaining / CHUNK_SIZE) + 1;
                var request = requiredChunks * CHUNK_SIZE;

                // since we are directly satisfying the request from the parent pool, we'll simply
                // add the difference between our constructed request and the actual number of
                // remaining bytes to the local pool and return early
                this.parent.reserveHeap(request);
                this.pooled.addAndGet(request - remaining);
                return;
            }
        } while ((remaining -= this.requestFromLocalPool(previousAvailable, allocation)) != 0);
    }

    @Override
    public void allocateHeap(long bytes) {
        // first we'll need to make sure that we have sufficient memory within the pool to satisfy
        // the new requirement - this call may fail if the pool has a restriction set which would be
        // exceeded by this request
        this.request(bytes);

        // add the newly allocated memory to our tracking variable to accurately reflect the total
        // consumed memory for this connection
        // TODO: Introduce per-connection memory limit
        var allocated = this.allocated.addAndGet(bytes);

        // just pass the current allocation to the accumulator - it will figure out the new maximum
        // value for us in a thread safe manner
        this.watermark.accumulate(allocated);
    }

    @Override
    public void releaseHeap(long bytes) {
        // first remove the allocated memory from the active section of this tracker in order to
        // update the indicated values
        long newAllocation;
        {
            long previousAllocation;
            do {
                previousAllocation = this.allocated.get();

                assert previousAllocation >= bytes
                        : "Can't release more than it was allocated. Allocated heap: " + previousAllocation
                                + ", release request: " + bytes;

                newAllocation = previousAllocation - bytes;
            } while (!this.allocated.compareAndSet(previousAllocation, newAllocation));
        }

        // once de-allocated we'll return the released memory to the local pool so long as it does
        // not significantly exceed the expected usage
        var release = 0L;
        long previousPool;
        long newPool;
        do {
            previousPool = this.pooled.get();
            newPool = previousPool + bytes;

            if (newPool < CHUNK_SIZE || (newPool / 2) < newAllocation) {
                break;
            }

            release = newPool / 4;
            newPool -= release;
        } while (!this.pooled.compareAndSet(previousPool, newPool));

        if (release != 0) {
            this.parent.releaseHeap(release);
        }
    }

    @Override
    public long heapHighWaterMark() {
        return this.watermark.get();
    }

    @Override
    public MemoryTracker getScopedMemoryTracker() {
        return new DefaultScopedMemoryTracker(this);
    }

    @Override
    public void reset() {
        // simply reset the allocation counter to zero - we'll actually deal with the reclaimed
        // memory in the next steps
        var reclaimedMemory = this.allocated.getAndSet(0);

        // if the pool has less than CHUNK_SIZE bytes remaining, we'll add back the difference in
        // order to minimize potential contention on the parent memory pool as this tracker is
        // reused
        long previousPooledMemory;
        long newPooledMemory;
        long returnedToLocal;
        var returnedToParent = 0L;
        do {
            previousPooledMemory = this.pooled.get();
            if (previousPooledMemory >= CHUNK_SIZE) {
                returnedToLocal = 0;

                // if there is more memory left within the pool than we would expect on a newly
                // constructed instance, we will also return some memory to the parent
                returnedToParent = previousPooledMemory - CHUNK_SIZE;
                newPooledMemory = CHUNK_SIZE;
            } else {
                returnedToLocal = CHUNK_SIZE - previousPooledMemory;
                newPooledMemory = previousPooledMemory + returnedToLocal;
            }
        } while (!this.pooled.compareAndSet(previousPooledMemory, newPooledMemory));

        reclaimedMemory -= returnedToLocal;
        reclaimedMemory += returnedToParent;

        // if there is any reclaimed memory remaining once the reset has completed, we'll release it
        // back into the parent pool
        if (reclaimedMemory != 0) {
            this.parent.releaseHeap(reclaimedMemory);
        }

        // also reset the high watermark back to its original value as the object has likely been
        // returned to a pool for reuse
        this.watermark.reset();
    }

    @Override
    public void close() {
        // TODO: This isn't perfect as we might be freeing memory that we shouldn't since an
        //       allocation _during_ close might become visible after this function returns
        //       For now we accept this as this pool is closed during the invalidation of the
        //       Connection where no more interactions should occur.
        var previouslyPooled = this.pooled.getAndSet(0);
        var previouslyAllocated = this.allocated.getAndSet(0);

        this.parent.releaseHeap(previouslyPooled + previouslyAllocated);
    }
}
