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
package org.neo4j.index.internal.gbptree;

import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.neo4j.internal.helpers.VarHandleUtils.getVarHandle;

import java.lang.invoke.VarHandle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongConsumer;

/**
 * Latch which acquires and releases read/write locks using compare-and-swap on a {@code long} field.
 * Threads trying to make changes to the latch will busy-spin for a while and then go into timed waiting.
 * <p>
 * Latches have an initial tree node id and last thread releasing the last acquisition will reset the tree node id bits to 0
 * and run a {@code removeAction}. A latch with its tree node id not equal to initial tree node id may not be acquired anymore.
 * This functionality allows these latches to be used in e.g. a {@link ConcurrentHashMap}.
 */
class LongSpinLatch {
    private static final int SPIN_THRESHOLD = Runtime.getRuntime().availableProcessors() < 2 ? 1 : 1000;
    private static final int SHORT_PARK_THRESHOLD = 100_000;
    private static final int LONG_PARK_COUNTER = SHORT_PARK_THRESHOLD + 1;
    private static final int SHORT_PARK_TIME = 10;
    private static final long LONG_PARK_TIME = MILLISECONDS.toNanos(1);

    private static final long WRITE_LOCK_MASK = 0x8000;
    private static final long READ_LOCK_MASK = 0x7FFF;
    private static final long LOCK_MASK = WRITE_LOCK_MASK | READ_LOCK_MASK;
    private static final long REF_COUNT_MASK = 0x7FFF0000;
    private static final long REF_COUNT_UNIT = 0x00010000;

    private static final long DEAD_MASK = 0x80000000L;

    private final long initialTreeNodeId;
    private final LongConsumer removeAction;
    /**
     * Bits that make up this lock:
     * <pre>
     * [    ,    ][    ,    ][    ,    ][    ,    ]
     *  ^^                 ^  ^^                 ^
     *  │└── Ref count ────┘  │└─ Read lock bits ┘
     *  └─ dead               └── Write lock bit
     * </pre>
     */
    @SuppressWarnings("FieldMayBeFinal") // Accessed through VarHandle
    private volatile long lockBits;

    private static final VarHandle LOCK_BITS = getVarHandle(lookup(), "lockBits");

    /**
     * Instantiates a latch which can be acquired, with one or more acquisitions, released, acquired again until finally
     * a release leaving no acquisitions left - being marked as dead and unable to be acquired after that point.
     * @param removeAction action to run when marked as dead, i.e. when last acquisition is released.
     */
    LongSpinLatch(long initialTreeNodeId, LongConsumer removeAction) {
        this.initialTreeNodeId = initialTreeNodeId;
        this.removeAction = removeAction;
    }

    /**
     * Notify you are taking a reference to the latch. If this fails, you are not allowed to use the latch.
     *
     * @return {@code true} if you managed to successfully increase the counter, {@code false} otherwise.
     */
    boolean ref() {
        long prevBits;
        while (true) {
            prevBits = getAcquireBits();
            if (!isAlive(prevBits) || (prevBits & REF_COUNT_MASK) == REF_COUNT_MASK) {
                return false;
            }

            if (LOCK_BITS.weakCompareAndSetRelease(this, prevBits, prevBits + REF_COUNT_UNIT)) {
                return true;
            }
        }
    }

    /**
     * Notify that you are done using this latch.
     */
    void deref() {
        long bits;
        long prevBits;
        while (true) {
            prevBits = getAcquireBits();
            assertAlive(prevBits);
            bits = prevBits - REF_COUNT_UNIT;
            if ((bits & REF_COUNT_MASK) == 0) {
                // We are possibly the last owner, try to mark as dead
                bits |= DEAD_MASK;
            }
            if (LOCK_BITS.weakCompareAndSetRelease(this, prevBits, bits)) {
                break;
            }
            Thread.onSpinWait();
        }

        // Check if we mark the latch as dead
        boolean lastRef = isAlive(prevBits) && !isAlive(bits);
        if (lastRef) {
            removeAction.accept(initialTreeNodeId);
        }

        if ((prevBits & REF_COUNT_MASK) == 0) {
            throw new IllegalStateException("Called 'deref()' on a latch without a matching 'ref()'");
        }
    }

    /**
     * Blocking call.
     * @return the read lock count this resulted in, > 0 if successful, otherwise 0 meaning that an acquisition on a dead lock was attempted.
     */
    long acquireRead() {
        long parkTime = 0;
        long prevBits;
        while (true) {
            prevBits = getAcquireBits();

            assertAlive(prevBits);
            if ((prevBits & READ_LOCK_MASK) == READ_LOCK_MASK) {
                throw new IllegalStateException("Too many readers");
            }

            if (hasWriter(prevBits)) {
                // A writer is holding the lock, let someone else proceed
                parkTime = exponentialPark(parkTime);
                continue;
            }

            if (LOCK_BITS.weakCompareAndSetRelease(this, prevBits, prevBits + 1)) {
                return (prevBits & READ_LOCK_MASK) + 1;
            }
            // Raced with another thread, retry
            Thread.onSpinWait();
        }
    }

    /**
     * Non-blocking call.
     * @return the read lock count this resulted in. 0 means this was the last read lock held.
     */
    long releaseRead() {
        long prevBits = (long) LOCK_BITS.getAndAdd(this, -1L);
        assertAlive(prevBits);
        if (!hasReaders(prevBits)) {
            throw new IllegalStateException("Called 'releaseRead()' on a latch without a reader");
        }
        return (prevBits & READ_LOCK_MASK) - 1;
    }

    /**
     * Non-blocking call.
     * Given that a read lock is already acquired, upgrade it to a write lock.
     * @return whether or not the lock was upgraded. Returns {@code false} for scenarios which would result in deadlock.
     */
    boolean tryUpgradeToWrite() {
        long bits = getAcquireBits();
        assertAlive(bits);
        if ((bits & LOCK_MASK) == 1) {
            // We are the only reader, try to upgrade by clearing read lock and taking write lock
            return LOCK_BITS.compareAndSet(this, bits, (bits & ~LOCK_MASK) | WRITE_LOCK_MASK);
        }
        return false;
    }

    /**
     * Non-blocking advisory call, to be used by a thread currently holding a read lock on this latch.
     * @return whether at this instant the caller is the sole reader and no writer holds the latch.
     */
    boolean couldUpgradeToWrite() {
        long bits = getAcquireBits();
        assertAlive(bits);
        return (bits & LOCK_MASK) == 1;
    }

    /**
     * Blocking call.
     * Acquire a write latch.
     */
    void acquireWrite() {
        long parkTime = 0;
        long prevBits;
        while (true) {
            prevBits = (long) LOCK_BITS.getAndBitwiseOr(this, WRITE_LOCK_MASK);
            assertAlive(prevBits);
            if (!hasWriter(prevBits)) {
                break; // Lock was free and we grabbed it, continue
            }
            // Someone else is holding the write lock
            parkTime = exponentialPark(parkTime);
        }

        // Wait for all readers to leave
        parkTime = 0;
        while (hasReaders(prevBits)) {
            parkTime = exponentialPark(parkTime);
            prevBits = getAcquireBits();
        }
    }

    /**
     * Tries to acquire write latch.
     */
    boolean tryAcquireWrite() {
        long prevBits = getAcquireBits();
        assertAlive(prevBits);
        if (!hasReaders(prevBits) && !hasWriter(prevBits)) {
            return LOCK_BITS.compareAndSet(this, prevBits, prevBits | WRITE_LOCK_MASK);
        }
        return false;
    }

    /**
     * Non-blocking call. Releases the write lock on this latch.
     */
    void releaseWrite() {
        long prevBits = (long) LOCK_BITS.getAndBitwiseAndRelease(this, ~WRITE_LOCK_MASK);

        assertAlive(prevBits);
        if (!hasWriter(prevBits)) {
            throw new IllegalStateException("Expected latch to be write locked. Got " + prevBits);
        }
    }

    private static boolean isAlive(long bits) {
        return (bits & DEAD_MASK) == 0;
    }

    private static void assertAlive(long bits) {
        if (!isAlive(bits)) {
            throw new IllegalStateException("Latch is dead");
        }
    }

    private static boolean hasReaders(long bits) {
        return (bits & READ_LOCK_MASK) != 0;
    }

    private static boolean hasWriter(long bits) {
        return (bits & WRITE_LOCK_MASK) != 0;
    }

    long treeNodeId() {
        return initialTreeNodeId;
    }

    private long getAcquireBits() {
        return (long) LOCK_BITS.getAcquire(this);
    }

    private long volatileGetBits() {
        return (long) LOCK_BITS.getVolatile(this);
    }

    public static long exponentialPark(long idleCounter) {
        if (idleCounter < SPIN_THRESHOLD) {
            Thread.onSpinWait();
        } else if (idleCounter < SHORT_PARK_THRESHOLD) {
            parkNanos(SHORT_PARK_TIME);
        } else {
            parkNanos(LONG_PARK_TIME);
            return LONG_PARK_COUNTER;
        }
        return idleCounter + 1;
    }

    @Override
    public String toString() {
        long bits = volatileGetBits();
        return format(
                "Lock[%d,w:%b,r:%d,refs:%d]",
                initialTreeNodeId, (bits & WRITE_LOCK_MASK) != 0, bits & READ_LOCK_MASK, (bits & REF_COUNT_MASK) >> 16);
    }
}
