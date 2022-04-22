/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.neo4j.internal.unsafe.UnsafeUtil;

/**
 * Latch which acquires and releases read/write locks using compare-and-swap on a {@code long} field.
 * Threads trying to make changes to the latch will busy-spin for a while and then go into timed waiting.
 * <p>
 * Latches have an initial tree node id and last thread releasing the last acquisition will reset the tree node id bits to 0
 * and run a {@code removeAction}. A latch with its tree node id not equal to initial tree node id may not be acquired anymore.
 * This functionality allows these latches to be used in e.g. a {@link ConcurrentHashMap}.
 */
class LongSpinLatch {
    private static final long BITS_OFFSET = UnsafeUtil.getFieldOffset(LongSpinLatch.class, "bits");
    private static final long MAX_SPIN_NANOS = MILLISECONDS.toNanos(10);
    private static final long PARK_NANOS = MILLISECONDS.toNanos(1);
    private static final long TEST_FAILED = -1;
    private static final long DEAD = -2;
    static final int WRITE_LATCH_DEAD = -1;
    static final int WRITE_LATCH_ACQUIRED = 1;
    static final int WRITE_LATCH_NOT_ACQUIRED = 0;

    private static final long WRITE_LOCK_MASK = 0x8000;
    private static final long READ_LOCK_MASK = 0x7FFF;
    private static final long LOCK_MASK = WRITE_LOCK_MASK | READ_LOCK_MASK;
    private static final long TREE_NODE_ID_MASK = ~LOCK_MASK;
    private static final int TREE_NODE_ID_SHIFT = 16;

    private static final LongPredicate ALWAYS_TRUE = bits -> true;
    private static final LongPredicate NO_LOCK = bits -> (bits & LOCK_MASK) == 0;
    private static final LongPredicate NO_WRITE_LOCK = bits -> (bits & WRITE_LOCK_MASK) == 0;
    private static final LongPredicate NO_WRITE_ONLY_MY_READ_LOCK = bits -> (bits & LOCK_MASK) == 1;
    private static final LongPredicate NO_READ_LOCK = bits -> (bits & READ_LOCK_MASK) == 0;

    private static final LongToLongFunction ACQUIRE_READ_LOCK = bits -> bits + 1;
    private static final LongToLongFunction RELEASE_READ_LOCK = bits -> markAsDeadIfNoLocksLeft(bits - 1);
    private static final LongToLongFunction ACQUIRE_WRITE_LOCK = bits -> bits | WRITE_LOCK_MASK;
    private static final LongToLongFunction ACQUIRE_WRITE_LOCK_CLEAR_READ_LOCKS =
            bits -> (bits & TREE_NODE_ID_MASK) | WRITE_LOCK_MASK;
    private static final LongToLongFunction RELEASE_WRITE_LOCK =
            bits -> markAsDeadIfNoLocksLeft(bits & ~WRITE_LOCK_MASK);
    private static final LongToLongFunction NO_TRANSFORM = bits -> bits;

    private final long initialTreeNodeId;
    private final LongConsumer removeAction;
    // Bits that make up this lock:
    // [    ,    ][    ,    ][    ,    ][    ,    ][    ,    ][    ,    ][    ,    ][    ,    ]
    //  ▲                                                              ▲  ▲▲                 ▲
    //  └────────────────────────── Tree node id ──────────────────────┘  │└─ Read lock bits ┘
    //                                                                    └── Write lock bit
    //
    // 48 tree node id bits => 281 trillion, 2 quintillion bytes, which is more than the page cache supports
    // 15 read lock bits => max 32769 concurrent threads holding a single read lock. Should be good for anyone
    private long bits;

    /**
     * Instantiates a latch which can be acquired, with one or more acquisitions, released, acquired again until finally
     * a release leaving no acquisitions left - being marked as dead and unable to be acquired after that point.
     * @param treeNodeId tree node id this latch is for.
     * @param removeAction action to run when marked as dead, i.e. when last acquisition is released.
     */
    LongSpinLatch(long treeNodeId, LongConsumer removeAction) {
        assert treeNodeId > 0;
        bits = treeNodeId << TREE_NODE_ID_SHIFT;
        initialTreeNodeId = treeNodeId;
        this.removeAction = removeAction;
    }

    /**
     * Blocking call.
     * @return the read lock count this resulted in, > 0 if successful, otherwise 0 meaning that an acquisition on a dead lock was attempted.
     */
    int acquireRead() {
        long transformed = spinTransform(NO_WRITE_LOCK, ACQUIRE_READ_LOCK, false);
        if (transformed == DEAD) {
            return 0;
        }
        return toIntExact(transformed & READ_LOCK_MASK);
    }

    /**
     * Non-blocking call.
     * @return the read lock count this resulted in. 0 means this was the last read lock held.
     */
    int releaseRead() {
        long transformed = spinTransform(ALWAYS_TRUE, RELEASE_READ_LOCK, false);
        checkRemove(transformed);
        return toIntExact(transformed & READ_LOCK_MASK);
    }

    private void checkRemove(long transformed) {
        if (transformed == 0) {
            removeAction.accept(initialTreeNodeId);
        }
    }

    /**
     * Non-blocking call.
     * Given that a read lock is already acquired, upgrade it to a write lock.
     * @return whether or not the lock was upgraded. Returns {@code false} for scenarios which would result in deadlock.
     */
    boolean tryUpgradeToWrite() {
        return spinTransform(NO_WRITE_ONLY_MY_READ_LOCK, ACQUIRE_WRITE_LOCK_CLEAR_READ_LOCKS, true) != TEST_FAILED;
    }

    /**
     * Blocking call.
     * @return true if write lock was acquired, otherwise false which means that acquisition was attempted on a dead lock.
     */
    boolean acquireWrite() {
        long writeLockResult = spinTransform(NO_WRITE_LOCK, ACQUIRE_WRITE_LOCK, false);
        if (writeLockResult == DEAD) {
            return false;
        }
        spinTransform(NO_READ_LOCK, NO_TRANSFORM, false);
        return true;
    }

    /**
     * Tries to acquire write latch.
     * @return {@link #WRITE_LATCH_ACQUIRED} on success, {@link #WRITE_LATCH_NOT_ACQUIRED} if someone else has acquired either write or read latch,
     * or {@link #WRITE_LATCH_DEAD} if this latch is dead meaning that a new latch will have to be created by the caller to retry this operation.
     */
    int tryAcquireWrite() {
        long writeLockResult = spinTransform(NO_WRITE_LOCK, ACQUIRE_WRITE_LOCK, true);
        if (writeLockResult == DEAD) {
            return WRITE_LATCH_DEAD;
        }
        if (writeLockResult == TEST_FAILED) {
            return WRITE_LATCH_NOT_ACQUIRED;
        }
        return WRITE_LATCH_ACQUIRED;
    }

    /**
     * Non-blocking call. Releases the write lock on this latch.
     */
    void releaseWrite() {
        long transformed = spinTransform(ALWAYS_TRUE, RELEASE_WRITE_LOCK, false);
        checkRemove(transformed);
    }

    long treeNodeId() {
        return treeNodeIdFromBits(volatileGetBits());
    }

    private long spinTransform(LongPredicate tester, LongToLongFunction transformer, boolean breakOnTestFail) {
        long bits;
        long transformedBits;
        long timedWaitStartTime = 0;
        while (true) {
            bits = volatileGetBits();
            long l = treeNodeIdFromBits(bits);
            if (l != initialTreeNodeId) {
                return DEAD;
            }

            if (!tester.test(bits)) {
                if (breakOnTestFail) {
                    return TEST_FAILED;
                }
                if (timedWaitStartTime == 0) {
                    timedWaitStartTime = nanoTime() + MAX_SPIN_NANOS;
                } else if (nanoTime() > timedWaitStartTime) {
                    // We've spun a while, still the test returns false. Let's go into timed waiting.
                    LockSupport.parkNanos(PARK_NANOS);
                }
                Thread.onSpinWait();
            } else {
                transformedBits = transformer.applyAsLong(bits);
                if (UnsafeUtil.compareAndSwapLong(this, BITS_OFFSET, bits, transformedBits)) {
                    break;
                }
                // Continue spinning here since we're close to making the transformation,
                // it's just that we're currently competing with others also trying to do this.
            }
        }
        return transformedBits;
    }

    private static long treeNodeIdFromBits(long bits) {
        return (bits & TREE_NODE_ID_MASK) >>> TREE_NODE_ID_SHIFT;
    }

    private static long markAsDeadIfNoLocksLeft(long result) {
        return (result & LOCK_MASK) == 0 ? 0 : result;
    }

    private long volatileGetBits() {
        return UnsafeUtil.getLongVolatile(this, BITS_OFFSET);
    }

    @Override
    public String toString() {
        long bits = volatileGetBits();
        return format(
                "Lock[%d,w:%b,r:%d]", treeNodeIdFromBits(bits), (bits & WRITE_LOCK_MASK) != 0, bits & READ_LOCK_MASK);
    }
}
