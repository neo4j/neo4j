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

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;

/**
 * Latch which acquires and releases read/write locks using compare-and-swap on a {@code long} field.
 * Threads trying to make changes to the latch will busy-spin for a while and then go into timed waiting.
 * <p>
 * Latches have an initial tree node id and last thread releasing the last acquisition will reset the tree node id bits to 0
 * and run a {@code removeAction}. A latch with its tree node id not equal to initial tree node id may not be acquired anymore.
 * This functionality allows these latches to be used in e.g. a {@link ConcurrentHashMap}.
 */
class LongSpinLatch {
    private static final long MAX_SPIN_NANOS = MICROSECONDS.toNanos(500);
    private static final long PARK_NANOS = MICROSECONDS.toNanos(100);
    private static final long TEST_FAILED = -1;
    private static final long DEAD = -2;

    private static final long WRITE_LOCK_MASK = 0x8000;
    private static final long READ_LOCK_MASK = 0x7FFF;
    private static final long LOCK_MASK = WRITE_LOCK_MASK | READ_LOCK_MASK;
    private static final long REF_COUNT_MASK = 0x7FFF0000;
    private static final int REF_COUNT_SHIFT = 16;
    private static final long DEAD_MASK = 0x80000000L;

    private static final LongPredicate ALWAYS_TRUE = bits -> true;
    private static final LongPredicate NO_WRITE_LOCK = bits -> (bits & WRITE_LOCK_MASK) == 0;
    private static final LongPredicate NO_WRITE_ONLY_ONE_READ_LOCK = bits -> (bits & LOCK_MASK) == 1;
    private static final LongPredicate NO_READ_LOCK = bits -> (bits & READ_LOCK_MASK) == 0;

    private static final LongToLongFunction ACQUIRE_READ_LOCK = bits -> {
        assert (bits & READ_LOCK_MASK) < READ_LOCK_MASK : "Too many readers";
        return bits + 1;
    };
    private static final LongToLongFunction RELEASE_READ_LOCK = bits -> {
        assert (bits & READ_LOCK_MASK) > 0 : "No readers";
        return bits - 1;
    };
    private static final LongToLongFunction ACQUIRE_WRITE_LOCK = bits -> bits | WRITE_LOCK_MASK;
    private static final LongToLongFunction ACQUIRE_WRITE_LOCK_CLEAR_READ_LOCKS =
            bits -> (bits & ~LOCK_MASK) | WRITE_LOCK_MASK;
    private static final LongToLongFunction RELEASE_WRITE_LOCK = bits -> bits & ~WRITE_LOCK_MASK;
    private static final LongToLongFunction NO_TRANSFORM = bits -> bits;

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

    private static final VarHandle LOCK_BITS;

    static {
        try {
            LOCK_BITS = MethodHandles.lookup().findVarHandle(LongSpinLatch.class, "lockBits", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Instantiates a latch which can be acquired, with one or more acquisitions, released, acquired again until finally
     * a release leaving no acquisitions left - being marked as dead and unable to be acquired after that point.
     * @param removeAction action to run when marked as dead, i.e. when last acquisition is released.
     */
    LongSpinLatch(long initialTreeNodeId, LongConsumer removeAction) {
        this.initialTreeNodeId = initialTreeNodeId;
        this.removeAction = removeAction;
    }

    boolean ref() {
        var result = spinTransform(
                ALWAYS_TRUE,
                bits -> {
                    var refCount = (bits & REF_COUNT_MASK) >>> REF_COUNT_SHIFT;
                    refCount++;
                    assert ((refCount << REF_COUNT_SHIFT) & REF_COUNT_MASK) == (refCount << REF_COUNT_SHIFT) : refCount;
                    return (bits & ~REF_COUNT_MASK) | (refCount << REF_COUNT_SHIFT);
                },
                false,
                true);
        return result != DEAD;
    }

    void deref() {
        var result = spinTransform(
                ALWAYS_TRUE,
                bits -> {
                    var refCount = (bits & REF_COUNT_MASK) >>> REF_COUNT_SHIFT;
                    refCount--;
                    assert refCount >= 0;
                    if (refCount == 0) {
                        assert (bits & LOCK_MASK) == 0;
                        bits |= DEAD_MASK;
                    }
                    return (bits & ~REF_COUNT_MASK) | (refCount << REF_COUNT_SHIFT);
                },
                false,
                false);
        boolean lastRef = (result & DEAD_MASK) != 0;
        if (lastRef) {
            removeAction.accept(initialTreeNodeId);
        }
    }

    /**
     * Blocking call.
     * @return the read lock count this resulted in, > 0 if successful, otherwise 0 meaning that an acquisition on a dead lock was attempted.
     */
    int acquireRead() {
        long transformed = spinTransform(NO_WRITE_LOCK, ACQUIRE_READ_LOCK, false, false);
        return toIntExact(transformed & READ_LOCK_MASK);
    }

    /**
     * Non-blocking call.
     * @return the read lock count this resulted in. 0 means this was the last read lock held.
     */
    int releaseRead() {
        long transformed = spinTransform(ALWAYS_TRUE, RELEASE_READ_LOCK, false, false);
        return toIntExact(transformed & READ_LOCK_MASK);
    }

    /**
     * Non-blocking call.
     * Given that a read lock is already acquired, upgrade it to a write lock.
     * @return whether or not the lock was upgraded. Returns {@code false} for scenarios which would result in deadlock.
     */
    boolean tryUpgradeToWrite() {
        return spinTransform(NO_WRITE_ONLY_ONE_READ_LOCK, ACQUIRE_WRITE_LOCK_CLEAR_READ_LOCKS, true, false)
                != TEST_FAILED;
    }

    /**
     * Blocking call.
     * @return true if write lock was acquired, otherwise false which means that acquisition was attempted on a dead lock.
     */
    boolean acquireWrite() {
        boolean writeAcquired = false;
        boolean success = false;
        try {
            spinTransform(NO_WRITE_LOCK, ACQUIRE_WRITE_LOCK, false, false);
            writeAcquired = true;
            spinTransform(NO_READ_LOCK, NO_TRANSFORM, false, false);
            success = true;
        } finally {
            if (!success && writeAcquired) {
                spinTransform(ALWAYS_TRUE, RELEASE_WRITE_LOCK, false, true);
            }
        }
        return true;
    }

    /**
     * Tries to acquire write latch.
     */
    boolean tryAcquireWrite() {
        return spinTransform(NO_WRITE_LOCK, ACQUIRE_WRITE_LOCK, true, false) != TEST_FAILED;
    }

    /**
     * Non-blocking call. Releases the write lock on this latch.
     */
    void releaseWrite() {
        spinTransform(ALWAYS_TRUE, RELEASE_WRITE_LOCK, false, false);
    }

    long treeNodeId() {
        return initialTreeNodeId;
    }

    private long spinTransform(
            LongPredicate tester, LongToLongFunction transformer, boolean breakOnTestFail, boolean allowOnDead) {
        long bits;
        long transformedBits;
        long timedWaitStartTime = 0;
        while (true) {
            bits = volatileGetBits();
            if ((bits & DEAD_MASK) != 0) {
                if (!allowOnDead) {
                    throw new IllegalStateException("Tried to transform a dead latch");
                } else {
                    return DEAD;
                }
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
                if (LOCK_BITS.compareAndSet(this, bits, transformedBits)) {
                    break;
                }
                // Continue spinning here since we're close to making the transformation,
                // it's just that we're currently competing with others also trying to do this.
            }
        }
        return transformedBits;
    }

    private long volatileGetBits() {
        return (long) LOCK_BITS.getVolatile(this);
    }

    /**
     * @return if this latch is currently write locked, or if there's currently an ongoing
     * {@link #acquireWrite()} call that awaits all reads to be released.
     */
    boolean hasWrite() {
        return !NO_WRITE_LOCK.test(volatileGetBits());
    }

    @Override
    public String toString() {
        long bits = volatileGetBits();
        return format("Lock[%d,w:%b,r:%d]", initialTreeNodeId, (bits & WRITE_LOCK_MASK) != 0, bits & READ_LOCK_MASK);
    }
}
