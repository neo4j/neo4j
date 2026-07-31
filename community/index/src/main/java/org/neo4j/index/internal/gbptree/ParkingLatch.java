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
import static org.neo4j.internal.helpers.VarHandleUtils.getVarHandle;

import java.lang.invoke.VarHandle;
import java.util.function.LongConsumer;

/**
 * Latch which acquires and releases read/write locks using compare-and-swap on a {@code long} field,
 * waiting through the {@link ParkingLot}: uncontended operations cost a single atomic CAS,
 * contended waiters spin a short budget and then sleep untimed until the releasing thread wakes them.
 *
 * Bit layout:
 *
 *  63                34 33 32 31 30           16 15 14            0
 * +--------------------+--+--+--+---------------+--+---------------+
 * |       unused       |DP|FP|DE|   ref count   |WL|  read count   |
 * +--------------------+--+--+--+---------------+--+---------------+
 *
 * WL - the write lock
 * DE - the dead marker
 * FP - the flag-parked hint
 * DP - the drain-parked hint.
 */
final class ParkingLatch implements TreeNodeLatch {
    private static final int SPIN_BUDGET = Runtime.getRuntime().availableProcessors() < 2 ? 1 : 256;

    private static final long WRITE_LOCK_MASK = 0x8000;
    private static final long READ_LOCK_MASK = 0x7FFF;
    private static final long LOCK_MASK = WRITE_LOCK_MASK | READ_LOCK_MASK;
    private static final long REF_COUNT_MASK = 0x7FFF0000;
    private static final long REF_COUNT_UNIT = 0x00010000;
    private static final long DEAD_MASK = 0x80000000L;
    private static final long FLAG_PARKED_MASK = 1L << 32;
    private static final long DRAIN_PARKED_MASK = 1L << 33;
    private static final long PARKED_MASK = FLAG_PARKED_MASK | DRAIN_PARKED_MASK;

    private static final int FLAG_QUEUE = 0;
    private static final int DRAIN_QUEUE = 1;

    private final long treeNodeId;
    private final LongConsumer removeAction;

    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile long bits;

    private static final VarHandle BITS = getVarHandle(lookup(), "bits");

    ParkingLatch(long treeNodeId, LongConsumer removeAction) {
        this.treeNodeId = treeNodeId;
        this.removeAction = removeAction;
    }

    boolean ref() {
        while (true) {
            long prev = getAcquireBits();
            if (!isAlive(prev) || (prev & REF_COUNT_MASK) == REF_COUNT_MASK) {
                return false;
            }
            if (BITS.weakCompareAndSetRelease(this, prev, prev + REF_COUNT_UNIT)) {
                return true;
            }
            Thread.onSpinWait();
        }
    }

    @Override
    public void deref() {
        while (true) {
            long prev = getAcquireBits();
            if ((prev & REF_COUNT_MASK) == 0) {
                throw new IllegalStateException("Called 'deref()' on a latch without a matching 'ref()'");
            }
            long next = prev - REF_COUNT_UNIT;
            boolean lastRef = (next & REF_COUNT_MASK) == 0;
            if (lastRef) {
                if ((next & (LOCK_MASK | PARKED_MASK)) != 0) {
                    throw new IllegalStateException(
                            "Called 'deref()' on the last reference of a latch that is still acquired. Got " + prev);
                }
                next = DEAD_MASK;
            }
            if (BITS.weakCompareAndSetRelease(this, prev, next)) {
                if (lastRef) {
                    removeAction.accept(treeNodeId);
                }
                return;
            }
            Thread.onSpinWait();
        }
    }

    @Override
    public long acquireRead() {
        int spins = 0;
        while (true) {
            long prev = getAcquireBits();
            assertAlive(prev);
            if ((prev & READ_LOCK_MASK) == READ_LOCK_MASK) {
                throw new IllegalStateException("Too many readers");
            }
            if (hasWriter(prev)) {
                spins = awaitWriterFlagClear(spins);
                continue;
            }
            if (BITS.weakCompareAndSetAcquire(this, prev, prev + 1)) {
                return (prev & READ_LOCK_MASK) + 1;
            }
            Thread.onSpinWait();
        }
    }

    @Override
    public long releaseRead() {
        while (true) {
            long prev = getAcquireBits();
            if (!hasReaders(prev)) {
                throw new IllegalStateException("Called 'releaseRead()' on a latch without a reader. Got " + prev);
            }
            long next = prev - 1;
            if (BITS.weakCompareAndSetRelease(this, prev, next)) {
                long remaining = next & READ_LOCK_MASK;
                if (remaining == 0 && (next & DRAIN_PARKED_MASK) != 0) {
                    ParkingLot.unparkAll(this, DRAIN_QUEUE);
                }
                return remaining;
            }
            Thread.onSpinWait();
        }
    }

    @Override
    public boolean tryUpgradeToWrite() {
        long prev = getAcquireBits();
        assertAlive(prev);
        if ((prev & LOCK_MASK) == 1) {
            return BITS.compareAndSet(this, prev, (prev & ~LOCK_MASK) | WRITE_LOCK_MASK);
        }
        return false;
    }

    @Override
    public boolean couldUpgradeToWrite() {
        long prev = getAcquireBits();
        assertAlive(prev);
        return (prev & LOCK_MASK) == 1;
    }

    @Override
    public void acquireWrite() {
        int spins = 0;
        long prev;
        while (true) {
            prev = (long) BITS.getAndBitwiseOrAcquire(this, WRITE_LOCK_MASK);
            assertAlive(prev);
            if (!hasWriter(prev)) {
                break;
            }
            spins = awaitWriterFlagClear(spins);
        }
        awaitReadersDrained(prev);
    }

    @Override
    public boolean tryAcquireWrite() {
        long prev = getAcquireBits();
        assertAlive(prev);
        if ((prev & LOCK_MASK) == 0) {
            return BITS.compareAndSet(this, prev, prev | WRITE_LOCK_MASK);
        }
        return false;
    }

    @Override
    public void releaseWrite() {
        long prev =
                (long) BITS.getAndBitwiseAndRelease(this, ~(WRITE_LOCK_MASK | FLAG_PARKED_MASK | DRAIN_PARKED_MASK));
        assertAlive(prev);
        if (!hasWriter(prev)) {
            throw new IllegalStateException("Expected latch to be write locked. Got " + prev);
        }
        if ((prev & FLAG_PARKED_MASK) != 0) {
            ParkingLot.unparkAll(this, FLAG_QUEUE);
        }
    }

    /**
     * Re-evaluates the wait condition for one of the parking lot's queues. Runs under the bucket lock, so it must
     * stay cheap and non-blocking.
     */
    boolean shouldPark(int queue) {
        long current = getAcquireBits();
        return queue == FLAG_QUEUE ? (current & FLAG_PARKED_MASK) != 0 : hasReaders(current);
    }

    private int awaitWriterFlagClear(int spins) {
        if (spins < SPIN_BUDGET) {
            Thread.onSpinWait();
            return spins + 1;
        }
        while (true) {
            long prev = getAcquireBits();
            if (!hasWriter(prev)) {
                return spins;
            }
            if ((prev & FLAG_PARKED_MASK) != 0 || BITS.weakCompareAndSetRelease(this, prev, prev | FLAG_PARKED_MASK)) {
                break;
            }
            Thread.onSpinWait();
        }
        ParkingLot.park(this, FLAG_QUEUE);
        return spins;
    }

    private void awaitReadersDrained(long current) {
        int spins = 0;
        while (hasReaders(current)) {
            if (spins < SPIN_BUDGET) {
                assert isAlive(current) : "latch died while draining readers";
                Thread.onSpinWait();
                spins++;
            } else {
                long ignored = (long) BITS.getAndBitwiseOrRelease(this, DRAIN_PARKED_MASK);
                ParkingLot.park(this, DRAIN_QUEUE);
            }
            current = getAcquireBits();
        }
    }

    @Override
    public long treeNodeId() {
        return treeNodeId;
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

    private long getAcquireBits() {
        return (long) BITS.getAcquire(this);
    }

    @Override
    public String toString() {
        long current = getAcquireBits();
        return format(
                "Latch[%d,w:%b,r:%d,refs:%d,parked:%s%s%s]",
                treeNodeId,
                hasWriter(current),
                current & READ_LOCK_MASK,
                (current & REF_COUNT_MASK) >> 16,
                (current & FLAG_PARKED_MASK) != 0 ? "F" : "",
                (current & DRAIN_PARKED_MASK) != 0 ? "D" : "",
                isAlive(current) ? "" : ",dead");
    }
}
