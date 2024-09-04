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
package org.neo4j.internal.id.indexed;

import static java.lang.Integer.max;
import static java.lang.invoke.MethodHandles.lookup;
import static org.neo4j.internal.helpers.VarHandleUtils.arrayElementVarHandle;
import static org.neo4j.internal.helpers.VarHandleUtils.consumeLong;
import static org.neo4j.internal.helpers.VarHandleUtils.getVarHandle;
import static org.neo4j.util.Preconditions.requirePowerOfTwo;

import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Concurrent bit-set for longs. Basically a {@link ConcurrentHashMap} where each map entry is a small bit-set.
 * Reading is done by making a snapshot of a map entry into a target long[]. Reading is lock-free and retry-spins as long as there's a writer on the particular
 * entry. Writers are exclusive and other concurrent writers will retry-spin for the lock until getting it.
 * Entries with all-zero bit-sets are removed by the writer removing the last bit.
 */
class ConcurrentSparseLongBitSet {
    private final ConcurrentHashMap<Long, Range> ranges = new ConcurrentHashMap<>();
    private final int idsPerEntry;
    private final int longsPerRange;

    ConcurrentSparseLongBitSet(int idsPerEntry) {
        requirePowerOfTwo(idsPerEntry);
        this.idsPerEntry = idsPerEntry;
        this.longsPerRange = max(1, ((idsPerEntry - 1) / Long.SIZE) + 1);
    }

    int getIdsPerEntry() {
        return idsPerEntry;
    }

    boolean set(long id, int slots, boolean value) {
        long idRange = id / idsPerEntry;
        int offset = (int) (id % idsPerEntry);
        do {
            Range range = ranges.computeIfAbsent(idRange, r -> new Range(longsPerRange));
            if (range.lock()) {
                boolean empty = false;
                try {
                    if (!range.set(offset, slots, value)) {
                        return false;
                    }
                    if (!value) {
                        empty = range.isEmpty();
                    }
                    return true;
                } finally {
                    if (!value && empty) {
                        range.close();
                        Range removedRange = ranges.remove(idRange);
                        assert removedRange == range;
                    } else {
                        range.unlock();
                    }
                }
            }
            // else this range is locked or closed, we'll just retry
        } while (true);
    }

    void snapshotRange(long idRange, long[] into) {
        long lockStampBefore;
        long lockStampAfter;
        do {
            Range range = ranges.get(idRange);
            if (range == null) {
                Arrays.fill(into, 0);
                return;
            }

            lockStampBefore = range.getLockStamp();
            if (range.isUnlocked()) {
                for (int i = 0; i < into.length; i++) {
                    into[i] = range.getLong(i);
                }
                lockStampAfter = range.getLockStamp();
            } else {
                lockStampAfter = lockStampBefore + 1;
            }
        } while (lockStampAfter != lockStampBefore);
    }

    int size() {
        return ranges.size();
    }

    private static class Range {
        private static final int STATUS_UNLOCKED = 0;
        private static final int STATUS_LOCKED = 1;
        private static final int STATUS_CLOSED = 2;

        /**
         * Accessed via unsafe so is effectively volatile and is updated atomically
         */
        private final long[] bits;

        private static final VarHandle BITS_ARRAY = arrayElementVarHandle(long[].class);

        private final int longs;

        /**
         * 0=unlocked, 1=locked, 2=empty and dead.
         */
        @SuppressWarnings("unused") // accessed via VarHandle
        private int status;

        private static final VarHandle STATUS = getVarHandle(lookup(), "status");

        @SuppressWarnings("unused") // accessed via VarHandle
        private long lockStamp;

        private static final VarHandle LOCK_STAMP = getVarHandle(lookup(), "lockStamp");

        private Range(int longs) {
            this.longs = longs;
            // [0..longs]:       the actual bitset bits
            // [longs..longs*2]: temp bits
            this.bits = new long[longs * 2];
        }

        /**
         * @return {@code false} if this range is either locked or dead, otherwise {@code true} if it was locked and now owned by this thread.
         */
        private boolean lock() {
            boolean locked = STATUS.compareAndSet(this, STATUS_UNLOCKED, STATUS_LOCKED);
            if (locked) {
                consumeLong((long) LOCK_STAMP.getAndAdd(this, 1L));
            }
            return locked;
        }

        private void unlock() {
            boolean unlocked = STATUS.compareAndSet(this, STATUS_LOCKED, STATUS_UNLOCKED);
            assert unlocked;
        }

        private void close() {
            boolean closed = STATUS.compareAndSet(this, STATUS_LOCKED, STATUS_CLOSED);
            assert closed;
        }

        private long getLong(int arrayIndex) {
            return (long) BITS_ARRAY.getVolatile(bits, arrayIndex);
        }

        private void setLong(int arrayIndex, long value) {
            BITS_ARRAY.setVolatile(bits, arrayIndex, value);
        }

        /**
         * Sets a range of bits to either 0 (value == false), or 1 (value == true). All bits must be changed, otherwise {@code false} is returned
         * and no bits will be changed.
         *
         * @param start start bit to start changing from.
         * @param slots number of bits from start offset to change.
         * @param value {@code true} for 1, {@code false} for 0.
         * @return {@code true} if all bits between start and start+slots were the opposite of specified by {@code value}, such that all
         * bits were changed, otherwise {@code false}.
         */
        boolean set(int start, int slots, boolean value) {
            Arrays.fill(bits, longs, bits.length, 0);
            BitsUtil.setBits(bits, start, slots, longs);
            if (value) {
                // First check
                for (int i = 0; i < longs; i++) {
                    if ((getLong(i) & bits[longs + i]) != 0) {
                        return false;
                    }
                }

                // Then set
                for (int i = 0; i < longs; i++) {
                    setLong(i, getLong(i) | bits[longs + i]);
                }
            } else {
                // First check
                for (int i = 0; i < longs; i++) {
                    if ((getLong(i) & bits[longs + i]) != bits[longs + i]) {
                        return false;
                    }
                }

                // Then set
                for (int i = 0; i < longs; i++) {
                    setLong(i, getLong(i) & ~bits[longs + i]);
                }
            }
            return true;
        }

        private boolean isEmpty() {
            for (int i = 0; i < longs; i++) {
                if (getLong(i) != 0) {
                    return false;
                }
            }
            return true;
        }

        long getLockStamp() {
            return (long) LOCK_STAMP.getVolatile(this);
        }

        boolean isUnlocked() {
            return (int) STATUS.getVolatile(this) == STATUS_UNLOCKED;
        }
    }
}
