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
package org.neo4j.util.concurrent;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.internal.helpers.Numbers;

/**
 * A crude, synchronized implementation of OutOfOrderSequence. Please implement a faster one if need be.
 */
public class ArrayQueueOutOfOrderSequence implements OutOfOrderSequence {
    private final SequenceArray outOfOrderQueue;
    private final AtomicReference<NumberWithMeta> highestGapFreeNumber;
    private final AtomicLong highestEverSeen;
    private final AtomicReference<ReverseSnapshot> reverseSnapshot;

    public ArrayQueueOutOfOrderSequence(long startingNumber, int initialArraySize, Meta initialMeta) {
        this.highestGapFreeNumber = new AtomicReference<>(new NumberWithMeta(startingNumber, initialMeta));
        this.highestEverSeen = new AtomicLong(startingNumber);
        this.outOfOrderQueue = new SequenceArray(Numbers.ceilingPowerOfTwo(initialArraySize));
        this.reverseSnapshot =
                new AtomicReference<>(new ReverseSnapshot(startingNumber, startingNumber, EMPTY_LONG_ARRAY));
    }

    @Override
    public synchronized boolean offer(long number, Meta meta) {
        highestEverSeen.setRelease(Math.max(highestEverSeen.getAcquire(), number));
        NumberWithMeta localGapFree = highestGapFreeNumber.getAcquire();
        if (localGapFree.number() + 1 == number) {
            highestGapFreeNumber.setRelease(outOfOrderQueue.pollHighestGapFree(number, meta));
            reverseSnapshot.setRelease(null);
            return true;
        }

        if (number <= localGapFree.number()) {
            throw new IllegalStateException("Was offered " + number + ", but highest gap-free is "
                    + highestGapFreeNumber + " and was only expecting values higher than that");
        }
        outOfOrderQueue.offer(localGapFree.number(), number, meta);
        reverseSnapshot.setRelease(null);
        return false;
    }

    @Override
    public long highestEverSeen() {
        return this.highestEverSeen.getAcquire();
    }

    @Override
    public NumberWithMeta get() {
        return highestGapFreeNumber.getAcquire();
    }

    @Override
    public long getHighestGapFreeNumber() {
        return highestGapFreeNumber.getAcquire().number();
    }

    @Override
    public synchronized void set(long number, Meta meta) {
        highestEverSeen.setRelease(number);
        highestGapFreeNumber.setRelease(new NumberWithMeta(number, meta));
        outOfOrderQueue.clear();
    }

    @Override
    public synchronized Snapshot snapshot() {
        return new Snapshot(highestGapFreeNumber.getAcquire().number(), outOfOrderQueue.snapshot());
    }

    @Override
    public ReverseSnapshot reverseSnapshot() {
        var rs = reverseSnapshot.getAcquire();
        if (rs != null) {
            return rs;
        }
        synchronized (this) {
            rs = reverseSnapshot.getAcquire();
            if (rs != null) {
                return rs;
            }
            rs = createReverseSnapshot();
            reverseSnapshot.setRelease(rs);
            return rs;
        }
    }

    private ReverseSnapshot createReverseSnapshot() {
        long gapFree = highestGapFreeNumber.getAcquire().number();
        long everSeen = highestEverSeen.getAcquire();
        if (everSeen == gapFree) {
            return new ReverseSnapshot(gapFree, everSeen, EMPTY_LONG_ARRAY);
        }
        long[] missingNumbers = outOfOrderQueue.missingItems(gapFree);
        return new ReverseSnapshot(gapFree, everSeen, missingNumbers);
    }

    @Override
    public synchronized String toString() {
        return String.format(
                "out-of-order-sequence:%d %d [%s]",
                highestEverSeen.getAcquire(), highestGapFreeNumber.getAcquire().number(), outOfOrderQueue);
    }
}
