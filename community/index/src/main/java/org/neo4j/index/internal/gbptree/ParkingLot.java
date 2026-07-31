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

import static java.lang.invoke.MethodHandles.lookup;
import static org.neo4j.internal.helpers.VarHandleUtils.getVarHandle;

import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;
import org.neo4j.hashing.HashFunction;
import org.neo4j.util.FeatureToggles;

/**
 * Process-global wait queues for {@link ParkingLatch}, futex-style: a fixed array of buckets, each a spin-locked
 * intrusive list of waiting threads, indexed by the identity of the latch waited on. Waiters sleep untimed and are
 * woken exactly once by the releasing thread.
 *
 * Lost wakeups are closed the way futexes close them: {@link #park(ParkingLatch, int)} re-checks the wait condition
 * under the bucket lock before sleeping and wakers dequeue under the same lock, so a waker that makes the
 * condition false before calling {@link #unparkAll(ParkingLatch, int)} can never miss a waiter.
 */
final class ParkingLot {

    private static final int BUCKET_COUNT =
            Integer.highestOneBit(Math.max(1, FeatureToggles.getInteger(ParkingLot.class, "buckets", 256)));
    private static final int BUCKET_MASK = BUCKET_COUNT - 1;
    private static final Bucket[] BUCKETS = new Bucket[BUCKET_COUNT];
    private static final HashFunction HASH = HashFunction.xorShift32();

    static {
        for (int i = 0; i < BUCKET_COUNT; i++) {
            BUCKETS[i] = new Bucket();
        }
    }

    private ParkingLot() {}

    /**
     * Blocks until {@link #unparkAll(ParkingLatch, int)} for the same latch and queue, or returns immediately if
     * {@code key.shouldPark(queue)} is false at enqueue time.
     */
    static void park(ParkingLatch key, int queue) {
        var waiter = new Waiter(key, queue);
        var bucket = bucketOf(key, queue);
        bucket.lock();
        try {
            if (!key.shouldPark(queue)) {
                return;
            }
            waiter.next = bucket.head;
            bucket.head = waiter;
        } finally {
            bucket.unlock();
        }
        boolean interrupted = false;
        while (!waiter.signalled) {
            LockSupport.park(key);
            if (Thread.interrupted()) {
                // Interrupts do not abort the wait and interrupted status is restored before return.
                // This is because acquireRead/acquireWrite have no failure mode
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Wakes every thread parked on the given latch and queue. The caller must have made the wait condition false
     * first, so racing would-be waiters fail the re-check instead of sleeping.
     */
    static void unparkAll(ParkingLatch key, int queue) {
        var bucket = bucketOf(key, queue);
        Waiter matched = null;
        bucket.lock();
        try {
            Waiter prev = null;
            var waiter = bucket.head;
            while (waiter != null) {
                var next = waiter.next;
                if (waiter.key == key && waiter.queue == queue) {
                    if (prev == null) {
                        bucket.head = next;
                    } else {
                        prev.next = next;
                    }
                    waiter.next = matched;
                    matched = waiter;
                } else {
                    prev = waiter;
                }
                waiter = next;
            }
        } finally {
            bucket.unlock();
        }
        while (matched != null) {
            matched.signalled = true;
            LockSupport.unpark(matched.thread);
            matched = matched.next;
        }
    }

    private static Bucket bucketOf(ParkingLatch key, int queue) {
        return BUCKETS[hash(key, queue) & BUCKET_MASK];
    }

    private static int hash(ParkingLatch key, int queue) {
        return HASH.hashSingleValueToInt(((long) System.identityHashCode(key) << 1) | queue);
    }

    private static final class Waiter {
        final Thread thread = Thread.currentThread();
        final ParkingLatch key;
        final int queue;
        Waiter next;
        volatile boolean signalled;

        Waiter(ParkingLatch key, int queue) {
            this.key = key;
            this.queue = queue;
        }
    }

    private static final class Bucket {
        @SuppressWarnings("unused")
        private volatile int lock;

        Waiter head;

        private static final VarHandle LOCK = getVarHandle(lookup(), "lock");

        void lock() {
            while (!LOCK.weakCompareAndSetAcquire(this, 0, 1)) {
                Thread.onSpinWait();
            }
        }

        void unlock() {
            LOCK.setRelease(this, 0);
        }
    }
}
