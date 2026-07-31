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

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.Test;

class ParkingLatchTest extends TreeNodeLatchTestBase {
    private static final long TIMEOUT_SECONDS = 60;
    private static final long AWAIT_TIMEOUT_NANOS = SECONDS.toNanos(TIMEOUT_SECONDS);
    private static final long JOIN_TIMEOUT_MILLIS = SECONDS.toMillis(TIMEOUT_SECONDS);
    private static final long WRITER_HOLD_NANOS = MICROSECONDS.toNanos(20);
    private static final int REJECTED_RELEASES = 10_000;

    @Override
    TreeNodeLatch newLatch(long treeNodeId, LongConsumer removeAction) {
        var latch = new ParkingLatch(treeNodeId, removeAction);
        assertTrue(latch.ref());
        return latch;
    }

    @Override
    boolean ref(TreeNodeLatch latch) {
        return ((ParkingLatch) latch).ref();
    }

    @Override
    Class<?> latchClass() {
        return ParkingLatch.class;
    }

    @Override
    ParkingLatch latch() {
        return (ParkingLatch) super.latch();
    }

    @Test
    void shouldFailDerefOfLastReferenceWhileStillAcquired() {
        var latch = latch();

        latch.acquireRead();
        assertThatThrownBy(latch::deref).isInstanceOf(IllegalStateException.class);
        latch.releaseRead();

        latch.acquireWrite();
        assertThatThrownBy(latch::deref).isInstanceOf(IllegalStateException.class);
        latch.releaseWrite();

        latch.deref();
        assertEquals(1, removeAction.count.get());
        assertEquals(TREE_NODE_ID, removeAction.captured);
    }

    @Test
    void shouldRejectReleaseReadWithoutAReader() {
        var latch = latch();

        assertThatThrownBy(latch::releaseRead).isInstanceOf(IllegalStateException.class);

        latch.acquireRead();
        assertEquals(0, latch.releaseRead());
        latch.deref();
    }

    @Test
    void shouldNotPublishBorrowedStateWhenReleaseReadIsRejected() throws Exception {
        var latch = latch();
        var idle = latch.toString();
        var done = new AtomicBoolean();
        var observed = new AtomicReference<String>();
        var observer = new Thread(() -> {
            while (!done.get()) {
                var sample = latch.toString();
                if (!sample.equals(idle)) {
                    observed.compareAndSet(null, sample);
                }
            }
        });
        observer.start();

        try {
            for (int i = 0; i < REJECTED_RELEASES; i++) {
                assertThatThrownBy(latch::releaseRead).isInstanceOf(IllegalStateException.class);
            }
        } finally {
            done.set(true);
        }
        joinAll(new Thread[] {observer}, "observer never finished");
        assertNull(observed.get(), () -> "rejected releaseRead published " + observed.get());
        latch.deref();
    }

    @Test
    void shouldWakeEveryReaderParkedBehindAWriter() throws Exception {
        var latch = latch();
        latch.acquireWrite();
        var readers = startParkedReaders(latch, 8);

        latch.releaseWrite();

        joinAll(readers, "reader left parked behind a released writer");
        latch.deref();
    }

    @Test
    void shouldNotWakeReadersParkedOnAnotherLatch() throws Exception {
        var woken = latch();
        var parked = new ParkingLatch(2, removeAction);
        assertTrue(parked.ref());
        woken.acquireWrite();
        parked.acquireWrite();
        var wokenReader = startParkedReaders(woken, 1);
        var parkedReader = startParkedReaders(parked, 1);

        woken.releaseWrite();

        joinAll(wokenReader, "reader left parked behind a released writer");
        assertTrue(parkedReader[0].isAlive(), "reader got in while its own latch was still write locked");

        parked.releaseWrite();
        joinAll(parkedReader, "reader left parked behind a released writer");
        woken.deref();
        parked.deref();
    }

    @Test
    void shouldWakeOnlyTheDrainingWriterWhenTheLastReaderLeaves() throws Exception {
        var latch = latch();
        latch.acquireRead();
        var writerAcquired = new AtomicBoolean();
        var writer = new Thread(() -> {
            latch.acquireWrite();
            writerAcquired.set(true);
        });
        writer.start();
        awaitParked(writer);
        var reader = startParkedReaders(latch, 1);

        latch.releaseRead();

        writer.join(JOIN_TIMEOUT_MILLIS);
        assertTrue(writerAcquired.get(), "writer never woken by the last reader leaving");
        assertTrue(reader[0].isAlive(), "reader got in while the drained writer held the latch");

        latch.releaseWrite();
        joinAll(reader, "reader left parked behind a released writer");
        latch.deref();
    }

    @Test
    void shouldWakeWaitersAcrossManyLatches() throws Exception {
        int latchCount = 16;
        var latches = new ParkingLatch[latchCount];
        var readers = new Thread[latchCount * 2];
        for (int i = 0; i < latchCount; i++) {
            latches[i] = new ParkingLatch(i, removeAction);
            assertTrue(latches[i].ref());
            latches[i].acquireWrite();
        }
        for (int i = 0; i < readers.length; i++) {
            readers[i] = startParkedReaders(latches[i % latchCount], 1)[0];
        }

        for (var latch : latches) {
            latch.releaseWrite();
        }

        joinAll(readers, "reader stranded across many latches");
        for (var latch : latches) {
            latch.deref();
        }
    }

    private static Thread[] startParkedReaders(ParkingLatch latch, int count) {
        var readers = new Thread[count];
        for (int i = 0; i < count; i++) {
            readers[i] = new Thread(() -> {
                latch.acquireRead();
                latch.releaseRead();
            });
            readers[i].start();
        }
        for (var reader : readers) {
            awaitParked(reader);
        }
        return readers;
    }

    private static void await(BooleanSupplier condition, String message) {
        long deadline = System.nanoTime() + AWAIT_TIMEOUT_NANOS;
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, message);
            Thread.onSpinWait();
        }
    }

    private static void awaitParked(Thread thread) {
        await(() -> thread.getState() == Thread.State.WAITING, "thread never parked");
    }

    private static void awaitInterruptConsumed(Thread thread) {
        await(() -> !thread.isInterrupted(), "thread never consumed the interrupt");
    }

    private static void joinAll(Thread[] threads, String message) throws InterruptedException {
        for (var thread : threads) {
            thread.join(JOIN_TIMEOUT_MILLIS);
            assertFalse(thread.isAlive(), message);
        }
    }

    @Test
    void shouldConsumeInterruptWhileWaitingAndRestoreItOnReturn() throws Exception {
        var latch = latch();
        latch.acquireWrite();
        var acquired = new AtomicBoolean();
        var interruptedAfterAcquire = new AtomicBoolean();
        var reader = new Thread(() -> {
            latch.acquireRead();
            acquired.set(true);
            interruptedAfterAcquire.set(Thread.currentThread().isInterrupted());
            latch.releaseRead();
        });
        reader.start();
        awaitParked(reader);

        reader.interrupt();
        awaitInterruptConsumed(reader);
        awaitParked(reader);

        assertFalse(acquired.get(), "interrupt let the reader past a held write lock");
        latch.releaseWrite();
        joinAll(new Thread[] {reader}, "reader left parked behind a released writer");
        assertTrue(acquired.get());
        assertTrue(interruptedAfterAcquire.get());
        latch.deref();
    }

    @Test
    void shouldNotStrandWaitersAcrossConsecutiveWriterWindows() throws Exception {
        var latch = latch();
        int readerCount = 4;
        int writerCount = 2;
        int acquisitionsPerReader = 5_000;
        var stop = new AtomicBoolean();
        var readers = new Thread[readerCount];
        for (int i = 0; i < readerCount; i++) {
            readers[i] = new Thread(() -> {
                for (int n = 0; n < acquisitionsPerReader; n++) {
                    latch.acquireRead();
                    latch.releaseRead();
                }
            });
            readers[i].start();
        }
        var writers = new Thread[writerCount];
        for (int i = 0; i < writers.length; i++) {
            writers[i] = new Thread(() -> {
                while (!stop.get()) {
                    latch.acquireWrite();
                    long holdUntil = System.nanoTime() + WRITER_HOLD_NANOS;
                    while (System.nanoTime() < holdUntil) {
                        Thread.onSpinWait();
                    }
                    latch.releaseWrite();
                }
            });
            writers[i].start();
        }

        for (var reader : readers) {
            reader.join(JOIN_TIMEOUT_MILLIS);
            assertFalse(reader.isAlive(), "reader stranded in a writer window");
        }
        stop.set(true);
        for (var writer : writers) {
            writer.join(JOIN_TIMEOUT_MILLIS);
            assertFalse(writer.isAlive(), "writer stranded");
        }
        latch.deref();
    }
}
