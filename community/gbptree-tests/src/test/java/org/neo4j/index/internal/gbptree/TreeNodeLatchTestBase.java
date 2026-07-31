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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.Race.throwing;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.Test;
import org.neo4j.test.Race;

abstract class TreeNodeLatchTestBase extends LatchTestBase {
    static final long TREE_NODE_ID = 1;

    final LongCapture removeAction = new LongCapture();

    abstract TreeNodeLatch newLatch(long treeNodeId, LongConsumer removeAction);

    abstract boolean ref(TreeNodeLatch latch);

    abstract Class<?> latchClass();

    @Test
    void shouldAcquireAndReleaseRead() {
        var latch = latch();

        assertThat(latch.acquireRead()).isOne();
        assertThat(removeAction.count.get()).isZero();

        assertThat(latch.releaseRead()).isZero();
        assertThat(removeAction.count.get()).isZero();

        latch.deref();
        assertThat(removeAction.count.get()).isOne();
    }

    @Test
    void shouldAcquireMultipleTimesAndReleaseRead() {
        var latch = latch();
        int times = 5;
        for (int i = 0; i < times; i++) {
            assertEquals(i + 1, latch.acquireRead());
        }

        for (int i = times; i > 0; i--) {
            assertEquals(i - 1, latch.releaseRead());
        }
        latch.deref();
    }

    @Test
    void shouldAcquireAndReleaseWrite() {
        var latch = latch();

        latch.acquireWrite();

        assertFalse(latch.tryAcquireWrite());
        latch.releaseWrite();
        latch.deref();
    }

    @Test
    void shouldAcquireReadAfterWriteReleased() throws TimeoutException, ExecutionException, InterruptedException {
        var latch = latch();
        latch.acquireWrite();

        Future<Void> readAcquisition = beginAndAwaitLatchAcquisition(latch::acquireRead, latchClass(), "acquireRead");
        latch.releaseWrite();

        readAcquisition.get();
        latch.releaseRead();
        latch.deref();
    }

    @Test
    void shouldAcquireAnotherWriteAfterWriteReleased()
            throws TimeoutException, ExecutionException, InterruptedException {
        var latch = latch();
        latch.acquireWrite();

        Future<Void> writeAcquisition =
                beginAndAwaitLatchAcquisition(latch::acquireWrite, latchClass(), "acquireWrite");
        latch.releaseWrite();

        writeAcquisition.get();
        latch.releaseWrite();
        latch.deref();
    }

    @Test
    void shouldLetWriterInWhenLastReaderLeaves() throws Exception {
        var latch = latch();
        latch.acquireRead();

        Future<Void> writeAcquisition =
                beginAndAwaitLatchAcquisition(latch::acquireWrite, latchClass(), "acquireWrite");
        latch.releaseRead();

        writeAcquisition.get();
        t2.execute(() -> {
            latch.releaseWrite();
            return null;
        });
        latch.deref();
    }

    @Test
    void shouldAcquireReadFromMultipleThreadsWithoutBlocking() throws Throwable {
        Race race = new Race();
        var latch = latch();
        int numThreads = Runtime.getRuntime().availableProcessors();
        CountDownLatch countDownLatch = new CountDownLatch(numThreads);
        race.addContestants(
                numThreads,
                throwing(() -> {
                    long result = latch.acquireRead();
                    assertThat(result).isGreaterThan(0);
                    countDownLatch.countDown();
                    countDownLatch.await();
                    latch.releaseRead();
                }),
                1);

        race.go();
        latch.deref();
    }

    @Test
    void shouldTakeTurnAcquireWrite() throws Throwable {
        var latch = latch();
        Race race = new Race();
        AtomicBoolean singleHolder = new AtomicBoolean();
        race.addContestants(
                Runtime.getRuntime().availableProcessors(),
                throwing(() -> {
                    latch.acquireWrite();
                    assertThat(singleHolder.getAndSet(true)).isFalse();
                    Thread.sleep(1);
                    assertThat(singleHolder.getAndSet(false)).isTrue();
                    latch.releaseWrite();
                }),
                10);

        race.go();
        latch.deref();
    }

    @Test
    void shouldDieOnLastDeref() {
        var latch = latch();
        latch.acquireRead();
        latch.releaseRead();

        latch.deref();

        assertEquals(1, removeAction.count.get());
        assertEquals(TREE_NODE_ID, removeAction.captured);
        assertFalse(ref(latch));
        assertThatThrownBy(latch::acquireRead).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(latch::acquireWrite).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldUpgradeRead() {
        var latch = latch();
        latch.acquireRead();

        boolean upgraded = latch.tryUpgradeToWrite();

        assertTrue(upgraded);
        latch.releaseWrite();
        latch.deref();
    }

    @Test
    void shouldFailUpgradeReadOnAnotherReadHeld() {
        var latch = latch();
        latch.acquireRead();
        latch.acquireRead();

        boolean upgraded = latch.tryUpgradeToWrite();

        assertFalse(upgraded);
        latch.releaseRead();
        latch.releaseRead();
        latch.deref();
    }

    @Test
    void shouldPeekUpgradeabilityAsSoleReader() {
        var latch = latch();
        latch.acquireRead();

        assertTrue(latch.couldUpgradeToWrite());
        latch.releaseRead();
        latch.deref();
    }

    @Test
    void shouldPeekUpgradeabilityWithMultipleReaders() {
        var latch = latch();
        latch.acquireRead();
        latch.acquireRead();

        assertFalse(latch.couldUpgradeToWrite());

        latch.releaseRead();

        assertTrue(latch.couldUpgradeToWrite());
        latch.releaseRead();
        latch.deref();
    }

    @Test
    void shouldPeekUpgradeabilityWithWriter() {
        var latch = latch();
        latch.acquireWrite();

        assertFalse(latch.couldUpgradeToWrite());
        latch.releaseWrite();
        latch.deref();
    }

    TreeNodeLatch latch() {
        return newLatch(TREE_NODE_ID, removeAction);
    }
}
