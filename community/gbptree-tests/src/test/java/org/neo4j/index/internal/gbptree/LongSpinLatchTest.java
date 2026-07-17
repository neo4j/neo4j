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
import org.junit.jupiter.api.Test;
import org.neo4j.test.Race;

class LongSpinLatchTest extends LatchTestBase {
    private final LongCapture removeAction = new LongCapture();

    @Test
    void shouldAcquireAndReleaseRead() {
        // given
        LongSpinLatch latch = latch();
        latch.ref();
        long countAfterAcquired = latch.acquireRead();
        assertThat(countAfterAcquired).isOne();
        assertThat(removeAction.count.get()).isZero();

        // when
        long countAfterReleased = latch.releaseRead();
        assertThat(countAfterReleased).isZero();
        assertThat(removeAction.count.get()).isZero();

        // then good
        latch.deref();
        assertThat(countAfterReleased).isZero();
        assertThat(removeAction.count.get()).isOne();
    }

    @Test
    void shouldAcquireMultipleTimesAndReleaseRead() {
        // given
        LongSpinLatch latch = latch();
        int times = 5;
        for (int i = 0; i < times; i++) {
            long countAfterAcquired = latch.acquireRead();
            assertEquals(i + 1, countAfterAcquired);
        }

        // when/then
        for (int i = 5; i >= times; i--) {
            long countAfterReleased = latch.releaseRead();
            assertEquals(i - 1, countAfterReleased);
        }
    }

    @Test
    void shouldAcquireAndReleaseWrite() {
        // given
        LongSpinLatch latch = latch();

        // when
        latch.acquireWrite();

        // then good
        assertFalse(latch.tryAcquireWrite());
        latch.releaseWrite();
    }

    @Test
    void shouldAcquireReadAfterWriteReleased() throws TimeoutException, ExecutionException, InterruptedException {
        // given
        LongSpinLatch latch = latch();
        latch.acquireWrite();

        // when
        Future<Void> readAcquisition = beginAndAwaitLatchAcquisition(latch::acquireRead, "acquireRead");
        latch.releaseWrite();

        // then good
        readAcquisition.get();
        latch.releaseRead();
    }

    @Test
    void shouldAcquireAnotherWriteAfterWriteReleased()
            throws TimeoutException, ExecutionException, InterruptedException {
        // given
        LongSpinLatch latch = latch();
        latch.acquireWrite();

        // when
        Future<Void> writeAcquisition = beginAndAwaitLatchAcquisition(latch::acquireWrite, "acquireWrite");
        latch.releaseWrite();

        // then good
        writeAcquisition.get();
        latch.releaseWrite();
    }

    @Test
    void shouldAcquireReadFromMultipleThreadsWithoutBlocking() throws Throwable {
        // given
        Race race = new Race();
        LongSpinLatch latch = latch();
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
                    assertThat(result).isGreaterThanOrEqualTo(0);
                }),
                1);

        // when
        race.go();
    }

    @Test
    void shouldTakeTurnAcquireWrite() throws Throwable {
        // given
        LongSpinLatch latch = latch();
        latch.ref();
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

        // when
        race.go();
    }

    @Test
    void shouldNotReadAcquireDeadLatch() {
        // given
        LongCapture removeAction = new LongCapture();
        long treeNodeId = 101L;
        LongSpinLatch latch = new LongSpinLatch(treeNodeId, removeAction);
        latch.ref();

        // when
        latch.acquireRead();
        latch.releaseRead();
        latch.deref();

        // then
        assertEquals(1, removeAction.count.get());
        assertEquals(treeNodeId, removeAction.captured);
        assertThatThrownBy(latch::acquireRead).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldNotWriteAcquireDeadLatch() {
        // given
        LongCapture removeAction = new LongCapture();
        long treeNodeId = 101L;
        LongSpinLatch latch = new LongSpinLatch(treeNodeId, removeAction);
        latch.ref();

        // when
        latch.acquireWrite();
        latch.releaseWrite();
        latch.deref();

        // then
        assertEquals(1, removeAction.count.get());
        assertEquals(treeNodeId, removeAction.captured);
        assertThatThrownBy(latch::acquireWrite).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldUpgradeRead() {
        // given
        LongSpinLatch latch = latch();
        latch.acquireRead();

        // when
        boolean upgraded = latch.tryUpgradeToWrite();

        // then
        assertTrue(upgraded);
        latch.releaseWrite();
    }

    @Test
    void shouldFailUpgradeReadOnAnotherReadHeld() {
        // given
        LongSpinLatch latch = latch();
        latch.acquireRead();
        latch.acquireRead();

        // when
        boolean upgraded = latch.tryUpgradeToWrite();

        // then
        assertFalse(upgraded);
        latch.releaseRead();
        latch.releaseRead();
    }

    @Test
    void shouldPeekUpgradeabilityAsSoleReader() {
        LongSpinLatch latch = latch();
        latch.acquireRead();

        assertTrue(latch.couldUpgradeToWrite());
        latch.releaseRead();
    }

    @Test
    void shouldPeekUpgradeabilityWithMultipleReaders() {
        LongSpinLatch latch = latch();
        latch.acquireRead();
        latch.acquireRead();

        assertFalse(latch.couldUpgradeToWrite());

        latch.releaseRead();

        assertTrue(latch.couldUpgradeToWrite());
        latch.releaseRead();
    }

    @Test
    void shouldPeekUpgradeabilityWithWriter() {
        LongSpinLatch latch = latch();
        latch.acquireWrite();

        assertFalse(latch.couldUpgradeToWrite());
        latch.releaseWrite();
    }

    private LongSpinLatch latch() {
        return new LongSpinLatch(1, removeAction);
    }
}
