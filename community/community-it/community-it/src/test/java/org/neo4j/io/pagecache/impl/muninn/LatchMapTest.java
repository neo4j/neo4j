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
package org.neo4j.io.pagecache.impl.muninn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.test.ThreadTestUtils;
import org.neo4j.util.concurrent.BinaryLatch;

class LatchMapTest {
    @ValueSource(ints = {LatchMap.DEFAULT_FAULT_LOCK_STRIPING, 1 << 10, 1 << 11})
    @ParameterizedTest
    void takeOrAwaitLatchMustReturnLatchIfAvailable(int size) {
        LatchMap latches = new LatchMap(size);
        BinaryLatch latch = latches.takeOrAwaitLatch(0);
        assertThat(latch).isNotNull();
        latch.release();
    }

    @ValueSource(ints = {LatchMap.DEFAULT_FAULT_LOCK_STRIPING, 1 << 10, 1 << 11})
    @ParameterizedTest
    void takeOrAwaitLatchMustAwaitExistingLatchAndReturnNull(int size) throws Exception {
        LatchMap latches = new LatchMap(size);
        AtomicReference<Thread> threadRef = new AtomicReference<>();
        BinaryLatch latch = latches.takeOrAwaitLatch(42);
        assertThat(latch).isNotNull();
        ExecutorService executor = null;
        try {
            executor = Executors.newSingleThreadExecutor();
            Future<BinaryLatch> future = executor.submit(() -> {
                threadRef.set(Thread.currentThread());
                return latches.takeOrAwaitLatch(42);
            });
            Thread th;
            do {
                th = threadRef.get();
            } while (th == null);
            ThreadTestUtils.awaitThreadState(th, 10_000, Thread.State.WAITING);
            latch.release();
            assertThat(future.get(1, TimeUnit.SECONDS)).isNull();
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    @ValueSource(ints = {LatchMap.DEFAULT_FAULT_LOCK_STRIPING, 1 << 10, 1 << 11})
    @ParameterizedTest
    void takeOrAwaitLatchMustNotLetUnrelatedLatchesConflictTooMuch(int size) throws Exception {
        LatchMap latches = new LatchMap(size);
        BinaryLatch latch = latches.takeOrAwaitLatch(42);
        assertThat(latch).isNotNull();
        ExecutorService executor = null;
        try {
            executor = Executors.newSingleThreadExecutor();
            Future<BinaryLatch> future = executor.submit(() -> latches.takeOrAwaitLatch(33));
            assertThat(future.get(30, TimeUnit.SECONDS)).isNotNull();
            latch.release();
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    @ValueSource(ints = {LatchMap.DEFAULT_FAULT_LOCK_STRIPING, 1 << 10, 1 << 11})
    @ParameterizedTest
    void latchMustBeAvailableAfterRelease(int size) {
        LatchMap latches = new LatchMap(size);
        latches.takeOrAwaitLatch(42).release();
        latches.takeOrAwaitLatch(42).release();
    }

    @Test
    void shouldFailOnSizeNotPowerOfTwo() {
        assertThatThrownBy(() -> new LatchMap(123)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void largerLatchMapShouldAllowMoreLatches() {
        // given
        LatchMap latches = new LatchMap(512);

        // then
        List<LatchMap.Latch> latchList = new ArrayList<>();
        for (int i = 0; i < 256; i++) {
            latchList.add(latches.takeOrAwaitLatch(i)); // should not contend
        }
        latchList.forEach(LatchMap.Latch::release);
    }
}
