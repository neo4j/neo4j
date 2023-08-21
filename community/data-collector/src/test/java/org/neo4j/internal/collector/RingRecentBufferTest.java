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
package org.neo4j.internal.collector;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RingRecentBufferTest {
    @Test
    void shouldJustWork() {
        int bufferSize = 4;
        RingRecentBuffer<Long> buffer = new RingRecentBuffer<>(bufferSize, q -> {});

        buffer.foreach(l -> fail("boom"));

        for (long i = 0; i < 10; i++) {
            buffer.produce(i);
            buffer.foreach(Assertions::assertNotNull);
        }

        buffer.clearIf(l -> true);
        buffer.foreach(l -> fail("boom"));

        for (long i = 0; i < 10; i++) {
            buffer.produce(i);
        }
        buffer.foreach(Assertions::assertNotNull);

        assertEquals(0, buffer.numSilentQueryDrops());
    }

    @Test
    void shouldHandleSize0() {
        RingRecentBuffer<Long> buffer = new RingRecentBuffer<>(0, q -> {});

        buffer.foreach(l -> fail("boom"));
        buffer.clearIf(l -> true);

        buffer.produce(0L);
        buffer.foreach(l -> fail("boom"));
        buffer.clearIf(l -> true);

        assertEquals(0, buffer.numSilentQueryDrops());
    }

    @Test
    void shouldClearIf() {
        int bufferSize = 4;
        RingRecentBuffer<Long> buffer = new RingRecentBuffer<>(bufferSize, q -> {});

        for (long i = 0; i < 4; i++) {
            buffer.produce(i);
        }

        buffer.clearIf(i -> i % 2 == 0);

        List<Long> retained = new ArrayList<>();
        buffer.foreach(retained::add);
        assertEquals(2, retained.size());
        assertEquals(1, retained.get(0));
        assertEquals(3, retained.get(1));
    }

    @Test
    void shouldNotReadSameElementTwice() throws ExecutionException, InterruptedException {
        // given
        int n = 1000;
        int bufferSize = 16;
        RingRecentBuffer<Long> buffer = new RingRecentBuffer<>(bufferSize, q -> {});
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            UniqueElementsConsumer consumer = new UniqueElementsConsumer();

            // when
            // producer thread
            CountDownLatch latch = new CountDownLatch(1);
            Future<?> produce = executor.submit(stressUntil(latch, buffer::produce));

            // consumer thread
            Future<?> consume = executor.submit(stress(n, i -> {
                consumer.reset();
                buffer.foreach(consumer);
                assertTrue(consumer.values.size() <= bufferSize, format("Should see at most %d elements", bufferSize));
            }));

            // then without illegal transitions or exceptions
            consume.get();
            latch.countDown();
            produce.get();
        } finally {
            executor.shutdown();
        }
        assertEquals(0, buffer.numSilentQueryDrops());
    }

    @Test
    void shouldNeverReadUnwrittenElements() throws ExecutionException, InterruptedException {
        // given
        int n = 1000000;
        int bufferSize = 16;
        RingRecentBuffer<Long> buffer = new RingRecentBuffer<>(bufferSize, q -> {});
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            // when
            // producer thread
            CountDownLatch latch = new CountDownLatch(1);
            Future<?> produce = executor.submit(stressUntil(latch, buffer::produce));
            // consumer thread
            Future<?> consume = executor.submit(stress(n, i -> {
                buffer.clearIf(l -> true);
                buffer.foreach(Assertions::assertNotNull);
            }));

            // then without illegal transitions or exceptions
            consume.get();
            latch.countDown();
            produce.get();
        } finally {
            executor.shutdown();
        }
        assertEquals(0, buffer.numSilentQueryDrops());
    }

    @Test
    void shouldWorkWithManyConcurrentProducers() throws ExecutionException, InterruptedException {
        // given
        int n = 1000000;
        int bufferSize = 16;
        RingRecentBuffer<Long> buffer = new RingRecentBuffer<>(bufferSize, q -> {});
        ExecutorService executor = Executors.newFixedThreadPool(4);

        try {
            // when
            // producer threads
            CountDownLatch latch = new CountDownLatch(1);
            Future<?> produce1 = executor.submit(stressUntil(latch, buffer::produce));
            Future<?> produce2 = executor.submit(stressUntil(latch, buffer::produce));
            Future<?> produce3 = executor.submit(stressUntil(latch, buffer::produce));
            // consumer thread
            Future<?> consume = executor.submit(stress(n, i -> {
                buffer.clearIf(l -> true);
                buffer.foreach(Assertions::assertNotNull);
            }));

            // then without illegal transitions or exceptions
            consume.get();
            latch.countDown();
            produce1.get();
            produce2.get();
            produce3.get();
        } finally {
            executor.shutdown();
        }
        // on some systems thread scheduling variance actually causes ~100 silent drops in this test
        assertTrue(buffer.numSilentQueryDrops() < 1000, "only a few silent drops expected");
    }

    private static Runnable stress(int n, LongConsumer action) {
        return () -> {
            for (long i = 0; i < n; i++) {
                action.accept(i);
            }
        };
    }

    private static Runnable stressUntil(CountDownLatch latch, LongConsumer action) {
        return () -> {
            long i = 0;
            while (latch.getCount() != 0) {
                action.accept(i++);
            }
        };
    }

    static class UniqueElementsConsumer implements Consumer<Long> {
        MutableLongSet values = LongSets.mutable.empty();

        void reset() {
            values.clear();
        }

        @Override
        public void accept(Long newValue) {
            assertTrue(values.add(newValue), format("Value %d was seen twice", newValue));
        }
    }
}
