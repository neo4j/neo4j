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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.junit.jupiter.api.Test;
import org.neo4j.test.Race;

class DynamicConcurrentLongQueueTest {
    @Test
    void shouldOfferAndTakeOnSingleChunk() {
        // given
        var queue = new DynamicConcurrentLongQueue(8, 5);

        // when
        int size = 0;
        for (int i = 0; i < 5; i++) {
            boolean offsered = queue.offer(i * 100);
            assertThat(offsered).isTrue();
            size++;
        }

        // then
        for (int i = 0; i < 5; i++) {
            assertThat(queue.size()).isEqualTo(size--);
            assertThat(queue.takeOrDefault(-1)).isEqualTo(i * 100);
        }
        assertThat(queue.size()).isZero();
        assertThat(queue.takeOrDefault(-1)).isEqualTo(-1L);
    }

    @Test
    void shouldOfferAndTakeOnMultipleChunks() {
        // given
        var queue = new DynamicConcurrentLongQueue(8, 5);

        // when
        int size = 0;
        for (int i = 0; i < 40; i++) {
            boolean offered = queue.offer(i * 100);
            assertThat(offered).isTrue();
            size++;
        }

        // then
        for (int i = 0; i < 40; i++) {
            assertThat(queue.size()).isEqualTo(size--);
            assertThat(queue.takeOrDefault(-1)).isEqualTo(i * 100);
        }
        assertThat(queue.size()).isZero();
        assertThat(queue.takeOrDefault(-1)).isEqualTo(-1L);
    }

    @Test
    void shouldTakeParallel() {
        // given
        var queue = new DynamicConcurrentLongQueue(32, 100);
        var items = 32 * 100;
        for (int i = 0; i < items; i++) {
            queue.offer(i);
        }

        // when
        var taken = new MutableLongList[4];
        for (var i = 0; i < taken.length; i++) {
            taken[i] = LongLists.mutable.empty();
        }
        var race = new Race().withEndCondition(() -> false);
        race.addContestants(
                taken.length,
                c -> () -> {
                    long item;
                    do {
                        var size = queue.size();
                        item = queue.takeOrDefault(-1);
                        if (item != -1) {
                            taken[c].add(item);
                            assertThat(size).isGreaterThan(0);
                        }
                    } while (item != -1);
                },
                1);
        race.goUnchecked();

        // then
        var all = LongLists.mutable.empty();
        for (var list : taken) {
            all.addAll(list);
        }
        all.sortThis();
        var iterator = all.longIterator();
        for (int i = 0; iterator.hasNext(); i++) {
            assertThat(iterator.next()).isEqualTo(i);
        }
    }

    @Test
    void shouldOfferAndTakeParallel() {
        // given
        var itemsPerThread = 50_000;
        var offerThreads = Math.max(4, Runtime.getRuntime().availableProcessors() / 2);
        var queue = new DynamicConcurrentLongQueue(20, 20);
        var items = itemsPerThread * offerThreads;

        // when
        var taken = new ConcurrentLinkedDeque<Long>();
        var takenCount = new AtomicInteger();
        var race = new Race().withEndCondition(() -> takenCount.get() >= items);
        race.addContestants(
                offerThreads,
                c -> () -> {
                    var startItem = c * itemsPerThread;
                    for (var i = 0; i < itemsPerThread; i++) {
                        boolean offered = false;
                        for (long startTime = System.currentTimeMillis(),
                                        endTime = startTime + TimeUnit.SECONDS.toMillis(5);
                                !offered && System.currentTimeMillis() < endTime; ) {
                            offered = queue.offer(startItem + i);
                        }
                        assertThat(offered).isTrue();
                    }
                    assertSizeAndAvailableSpace(queue);
                },
                1);
        var takeInRangeHits = new AtomicInteger();
        var takeInRangeMisses = new AtomicInteger();
        race.addContestants(4, () -> {
            // takeInRange
            {
                var rng = ThreadLocalRandom.current();
                var min = rng.nextInt(items - 10);
                var max = rng.nextInt(min, items);
                var candidate = queue.takeInRange(min, max);
                if (candidate != Long.MAX_VALUE) {
                    taken.add(candidate);
                    takenCount.incrementAndGet();
                    takeInRangeHits.incrementAndGet();
                } else {
                    takeInRangeMisses.incrementAndGet();
                }
            }

            // take
            {
                var candidate = queue.takeOrDefault(-1);
                if (candidate != -1) {
                    taken.add(candidate);
                    takenCount.incrementAndGet();
                }
            }
            assertSizeAndAvailableSpace(queue);
        });
        race.goUnchecked();

        // then
        var all = LongLists.mutable.empty();
        taken.forEach(all::add);
        all.sortThis();
        var iterator = all.longIterator();
        for (var i = 0; i < items; i++) {
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isEqualTo(i);
        }
        assertThat(iterator.hasNext()).isFalse();
        assertThat(takeInRangeHits.get()).isGreaterThan(0);
        assertThat(takeInRangeMisses.get()).isGreaterThan(0);
    }

    /**
     * This assertion is used in a concurrent setting which makes it impossible to accurately assert that size + availableSpace == maxCapacity,
     * so therefor it asserts on both being >= 0. It's very hard to do otherwise.
     */
    private static void assertSizeAndAvailableSpace(DynamicConcurrentLongQueue queue) {
        assertThat(queue.size()).isGreaterThanOrEqualTo(0);
        assertThat(queue.availableSpace()).isGreaterThanOrEqualTo(0);
    }
}
