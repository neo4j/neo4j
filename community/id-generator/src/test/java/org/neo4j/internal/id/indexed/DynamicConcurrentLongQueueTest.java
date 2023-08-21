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
        var queue = new DynamicConcurrentLongQueue(4, 100);
        var items = 400;

        // when
        var taken = new ConcurrentLinkedDeque<Long>();
        var takenCount = new AtomicInteger();
        var race = new Race().withEndCondition(() -> takenCount.get() >= items);
        race.addContestant(
                () -> {
                    for (long i = 0; i < items; i++) {
                        assertThat(queue.offer(i)).isTrue();
                    }
                    assertThat(queue.size()).isGreaterThanOrEqualTo(0);
                },
                1);
        race.addContestants(4, () -> {
            var candidate = queue.takeOrDefault(-1);
            if (candidate != -1) {
                taken.add(candidate);
                takenCount.incrementAndGet();
            }
            assertThat(queue.size()).isGreaterThanOrEqualTo(0);
        });
        race.goUnchecked();

        // then
        var all = LongLists.mutable.empty();
        taken.forEach(all::add);
        all.sortThis();
        var iterator = all.longIterator();
        for (long i = 0; i < items; i++) {
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isEqualTo(i);
        }
        assertThat(iterator.hasNext()).isFalse();
    }
}
