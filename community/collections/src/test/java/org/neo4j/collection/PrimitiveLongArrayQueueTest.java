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
package org.neo4j.collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.NoSuchElementException;
import org.eclipse.collections.api.iterator.LongIterator;
import org.junit.jupiter.api.Test;

class PrimitiveLongArrayQueueTest {

    @Test
    void newQueueIsEmpty() {
        assertThat(createQueue().isEmpty()).isTrue();
    }

    @Test
    void growQueueOnElementOffer() {
        PrimitiveLongArrayQueue longArrayQueue = createQueue();
        for (int i = 1; i < 1000; i++) {
            longArrayQueue.enqueue(i);
            assertThat(longArrayQueue.size()).isEqualTo(i);
        }
    }

    @Test
    void addRemoveElementKeepQueueEmpty() {
        PrimitiveLongArrayQueue longArrayQueue = createQueue();
        for (int i = 0; i < 1000; i++) {
            longArrayQueue.enqueue(i);
            assertThat(longArrayQueue.dequeue()).isEqualTo(i);
            assertThat(longArrayQueue.isEmpty()).isTrue();
        }
    }

    @Test
    void offerLessThenQueueCapacityElements() {
        PrimitiveLongArrayQueue arrayQueue = createQueue();
        for (int i = 1; i < 16; i++) {
            arrayQueue.enqueue(i);
            assertThat(arrayQueue.size()).isEqualTo(i);
        }
    }

    @Test
    void failToRemoveElementFromNewEmptyQueue() {
        assertThatThrownBy(() -> createQueue().dequeue()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void offerMoreThenQueueCapacityElements() {
        PrimitiveLongArrayQueue arrayQueue = createQueue();
        for (int i = 1; i < 1234; i++) {
            arrayQueue.enqueue(i);
        }
        int currentValue = 1;
        while (!arrayQueue.isEmpty()) {
            assertThat(arrayQueue.dequeue()).isEqualTo(currentValue++);
        }
    }

    @Test
    void tailBeforeHeadCorrectSize() {
        PrimitiveLongArrayQueue queue = createQueue();
        for (int i = 0; i < 14; i++) {
            queue.enqueue(i);
        }
        for (int i = 0; i < 10; i++) {
            assertThat(queue.dequeue()).isEqualTo(i);
        }
        for (int i = 14; i < 24; i++) {
            queue.enqueue(i);
        }

        assertThat(queue.size()).isEqualTo(14);
    }

    @Test
    void tailBeforeHeadCorrectResize() {
        PrimitiveLongArrayQueue queue = createQueue();
        for (int i = 0; i < 14; i++) {
            queue.enqueue(i);
        }
        for (int i = 0; i < 10; i++) {
            assertThat(queue.dequeue()).isEqualTo(i);
        }
        for (int i = 14; i < 34; i++) {
            queue.enqueue(i);
        }

        assertThat(queue.size()).isEqualTo(24);
        for (int j = 10; j < 34; j++) {
            assertThat(queue.dequeue()).isEqualTo(j);
        }
    }

    @Test
    void tailBeforeHeadCorrectIteration() {
        PrimitiveLongArrayQueue queue = createQueue();
        for (int i = 0; i < 14; i++) {
            queue.enqueue(i);
        }
        for (int i = 0; i < 10; i++) {
            assertThat(queue.dequeue()).isEqualTo(i);
        }
        for (int i = 14; i < 24; i++) {
            queue.enqueue(i);
        }

        assertThat(queue.size()).isEqualTo(14);
        LongIterator iterator = queue.longIterator();
        for (int j = 10; j < 24; j++) {
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isEqualTo(j);
        }
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void failToGetNextOnEmptyQueueIterator() {
        assertThatThrownBy(() -> createQueue().longIterator().next()).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void addAllElementsFromOtherQueue() {
        PrimitiveLongArrayQueue queue = createQueue();
        queue.enqueue(1);
        queue.enqueue(2);
        PrimitiveLongArrayQueue otherQueue = createQueue();
        otherQueue.enqueue(3);
        otherQueue.enqueue(4);
        queue.addAll(otherQueue);

        assertThat(otherQueue.isEmpty()).isTrue();
        assertThat(otherQueue.size()).isZero();
        assertThat(queue.size()).isEqualTo(4);
        for (int value = 1; value <= 4; value++) {
            assertThat(queue.dequeue()).isEqualTo(value);
        }
        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    void doNotAllowCreationOfQueueWithRandomCapacity() {
        assertThatThrownBy(() -> new PrimitiveLongArrayQueue(7)).isInstanceOf(IllegalArgumentException.class);
    }

    private static PrimitiveLongArrayQueue createQueue() {
        return new PrimitiveLongArrayQueue();
    }
}
