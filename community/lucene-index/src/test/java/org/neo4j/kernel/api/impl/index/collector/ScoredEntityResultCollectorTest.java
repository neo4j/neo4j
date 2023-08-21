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
package org.neo4j.kernel.api.impl.index.collector;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;
import java.util.PriorityQueue;
import org.eclipse.collections.api.block.procedure.primitive.LongFloatProcedure;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.kernel.api.impl.index.collector.ScoredEntityResultCollector.ScoredEntityPriorityQueue;
import org.neo4j.kernel.api.impl.index.collector.ScoredEntityResultCollector.ScoredEntityResultsMaxQueueIterator;
import org.neo4j.kernel.api.impl.index.collector.ScoredEntityResultCollector.ScoredEntityResultsMinQueueIterator;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class ScoredEntityResultCollectorTest {
    @Inject
    RandomSupport random;

    @Nested
    class PriorityQueueTest {
        @Test
        void queueMustCollectAndOrderResultsByScore() {
            final var pq = new ScoredEntityPriorityQueue(true);
            assertThat(pq.isEmpty()).isTrue();
            pq.insert(1, 3.0f);
            assertThat(pq.isEmpty()).isFalse();
            pq.insert(2, 1.0f);
            pq.insert(3, 4.0f);
            pq.insert(4, 2.0f);
            pq.insert(5, 7.0f);
            pq.insert(6, 5.0f);
            pq.insert(7, 6.0f);

            final var ids = new ArrayList<Integer>(7);
            final var receiver = (LongFloatProcedure) (id, score) -> ids.add((int) id);
            assertThat(pq.size()).isEqualTo(7);
            pq.removeTop(receiver);
            assertThat(pq.size()).isEqualTo(6);
            pq.removeTop(receiver);
            assertThat(pq.size()).isEqualTo(5);
            pq.removeTop(receiver);
            assertThat(pq.size()).isEqualTo(4);
            pq.removeTop(receiver);
            assertThat(pq.size()).isEqualTo(3);
            pq.removeTop(receiver);
            assertThat(pq.size()).isEqualTo(2);
            pq.removeTop(receiver);
            assertThat(pq.size()).isEqualTo(1);
            assertThat(pq.isEmpty()).isFalse();
            pq.removeTop(receiver);
            assertThat(pq.size()).isEqualTo(0);
            assertThat(pq.isEmpty()).isTrue();
            assertThat(ids).containsExactly(5, 7, 6, 3, 1, 4, 2);
        }

        @Test
        void queueMustCollectAndMinOrderResultsByScore() {
            final var pq = new ScoredEntityPriorityQueue(false);
            assertThat(pq.isEmpty()).isTrue();
            pq.insert(1, 3.0f);
            assertThat(pq.isEmpty()).isFalse();
            pq.insert(2, 1.0f);
            pq.insert(3, 4.0f);
            pq.insert(4, 2.0f);
            pq.insert(5, 7.0f);
            pq.insert(6, 5.0f);
            pq.insert(7, 6.0f);

            final var ids = new ArrayList<Integer>(7);
            final var receiver = (LongFloatProcedure) (id, score) -> ids.add((int) id);
            while (!pq.isEmpty()) {
                pq.removeTop(receiver);
            }

            assertThat(ids).containsExactly(2, 4, 1, 3, 6, 7, 5);
        }

        @RepeatedTest(200)
        void randomizedMaxPriorityQueueTest() {
            final var count = random.nextInt(5, 100);

            final var actualQueue = new ScoredEntityPriorityQueue(true);
            final var expectedQueue = new PriorityQueue<ScoredEntity>();
            for (int i = 0; i < count; i++) {
                final var score = random.nextFloat();
                expectedQueue.add(new ScoredEntity(i, score));
                actualQueue.insert(i, score);
            }

            assertThat(actualQueue.size()).isEqualTo(expectedQueue.size());

            final var scoredEntity = new ScoredEntity(0, 0.0f);
            while (!actualQueue.isEmpty()) {
                actualQueue.removeTop(scoredEntity);
                assertThat(scoredEntity).isEqualTo(expectedQueue.remove());
            }
            assertThat(expectedQueue).isEmpty();
        }

        @RepeatedTest(200)
        void randomizedMinPriorityQueueTest() {
            final var count = random.nextInt(5, 100);

            final var actualQueue = new ScoredEntityPriorityQueue(false);
            final var expectedQueue = new PriorityQueue<ScoredEntity>(Comparator.reverseOrder());
            for (int i = 0; i < count; i++) {
                final var score = random.nextFloat();
                expectedQueue.add(new ScoredEntity(i, score));
                actualQueue.insert(i, score);
            }

            assertThat(actualQueue.size()).isEqualTo(expectedQueue.size());

            final var scoredEntity = new ScoredEntity(0, 0.0f);
            while (!actualQueue.isEmpty()) {
                actualQueue.removeTop(scoredEntity);
                assertThat(scoredEntity).isEqualTo(expectedQueue.remove());
            }
            assertThat(expectedQueue).isEmpty();
        }
    }

    @Nested
    class ScoredEntityResultsMaxQueueIteratorTest {
        @RepeatedTest(200)
        void randomizedPriorityQueueTest() {
            final var count = random.nextInt(50, 100);
            final var actualQueue = new ScoredEntityPriorityQueue(true);
            final var expectedQueue = new PriorityQueue<ScoredEntity>(count);
            for (int i = 0, j = 1; i < count; i++, j++) {
                final var score = random.nextFloat();
                expectedQueue.add(new ScoredEntity(i, score));
                actualQueue.insert(i, score);
            }
            final var iterator = new ScoredEntityResultsMaxQueueIterator(actualQueue);

            final var scoredEntity = new ScoredEntity(0, 0.0f);
            int i = 0;
            while (iterator.hasNext()) {
                iterator.next();
                scoredEntity.value(iterator.current(), iterator.currentScore());
                assertThat(scoredEntity).as("iteration %s", i++).isEqualTo(expectedQueue.remove());
            }
            assertThat(expectedQueue).isEmpty();
        }
    }

    @Nested
    class ScoredEntityResultsMinQueueIteratorTest {
        @Test
        void mustReturnEntriesFromMinQueueInDescendingOrder() {
            final var pq = new ScoredEntityPriorityQueue(false);
            pq.insert(1, 2.0f);
            pq.insert(2, 3.0f);
            pq.insert(3, 1.0f);

            final var iterator = new ScoredEntityResultsMinQueueIterator(pq);
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isEqualTo(2);
            assertThat(iterator.current()).isEqualTo(2);
            assertThat(iterator.currentScore()).isEqualTo(3.0f);
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isEqualTo(1);
            assertThat(iterator.current()).isEqualTo(1);
            assertThat(iterator.currentScore()).isEqualTo(2.0f);
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isEqualTo(3);
            assertThat(iterator.current()).isEqualTo(3);
            assertThat(iterator.currentScore()).isEqualTo(1.0f);
            assertThat(iterator.hasNext()).isFalse();
        }
    }

    static final class ScoredEntity implements Comparable<ScoredEntity>, LongFloatProcedure {
        private long entity;
        private float score;

        ScoredEntity(long entity, float score) {
            this.entity = entity;
            this.score = score;
        }

        @Override
        public void value(long entity, float score) {
            this.entity = entity;
            this.score = score;
        }

        @Override
        public int compareTo(ScoredEntity scoredEntity) {
            return Float.compare(scoredEntity.score, score);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            final var that = (ScoredEntity) obj;
            return this.entity == that.entity && Float.floatToIntBits(this.score) == Float.floatToIntBits(that.score);
        }

        @Override
        public int hashCode() {
            return Objects.hash(entity, score);
        }

        @Override
        public String toString() {
            return "ScoredEntity[" + "entity=" + entity + ", " + "score=" + score + ']';
        }
    }
}
