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
package org.neo4j.internal.counts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.counts.CountsChanges.ABSENT;
import static org.neo4j.internal.counts.GBPTreeCountsStore.nodeKey;
import static org.neo4j.internal.counts.GBPTreeCountsStore.relationshipKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class CountsChangesTest {
    @Inject
    private RandomSupport random;

    private static final Function<CountsKey, AtomicLong> NOT_STORED = key -> new AtomicLong();

    @Test
    void shouldReturnAbsentIfNoCountAndNotStored() {
        // given
        CountsChanges changes = new MapCountsChanges();

        // when
        long count = changes.get(nodeKey(1));

        // then
        assertThat(count).isEqualTo(ABSENT);
        assertThat(changes.containsChange(nodeKey(1))).isFalse();
    }

    @Test
    void shouldAddNewCountIfMissingAndNotStored() {
        // given
        CountsChanges changes = new MapCountsChanges();
        CountsKey key = nodeKey(2);

        // when
        long delta = 5;
        changes.add(key, delta, NOT_STORED);

        // then
        assertThat(changes.get(key)).isEqualTo(delta);
        assertThat(changes.containsChange(key)).isTrue();
    }

    @Test
    void shouldAddNewCountFromStoreIfMissingAndStored() {
        // given
        CountsChanges changes = new MapCountsChanges();
        CountsKey key = nodeKey(2);

        // when
        long storedCount = 3;
        long delta = 5;
        changes.add(key, delta, stored(storedCount));

        // then
        assertThat(changes.get(key)).isEqualTo(storedCount + delta);
        assertThat(changes.containsChange(key)).isTrue();
    }

    @Test
    void shouldUpdateExistingCountIfPresent() {
        // given
        CountsChanges changes = new MapCountsChanges();
        CountsKey key = nodeKey(99);
        long delta1 = 9;
        long delta2 = 5;
        changes.add(key, delta1, NOT_STORED);
        assertThat(changes.containsChange(key)).isTrue();

        // when
        changes.add(key, delta2, NOT_STORED);

        // then
        assertThat(changes.get(key)).isEqualTo(delta1 + delta2);
        assertThat(changes.containsChange(key)).isTrue();
    }

    @Test
    void shouldNotReadFromStoreOnUpdate() {
        // given
        CountsChanges changes = new MapCountsChanges();
        CountsKey key = nodeKey(99);
        long storedCount = 2;
        long delta1 = 9;
        long delta2 = 5;
        changes.add(key, delta1, stored(storedCount));

        // when
        changes.add(key, delta2, stored(storedCount));

        // then
        assertThat(changes.get(key)).isEqualTo(storedCount + delta1 + delta2);
        assertThat(changes.containsChange(key)).isTrue();
    }

    @Test
    void shouldUpdateWithNegativeDelta() {
        // given
        CountsChanges changes = new MapCountsChanges();
        CountsKey key = nodeKey(99);
        long delta1 = 9;
        long delta2 = -5;
        changes.add(key, delta1, NOT_STORED);

        // when
        changes.add(key, delta2, NOT_STORED);

        // then
        assertThat(changes.get(key)).isEqualTo(delta1 + delta2);
        assertThat(changes.containsChange(key)).isTrue();
    }

    @Test
    void shouldFailUpdateOnFrozenChanges() {
        // given
        CountsChanges changes = new MapCountsChanges();
        CountsKey key = relationshipKey(1, 2, 3);
        changes.add(key, 10, NOT_STORED);

        // when
        changes.freezeAndFork();

        // then
        assertThatThrownBy(() -> changes.add(key, 99, NOT_STORED)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldFindCountInOldChanges() {
        // given
        CountsChanges oldChanges = new MapCountsChanges();
        CountsKey key = relationshipKey(4, 99, 21);
        long delta = 10;
        oldChanges.add(key, delta, NOT_STORED);

        // when
        CountsChanges newChanges = oldChanges.freezeAndFork();

        // then
        assertThat(newChanges.get(key)).isEqualTo(delta);
        assertThat(newChanges.containsChange(key)).isTrue();
    }

    @Test
    void shouldUpdateNewChangesBasedOnOldChanges() {
        // given
        CountsChanges oldChanges = new MapCountsChanges();
        CountsKey key = relationshipKey(4, 99, 21);
        long delta1 = 10;
        oldChanges.add(key, delta1, NOT_STORED);
        CountsChanges newChanges = oldChanges.freezeAndFork();

        // when
        long delta2 = 23;
        newChanges.add(key, delta2, stored(999));

        // then
        assertThat(newChanges.get(key)).isEqualTo(delta1 + delta2);
        assertThat(newChanges.containsChange(key)).isTrue();
        assertThat(oldChanges.containsChange(key)).isTrue();
    }

    @Test
    void shouldReturnAbsentIfMissingFromNewAndOld() {
        // given
        CountsChanges oldChanges = new MapCountsChanges();
        CountsKey key = nodeKey(123);
        oldChanges.add(key, 10, NOT_STORED);
        CountsChanges newChanges = oldChanges.freezeAndFork();

        // when
        CountsKey absentKey = nodeKey(101);
        long count = newChanges.get(absentKey);

        // then
        assertThat(count).isEqualTo(ABSENT);
        assertThat(newChanges.containsChange(absentKey)).isFalse();
    }

    @Test
    void shouldUpdateConcurrently() {
        // given
        CountsChanges changes = new MapCountsChanges();
        InMemoryCountsStore store = new InMemoryCountsStore();
        int numStoredCounts = random.nextInt(10, 100);
        for (int i = 0; i < numStoredCounts; i++) {
            CountsKey key;
            do {
                key = randomKey(random.random());
            } while (store.counts.containsKey(key));
            long count = random.nextLong(100);
            store.store(key, count);
        }

        // when
        Race race = new Race();
        List<Map<CountsKey, MutableLong>> allThreadChanges = new CopyOnWriteArrayList<>();
        race.addContestants(4, r -> () -> {
            Random threadRandom = new Random(random.seed() + r);
            Map<CountsKey, MutableLong> threadChanges = new HashMap<>();
            for (int i = 0; i < 1_0000; i++) {
                CountsKey key = randomKey(threadRandom);
                long delta = threadRandom.nextInt(12) - 2;
                changes.add(key, delta, store);
                threadChanges.computeIfAbsent(key, k -> new MutableLong()).add(delta);
            }
            allThreadChanges.add(threadChanges);
        });
        race.goUnchecked();

        // then
        Map<CountsKey, MutableLong> expectedCounts = new HashMap<>();
        store.counts.forEach((key, count) -> expectedCounts.put(key, new MutableLong(count.longValue())));
        for (Map<CountsKey, MutableLong> threadChanges : allThreadChanges) {
            threadChanges.forEach((key, delta) ->
                    expectedCounts.computeIfAbsent(key, k -> new MutableLong()).add(delta.longValue()));
        }
        expectedCounts.forEach((key, expectedCount) -> {
            long expectedCountFromChanges = changes.containsChange(key) ? expectedCount.longValue() : ABSENT;
            assertThat(changes.get(key)).as(key.toString()).isEqualTo(expectedCountFromChanges);
        });
    }

    @Test
    void shouldSortChanges() {
        // given
        CountsChanges changes = new MapCountsChanges();
        Set<CountsKey> expectedChangesSet = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            CountsKey key = randomKey(random.random());
            changes.add(key, 1, k -> new AtomicLong());
            expectedChangesSet.add(key);
        }
        CountsLayout comparator = new CountsLayout();
        List<CountsKey> expectedChanges = new ArrayList<>(expectedChangesSet);
        expectedChanges.sort(comparator);

        // when
        Iterable<Map.Entry<CountsKey, AtomicLong>> sortedChanges = changes.sortedChanges(comparator);

        // then
        Iterator<CountsKey> expectedChangesIterator = expectedChanges.iterator();
        for (Map.Entry<CountsKey, AtomicLong> change : sortedChanges) {
            CountsKey expectedChange = expectedChangesIterator.next();
            assertThat(comparator.compare(expectedChange, change.getKey())).isEqualTo(0);
        }
    }

    @Test
    void shouldReturnTrueWhenGoingToAndFromZero() {
        CountsChanges changes = new MapCountsChanges();
        CountsKey key = nodeKey(99);
        assertThat(changes.add(key, 1, NOT_STORED)).isTrue(); // 0->1 true
        assertThat(changes.add(key, 1, NOT_STORED)).isFalse(); // 1->2 false
        assertThat(changes.add(key, -1, NOT_STORED)).isFalse(); // 2->1 false
        assertThat(changes.add(key, -1, NOT_STORED)).isTrue(); // 1->0 true
    }

    private static CountsKey randomKey(Random random) {
        return random.nextBoolean()
                ? nodeKey(randomToken(random))
                : relationshipKey(randomToken(random), randomToken(random), randomToken(random));
    }

    private static int randomToken(Random random) {
        return random.nextInt(20);
    }

    private static Function<CountsKey, AtomicLong> stored(long count) {
        return key -> new AtomicLong(count);
    }

    private static class InMemoryCountsStore implements Function<CountsKey, AtomicLong> {
        private final ConcurrentHashMap<CountsKey, Long> counts = new ConcurrentHashMap<>();

        void store(CountsKey key, long count) {
            counts.put(key, count);
        }

        @Override
        public AtomicLong apply(CountsKey countsKey) {
            return new AtomicLong(counts.getOrDefault(countsKey, 0L));
        }
    }
}
