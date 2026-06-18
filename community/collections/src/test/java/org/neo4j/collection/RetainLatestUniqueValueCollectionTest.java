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
import static org.assertj.core.groups.Tuple.tuple;

import java.util.List;
import org.junit.jupiter.api.Test;

class RetainLatestUniqueValueCollectionTest {

    @Test
    void isEmptyOnCreation() {
        RetainLatestUniqueValueCollection<Object> collection = new RetainLatestUniqueValueCollection<>();
        assertThat(collection).isEmpty();
    }

    @Test
    void canCreateWithCollection() {
        List<Integer> existing = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        RetainLatestUniqueValueCollection<Integer> collection = new RetainLatestUniqueValueCollection<>(existing);
        assertThat(collection).containsExactlyInAnyOrderElementsOf(existing);
    }

    @Test
    void canAddValues() {
        RetainLatestUniqueValueCollection<Integer> collection = new RetainLatestUniqueValueCollection<>();
        collection.add(1);
        collection.add(2);
        collection.addAll(List.of(3, 4, 5));
        assertThat(collection).containsExactlyInAnyOrder(1, 2, 3, 4, 5);
    }

    @Test
    void canRemoveValues() {
        RetainLatestUniqueValueCollection<Integer> collection =
                new RetainLatestUniqueValueCollection<>(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        collection.remove(3);
        collection.removeAll(List.of(6, 7));
        collection.removeIf(value -> value >= 9);
        assertThat(collection).containsExactlyInAnyOrder(1, 2, 5, 4, 8);
    }

    @Test
    void size() {
        RetainLatestUniqueValueCollection<Integer> collection =
                new RetainLatestUniqueValueCollection<>(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        assertThat(collection).hasSize(10);
    }

    @Test
    void sizeChangesOnModification() {
        RetainLatestUniqueValueCollection<Integer> collection =
                new RetainLatestUniqueValueCollection<>(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        assertThat(collection).hasSize(10);

        collection.addAll(List.of(11, 12, 13, 14, 15));
        assertThat(collection).hasSize(15);

        collection.removeAll(List.of(11, 12));
        assertThat(collection).hasSize(13);

        collection.add(42);
        assertThat(collection).hasSize(14);

        collection.add(42);
        assertThat(collection).hasSize(14);
    }

    @Test
    void containsUniqueValues() {
        List<Integer> existing = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        RetainLatestUniqueValueCollection<Integer> collection = new RetainLatestUniqueValueCollection<>(existing);
        int count = existing.size();
        collection.addAll(existing.subList(count / 4, count / 2));
        assertThat(collection).hasSameSizeAs(existing).containsExactlyInAnyOrderElementsOf(existing);
    }

    @Test
    void retainsLatestVersion() {
        RetainLatestUniqueValueCollection<Entry> collection = new RetainLatestUniqueValueCollection<>();
        collection.add(new Entry(1));
        collection.add(new Entry(2));
        collection.add(new Entry(3));
        assertThat(collection)
                .containsExactlyInAnyOrder(new Entry(1), new Entry(2), new Entry(3))
                .allMatch(entry -> entry.version() == 0);

        collection.add(new Entry(2, 1));
        assertThat(collection)
                .containsExactlyInAnyOrder(new Entry(1), new Entry(2), new Entry(3))
                .map(Entry::value, Entry::version)
                .containsExactlyInAnyOrder(tuple(1, 0), tuple(2, 1), tuple(3, 0));
    }

    @Test
    void shouldBeEmptyAfterRemovingEverything() {
        RetainLatestUniqueValueCollection<Integer> collection =
                new RetainLatestUniqueValueCollection<>(List.of(1, 2, 3));
        assertThat(collection).isNotEmpty();

        collection.removeAll(List.of(1, 2, 3));
        assertThat(collection).isEmpty();
    }

    @Test
    void shouldBeEmptyAfterClearing() {
        RetainLatestUniqueValueCollection<Integer> collection =
                new RetainLatestUniqueValueCollection<>(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        assertThat(collection).isNotEmpty();

        collection.clear();
        assertThat(collection).isEmpty();
    }

    private record Entry(int value, int version) {
        Entry(int value) {
            this(value, 0);
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(value);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Entry that && this.value == that.value;
        }
    }
}
