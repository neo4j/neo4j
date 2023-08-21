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
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

class SortedMergeJoinTest {
    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldWorkWithEmptyLists(IndexOrder indexOrder) {
        assertThatItWorksOneWay(Collections.emptyList(), Collections.emptyList(), indexOrder);
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldWorkWithAList(IndexOrder indexOrder) {
        assertThatItWorks(
                Arrays.asList(node(1L, "a"), node(3L, "aa"), node(5L, "c"), node(7L, "g")),
                Collections.emptyList(),
                indexOrder);
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldWorkWith2Lists(IndexOrder indexOrder) {
        assertThatItWorks(
                Arrays.asList(node(1L, "a"), node(3L, "aa"), node(5L, "c"), node(7L, "g")),
                Arrays.asList(node(2L, "b"), node(4L, "ba"), node(6L, "ca"), node(8L, "d")),
                indexOrder);
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldWorkWithSameElements(IndexOrder indexOrder) {
        assertThatItWorks(
                Arrays.asList(node(1L, "a"), node(3L, "b"), node(5L, "c")),
                Arrays.asList(node(2L, "aa"), node(3L, "b"), node(6L, "ca")),
                indexOrder);
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldWorkWithCompositeValues(IndexOrder indexOrder) {
        assertThatItWorks(
                Arrays.asList(node(1L, "a", "a"), node(3L, "b", "a"), node(5L, "b", "b"), node(7L, "c", "d")),
                Arrays.asList(node(2L, "a", "b"), node(5L, "b", "b"), node(6L, "c", "e")),
                indexOrder);
    }

    private static void assertThatItWorks(
            List<EntityWithPropertyValues> listA, List<EntityWithPropertyValues> listB, IndexOrder indexOrder) {
        assertThatItWorksOneWay(listA, listB, indexOrder);
        assertThatItWorksOneWay(listB, listA, indexOrder);
    }

    private static void assertThatItWorksOneWay(
            List<EntityWithPropertyValues> listA, List<EntityWithPropertyValues> listB, IndexOrder indexOrder) {
        SortedMergeJoin sortedMergeJoin = new SortedMergeJoin();
        sortedMergeJoin.initialize(indexOrder);

        Comparator<EntityWithPropertyValues> comparator = indexOrder == IndexOrder.ASCENDING
                ? (a, b) -> ValueTuple.COMPARATOR.compare(ValueTuple.of(a.getValues()), ValueTuple.of(b.getValues()))
                : (a, b) -> ValueTuple.COMPARATOR.compare(ValueTuple.of(b.getValues()), ValueTuple.of(a.getValues()));

        listA.sort(comparator);
        listB.sort(comparator);

        List<EntityWithPropertyValues> result = process(sortedMergeJoin, listA.iterator(), listB.iterator());

        List<EntityWithPropertyValues> expected = new ArrayList<>();
        expected.addAll(listA);
        expected.addAll(listB);
        expected.sort(comparator);

        assertThat(result).isEqualTo(expected);
    }

    private static List<EntityWithPropertyValues> process(
            SortedMergeJoin sortedMergeJoin,
            Iterator<EntityWithPropertyValues> iteratorA,
            Iterator<EntityWithPropertyValues> iteratorB) {
        Collector collector = new Collector();
        do {
            if (iteratorA.hasNext() && sortedMergeJoin.needsA()) {
                EntityWithPropertyValues a = iteratorA.next();
                sortedMergeJoin.setA(a.getEntityId(), a.getValues());
            }
            if (iteratorB.hasNext() && sortedMergeJoin.needsB()) {
                EntityWithPropertyValues b = iteratorB.next();
                sortedMergeJoin.setB(b.getEntityId(), b.getValues());
            }
        } while (sortedMergeJoin.next(collector));
        return collector.result;
    }

    private static EntityWithPropertyValues node(long id, Object... values) {
        return new EntityWithPropertyValues(
                id, Stream.of(values).map(Values::of).toArray(Value[]::new));
    }

    static class Collector implements SortedMergeJoin.Sink {
        final List<EntityWithPropertyValues> result = new ArrayList<>();

        @Override
        public void acceptSortedMergeJoin(long nodeId, Value[] values) {
            result.add(new EntityWithPropertyValues(nodeId, values));
        }
    }
}
