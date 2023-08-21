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

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.internal.schema.IndexOrder;

class PrimitiveSortedMergeJoinTest {

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
        assertThatItWorks(asList(1L, 3L, 5L, 7L), Collections.emptyList(), indexOrder);
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldWorkWith2Lists(IndexOrder indexOrder) {
        assertThatItWorks(asList(1L, 3L, 5L, 7L), asList(2L, 4L, 6L, 8L), indexOrder);
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldWorkWithSameElements(IndexOrder indexOrder) {
        assertThatItWorks(asList(1L, 3L, 5L), asList(2L, 3L, 6L), indexOrder);
    }

    private static void assertThatItWorks(List<Long> listA, List<Long> listB, IndexOrder indexOrder) {
        assertThatItWorksOneWay(listA, listB, indexOrder);
        assertThatItWorksOneWay(listB, listA, indexOrder);
    }

    private static void assertThatItWorksOneWay(List<Long> listA, List<Long> listB, IndexOrder indexOrder) {
        PrimitiveSortedMergeJoin sortedMergeJoin = new PrimitiveSortedMergeJoin();
        sortedMergeJoin.initialize(indexOrder);

        Comparator<Long> comparator =
                indexOrder == IndexOrder.ASCENDING ? Comparator.naturalOrder() : Comparator.reverseOrder();

        listA.sort(comparator);
        listB.sort(comparator);

        List<Long> result = process(sortedMergeJoin, listA.iterator(), listB.iterator());

        List<Long> expected = new ArrayList<>();
        expected.addAll(listA);
        expected.addAll(listB);
        expected.sort(comparator);

        assertThat(result).isEqualTo(expected);
    }

    private static List<Long> process(
            PrimitiveSortedMergeJoin sortedMergeJoin, Iterator<Long> iteratorA, Iterator<Long> iteratorB) {
        final List<Long> result = new ArrayList<>();
        long node = 0;
        while (node != -1) {
            if (iteratorA.hasNext() && sortedMergeJoin.needsA()) {
                long a = iteratorA.next();
                sortedMergeJoin.setA(a);
            }
            if (iteratorB.hasNext() && sortedMergeJoin.needsB()) {
                long b = iteratorB.next();
                sortedMergeJoin.setB(b);
            }

            node = sortedMergeJoin.next();
            if (node != -1) {
                result.add(node);
            }
        }
        return result;
    }
}
