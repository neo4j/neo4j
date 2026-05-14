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
package org.neo4j.graphalgo.impl.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.impl.util.PriorityMap.Entry;

class TestPriorityMap {
    @Test
    void it() {
        PriorityMap<Integer, Integer, Double> map = PriorityMap.withSelfKeyNaturalOrder();
        map.put(0, 5d);
        map.put(1, 4d);
        map.put(1, 4d);
        map.put(1, 3d);
        assertEntry(map.pop(), 1, 3d);
        assertEntry(map.pop(), 0, 5d);
        assertThat(map.pop()).isNull();

        int start = 0;
        int a = 1;
        int b = 2;
        int c = 3;
        int d = 4;
        int e = 6;
        int f = 7;
        int y = 8;
        int x = 9;
        map.put(start, 0d);
        map.put(a, 1d);
        // get start
        // get a
        map.put(x, 10d);
        map.put(b, 2d);
        // get b
        map.put(x, 9d);
        map.put(c, 3d);
        // get c
        map.put(x, 8d);
        map.put(x, 6d);
        map.put(d, 4d);
        // get d
        map.put(x, 7d);
        map.put(e, 5d);
        // get e
        map.put(x, 6d);
        map.put(f, 7d);
        // get x
        map.put(y, 8d);
    }

    @Test
    void shouldReplaceIfBetter() {
        // GIVEN
        PriorityMap<Integer, Integer, Double> map = PriorityMap.withSelfKeyNaturalOrder();
        map.put(1, 2d);

        // WHEN
        boolean putResult = map.put(1, 1.5d);

        // THEN
        assertThat(putResult).isTrue();
        Entry<Integer, Double> top = map.pop();
        assertThat(map.peek()).isNull();
        assertThat(top.getEntity().intValue()).isOne();
        assertThat(top.getPriority()).isCloseTo(1.5d, within(0.00001));
    }

    @Test
    void shouldKeepAllPrioritiesIfToldTo() {
        // GIVEN
        int entity = 5;
        PriorityMap<Integer, Integer, Double> map = PriorityMap.withSelfKeyNaturalOrder(false, false);
        assertThat(map.put(entity, 3d)).isTrue();
        assertThat(map.put(entity, 2d)).isTrue();

        // WHEN
        assertThat(map.put(entity, 5d)).isTrue();
        assertThat(map.put(entity, 4d)).isTrue();

        // THEN
        assertEntry(map.pop(), entity, 2d);
        assertEntry(map.pop(), entity, 3d);
        assertEntry(map.pop(), entity, 4d);
        assertEntry(map.pop(), entity, 5d);
    }

    @Test
    void inCaseSaveAllPrioritiesShouldHandleNewEntryWithWorsePrio() {
        // GIVEN
        int first = 1;
        int second = 2;
        PriorityMap<Integer, Integer, Double> map = PriorityMap.withSelfKeyNaturalOrder(false, false);

        // WHEN
        assertThat(map.put(first, 1d)).isTrue();
        assertThat(map.put(second, 2d)).isTrue();
        assertThat(map.put(first, 3d)).isTrue();

        // THEN
        assertEntry(map.pop(), first, 1d);
        assertEntry(map.pop(), second, 2d);
        assertEntry(map.pop(), first, 3d);
        assertThat(map.peek()).isNull();
    }

    @Test
    void inCaseSaveAllPrioritiesShouldHandleNewEntryWithBetterPrio() {
        // GIVEN
        int first = 1;
        int second = 2;
        PriorityMap<Integer, Integer, Double> map = PriorityMap.withSelfKeyNaturalOrder(false, false);

        // WHEN
        assertThat(map.put(first, 3d)).isTrue();
        assertThat(map.put(second, 2d)).isTrue();
        assertThat(map.put(first, 1d)).isTrue();

        // THEN
        assertEntry(map.pop(), first, 1d);
        assertEntry(map.pop(), second, 2d);
        assertEntry(map.pop(), first, 3d);
        assertThat(map.peek()).isNull();
    }

    private static void assertEntry(Entry<Integer, Double> entry, Integer entity, Double priority) {
        assertThat(entry).isNotNull();
        assertThat(entry.getEntity()).isEqualTo(entity);
        assertThat(entry.getPriority()).isEqualTo(priority);
    }
}
