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
package org.neo4j.packstream.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PrimitiveLongIntKeyValueArrayTest {
    private static final int DEFAULT_VALUE = -1;

    @Test
    void testEnsureCapacity() {
        PrimitiveLongIntKeyValueArray map = new PrimitiveLongIntKeyValueArray();
        assertThat(map.capacity()).isEqualTo(PrimitiveLongIntKeyValueArray.DEFAULT_INITIAL_CAPACITY);

        map = new PrimitiveLongIntKeyValueArray(110);
        assertThat(map.capacity()).isEqualTo(110);

        map.ensureCapacity(10);
        assertThat(map.capacity()).isGreaterThanOrEqualTo(10);

        map.ensureCapacity(100);
        assertThat(map.capacity()).isGreaterThanOrEqualTo(100);

        map.ensureCapacity(1000);
        assertThat(map.capacity()).isGreaterThanOrEqualTo(1000);
    }

    @Test
    void testSize() {
        PrimitiveLongIntKeyValueArray map = new PrimitiveLongIntKeyValueArray();
        assertThat(map.size()).isEqualTo(0);

        map.putIfAbsent(1, 100);
        map.putIfAbsent(2, 200);
        map.putIfAbsent(3, 300);
        assertThat(map.size()).isEqualTo(3);
    }

    @Test
    void testGetOrDefault() {
        PrimitiveLongIntKeyValueArray map = new PrimitiveLongIntKeyValueArray();
        map.putIfAbsent(1, 100);
        map.putIfAbsent(2, 200);
        map.putIfAbsent(3, 300);

        assertThat(map.getOrDefault(1, DEFAULT_VALUE)).isEqualTo(100);
        assertThat(map.getOrDefault(2, DEFAULT_VALUE)).isEqualTo(200);
        assertThat(map.getOrDefault(3, DEFAULT_VALUE)).isEqualTo(300);
        assertThat(map.getOrDefault(4, DEFAULT_VALUE)).isEqualTo(DEFAULT_VALUE);
    }

    @Test
    void testPutIfAbsent() {
        PrimitiveLongIntKeyValueArray map = new PrimitiveLongIntKeyValueArray();

        assertThat(map.putIfAbsent(1, 100)).isEqualTo(true);
        assertThat(map.putIfAbsent(2, 200)).isEqualTo(true);
        assertThat(map.putIfAbsent(3, 300)).isEqualTo(true);
        assertThat(map.size()).isEqualTo(3);
        assertThat(map.keys()).isEqualTo(new long[] {1, 2, 3});

        assertThat(map.putIfAbsent(2, 2000)).isEqualTo(false);
        assertThat(map.putIfAbsent(3, 3000)).isEqualTo(false);
        assertThat(map.putIfAbsent(4, 4000)).isEqualTo(true);
        assertThat(map.size()).isEqualTo(4);
        assertThat(map.keys()).isEqualTo(new long[] {1, 2, 3, 4});
        assertThat(map.getOrDefault(2, DEFAULT_VALUE)).isEqualTo(200);
        assertThat(map.getOrDefault(3, DEFAULT_VALUE)).isEqualTo(300);
        assertThat(map.getOrDefault(4, DEFAULT_VALUE)).isEqualTo(4000);
    }

    @Test
    void testReset() {
        PrimitiveLongIntKeyValueArray map = new PrimitiveLongIntKeyValueArray();
        map.putIfAbsent(1, 100);
        map.putIfAbsent(2, 200);
        map.putIfAbsent(3, 300);

        map.reset(1000);
        assertThat(map.size()).isEqualTo(0);
        assertThat(map.capacity()).isGreaterThanOrEqualTo(1000);
    }

    @Test
    void testKeys() {
        PrimitiveLongIntKeyValueArray map = new PrimitiveLongIntKeyValueArray();
        map.putIfAbsent(1, 100);
        map.putIfAbsent(2, 200);
        map.putIfAbsent(3, 300);
        map.putIfAbsent(2, 200);
        map.putIfAbsent(3, 300);
        map.putIfAbsent(8, 800);
        map.putIfAbsent(7, 700);
        map.putIfAbsent(6, 600);
        map.putIfAbsent(5, 500);

        assertThat(map.size()).isEqualTo(7);
        assertThat(map.keys()).isEqualTo(new long[] {1, 2, 3, 8, 7, 6, 5});
    }

    @Test
    void testGrowth() {
        PrimitiveLongIntKeyValueArray map = new PrimitiveLongIntKeyValueArray(10);
        for (int i = 0; i < 100; i++) {
            map.putIfAbsent(i, i);
        }
    }
}
