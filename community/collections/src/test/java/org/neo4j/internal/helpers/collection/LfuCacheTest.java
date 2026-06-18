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
package org.neo4j.internal.helpers.collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class LfuCacheTest {
    @Test
    void shouldThrowWhenMaxSizeIsNotGreaterThanZero() {
        assertThatThrownBy(() -> new LfuCache<>("TestCache", 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowWhenPuttingEntryWithNullKey() {
        assertThatThrownBy(() -> new LfuCache<>("TestCache", 70).put(null, new Object()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowWhenPuttingEntryWithNullValue() {
        assertThatThrownBy(() -> new LfuCache<>("TestCache", 70).put(new Object(), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowWhenGettingWithANullKey() {
        assertThatThrownBy(() -> new LfuCache<>("TestCache", 70).get(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldWork() {
        LfuCache<Integer, String> cache = new LfuCache<>("TestCache", 3);

        String s1 = "1";
        Integer key1 = 1;
        String s2 = "2";
        Integer key2 = 2;
        String s3 = "3";
        Integer key3 = 3;
        String s4 = "4";
        Integer key4 = 4;
        String s5 = "5";
        Integer key5 = 5;

        cache.put(key1, s1);
        cache.put(key2, s2);
        cache.put(key3, s3);
        cache.get(key2);

        assertThat(cache.keySet()).containsOnly(key1, key2, key3);

        cache.put(key4, s4);

        assertThat(cache.keySet()).contains(key4);

        cache.put(key5, s5);

        int size = cache.size();

        assertThat(size).isEqualTo(3);
        assertThat(cache.get(key5)).isEqualTo(s5);

        cache.clear();
        assertThat(cache.size()).isZero();
    }

    @Test
    void remove() {
        final var cache = new LfuCache<Integer, String>("TestCache", 3);
        final var key1 = 13;
        final var key2 = 42;
        final var key3 = 69;
        final var key4 = 666;

        cache.put(key1, String.valueOf(key1));
        cache.put(key2, String.valueOf(key2));
        cache.put(key3, String.valueOf(key3));

        cache.remove(key1);
        assertThat(cache.keySet()).containsExactlyInAnyOrder(key2, key3);

        cache.put(key4, String.valueOf(key4));
        assertThat(cache.keySet()).containsExactlyInAnyOrder(key2, key3, key4);
    }

    @Test
    void shouldClear() {
        LfuCache<Integer, String> cache = new LfuCache<>("TestCache", 3);

        String s1 = "1";
        Integer key1 = 1;
        String s2 = "2";
        Integer key2 = 2;
        String s3 = "3";
        Integer key3 = 3;
        String s4 = "4";
        Integer key4 = 4;
        String s5 = "5";
        Integer key5 = 5;

        cache.put(key1, s1);
        cache.put(key2, s2);
        cache.put(key3, s3);
        cache.get(key2);

        assertThat(cache.keySet()).containsOnly(key1, key2, key3);
        assertThat(cache.size()).isEqualTo(cache.maxSize());

        cache.put(key4, s4);

        assertThat(cache.keySet()).contains(key4);

        cache.put(key5, s5);

        assertThat(cache.size()).isEqualTo(cache.maxSize());

        cache.clear();

        assertThat(cache.size()).isZero();
    }
}
