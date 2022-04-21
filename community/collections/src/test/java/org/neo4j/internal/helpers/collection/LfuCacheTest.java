/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.helpers.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LfuCacheTest {
    @Test
    void shouldThrowWhenMaxSizeIsNotGreaterThanZero() {
        assertThrows(IllegalArgumentException.class, () -> new LfuCache<>("TestCache", 0));
    }

    @Test
    void shouldThrowWhenPuttingEntryWithNullKey() {
        assertThrows(NullPointerException.class, () -> new LfuCache<>("TestCache", 70).put(null, new Object()));
    }

    @Test
    void shouldThrowWhenPuttingEntryWithNullValue() {
        assertThrows(NullPointerException.class, () -> new LfuCache<>("TestCache", 70).put(new Object(), null));
    }

    @Test
    void shouldThrowWhenGettingWithANullKey() {
        assertThrows(NullPointerException.class, () -> new LfuCache<>("TestCache", 70).get(null));
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

        assertEquals(new HashSet<>(Arrays.asList(key1, key2, key3)), cache.keySet());

        cache.put(key4, s4);

        assertEquals(new HashSet<>(Arrays.asList(key2, key3, key4)), cache.keySet());

        cache.put(key5, s5);

        int size = cache.size();

        assertEquals(3, size);
        assertEquals(s2, cache.get(key2));

        cache.clear();
        assertEquals(0, cache.size());
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

        assertEquals(Set.of(new Integer[] {key1, key2, key3}), cache.keySet());
        assertEquals(cache.maxSize(), cache.size());

        cache.put(key4, s4);

        assertEquals(Set.of(new Integer[] {key2, key3, key4}), cache.keySet());

        cache.put(key5, s5);

        assertEquals(cache.maxSize(), cache.size());

        cache.clear();

        assertEquals(0, cache.size());
    }
}
