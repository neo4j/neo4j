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
package org.neo4j.values.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class SimpleIdentityCacheTest {

    static class CountingSupplier<V> implements Function<V, V> {
        private final V value;
        private int count;

        CountingSupplier(V value) {
            this.value = value;
        }

        @Override
        public V apply(V ignore) {
            count++;
            return value;
        }
    }

    @Test
    void shouldCacheAndGet() {
        // given
        var cache = new SimpleIdentityCache<Object, String>(5);

        // when
        var k1 = new Object();
        var k2 = new Object();
        var k3 = new Object();
        var v1 = new CountingSupplier<>("hello");
        var v2 = new CountingSupplier<>("there");
        var v3 = new CountingSupplier<>("again");
        var notThere = new Object();

        // then
        assertThat(cache.getOrCache(k1, v1)).isEqualTo("hello");
        assertThat(cache.getOrCache(k2, v2)).isEqualTo("there");
        assertThat(cache.getOrCache(k3, v3)).isEqualTo("again");
        assertThat(cache.get(notThere)).isNull();
        assertThat(v1.count).isEqualTo(1);
        assertThat(v2.count).isEqualTo(1);
        assertThat(v3.count).isEqualTo(1);
    }

    @Test
    void shouldCache() {
        // given
        var cache = new SimpleIdentityCache<Object, String>(5);

        // when
        var k1 = new Object();
        var v1 = new CountingSupplier<>("hello");

        // then
        assertThat(cache.getOrCache(k1, v1)).isEqualTo("hello");
        assertThat(cache.getOrCache(k1, v1)).isEqualTo("hello");
        assertThat(cache.getOrCache(k1, v1)).isEqualTo("hello");
        assertThat(v1.count).isEqualTo(1);
    }

    @Test
    void shouldOnlyCompareKeysByIdentity() {
        // given, two keys that are equal but not identical
        var key1 = List.of(1, 2, 3);
        var copy = List.of(1, 2, 3);
        var cache = new SimpleIdentityCache<List<Integer>, String>(5);

        // when
        cache.getOrCache(key1, (v) -> "hello");

        // then
        assertThat(key1).isEqualTo(copy);
        assertThat(cache.get(key1)).isEqualTo("hello");
        assertThat(cache.get(copy)).isNull();
    }

    @Test
    void shouldEvictOldest() {
        // given
        var k1 = new Object();
        var k2 = new Object();
        var k3 = new Object();
        var k4 = new Object();
        var cache = new SimpleIdentityCache<Object, String>(3);

        // when
        cache.getOrCache(k1, (v) -> "a");
        cache.getOrCache(k2, (v) -> "b");
        cache.getOrCache(k3, (v) -> "c");
        cache.getOrCache(k4, (v) -> "d");

        // then
        assertThat(cache.get(k1)).isNull();
        assertThat(cache.get(k2)).isEqualTo("b");
        assertThat(cache.get(k3)).isEqualTo("c");
        assertThat(cache.get(k4)).isEqualTo("d");
    }

    @Test
    void shouldSeePreviousValueWhenEvicting() {
        // given
        var k1 = new Object();
        var k2 = new Object();
        var k3 = new Object();
        var k4 = new Object();
        var cache = new SimpleIdentityCache<Object, String>(3);

        // when
        cache.getOrCache(k1, (v) -> "a");
        cache.getOrCache(k2, (v) -> "b");
        cache.getOrCache(k3, (v) -> "c");

        // then
        cache.getOrCache(k4, (v) -> {
            assertThat(v).isEqualTo("a");
            return "d";
        });
        cache.getOrCache(k1, (v) -> {
            assertThat(v).isEqualTo("b");
            return "e";
        });
        cache.getOrCache(k2, (v) -> {
            assertThat(v).isEqualTo("c");
            return "f";
        });
    }

    @Test
    void shouldForeach() {
        // given
        var k1 = new Object();
        var k2 = new Object();
        var k3 = new Object();
        var cache = new SimpleIdentityCache<Object, String>(7);

        // when
        cache.getOrCache(k1, (v) -> "a");
        cache.getOrCache(k2, (v) -> "b");
        cache.getOrCache(k3, (v) -> "c");

        // then
        cache.foreach((k, v) -> {
            if (k == k1) {
                assertThat(v).isEqualTo("a");
            } else if (k == k2) {
                assertThat(v).isEqualTo("b");
            } else if (k == k3) {
                assertThat(v).isEqualTo("c");
            } else {
                fail("Unknown key and value, k=%s, v=%s", k, v);
            }
        });
    }
}
