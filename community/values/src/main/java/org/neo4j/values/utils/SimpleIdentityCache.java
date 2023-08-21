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

import java.util.function.BiConsumer;
import java.util.function.Function;
import org.neo4j.util.VisibleForTesting;

/**
 * This is a very simple cache implementation with a very specialized use case.
 * <p><ul>
 * <li> The cache key is only checked for referential equality (== not equals)
 * <li> Access is linear in the size of the cache (NOTE: do not use for bigger caches).
 * <li> When cache is full the oldest item is removed
 * </ul><p>
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public final class SimpleIdentityCache<K, V> {
    private final K[] keys;
    private final V[] values;
    private int next;

    @SuppressWarnings("unchecked")
    public SimpleIdentityCache(int size) {
        this.keys = (K[]) new Object[size];
        this.values = (V[]) new Object[size];
    }

    public V getOrCache(K key, Function<V, V> supplier) {
        V value = get(key);
        if (value == null) {
            value = supplier.apply(values[next]);
            keys[next] = key;
            values[next] = value;
            next = (next + 1) % keys.length;
        }
        return value;
    }

    @VisibleForTesting
    V get(K key) {
        for (int i = 0; i < keys.length; i++) {
            if (key == keys[i]) {
                return values[i];
            }
        }
        return null;
    }

    public void foreach(BiConsumer<K, V> consumer) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null) {
                consumer.accept(keys[i], values[i]);
            }
        }
    }
}
