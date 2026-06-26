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
package org.neo4j.bolt.test.connection.resolver.property;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class SimpleMutableTestPropertyContext implements MutableTestPropertyContext {
    private final Map<Key<?>, Object> properties;

    public SimpleMutableTestPropertyContext() {
        this.properties = new HashMap<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> get(Key<T> key) {
        return Optional.ofNullable((T) this.properties.get(key));
    }

    @Override
    public <T> void set(Key<T> key, T value) {
        this.properties.put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T computeIfAbsent(Key<T> key, Supplier<T> supplier) {
        return (T) this.properties.computeIfAbsent(key, (k) -> supplier.get());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T computeIfPresent(Key<T> key, Function<Key<T>, T> supplier) {
        return (T) this.properties.computeIfAbsent(key, (k) -> supplier.apply((Key<T>) k));
    }
}
