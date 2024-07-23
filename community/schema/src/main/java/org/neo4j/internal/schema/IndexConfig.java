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
package org.neo4j.internal.schema;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedMap;
import java.util.function.Supplier;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.factory.SortedMaps;
import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;
import org.eclipse.collections.api.tuple.Pair;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;

/**
 * The index configuration is an immutable map from Strings to Values.
 * <p>
 * Not all value types are supported, however. Only "storable" values are supported, with the additional restriction that temporal and spatial values are
 * <em>not</em> supported.
 */
public final class IndexConfig {
    private static final IndexConfig EMPTY = new IndexConfig(SortedMaps.immutable.empty());

    private final ImmutableSortedMap<String, Value> map;

    private IndexConfig(ImmutableSortedMap<String, Value> map) {
        this.map = map;
    }

    public static IndexConfig empty() {
        return EMPTY;
    }

    public static IndexConfig with(String key, Value value) {
        return new IndexConfig(SortedMaps.immutable.with(String.CASE_INSENSITIVE_ORDER, key, value));
    }

    public static IndexConfig with(Map<String, Value> map) {
        for (Value value : map.values()) {
            validate(value);
        }
        return new IndexConfig(SortedMaps.mutable
                .<String, Value>with(String.CASE_INSENSITIVE_ORDER)
                .withMap(map)
                .toImmutable());
    }

    private static void validate(Value value) {
        ValueCategory category = value.valueGroup().category();
        switch (category) {
            case GEOMETRY,
                    GEOMETRY_ARRAY,
                    TEMPORAL,
                    TEMPORAL_ARRAY,
                    UNKNOWN,
                    NO_CATEGORY -> throw new IllegalArgumentException(
                    "Value type not support in index configuration: " + value + ".");
                // Otherwise everything is fine.
            default -> {}
        }
    }

    public IndexConfig withIfAbsent(String key, Value value) {
        validate(value);
        if (map.containsKey(key)) {
            return this;
        }
        return new IndexConfig(map.newWithKeyValue(key, value));
    }

    @SuppressWarnings("unchecked")
    public <T extends Value> T get(String key) {
        return (T) map.get(key);
    }

    public <T extends Value> T getOrDefault(String key, T defaultValue) {
        T value = get(key);
        return value != null ? value : defaultValue;
    }

    public <T extends Value> T getOrThrow(String key) {
        return getOrThrow(key, () -> new NoSuchElementException("'%s' is not set".formatted(key)));
    }

    public <T extends Value, E extends Throwable> T getOrThrow(String key, Supplier<? extends E> exceptionSupplier)
            throws E {
        T value = get(key);
        if (value == null) {
            throw exceptionSupplier.get();
        }
        return value;
    }

    public RichIterable<Pair<String, Value>> entries() {
        return map.keyValuesView();
    }

    public SortedMap<String, Value> asMap() {
        return map.castToMap();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexConfig that)) {
            return false;
        }
        return map.equals(that.map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(map);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("IndexConfig[");
        for (Pair<String, Value> entry : entries()) {
            sb.append(entry.getOne()).append(" -> ").append(entry.getTwo()).append(", ");
        }
        if (!map.isEmpty()) {
            sb.setLength(sb.length() - 2);
        }
        sb.append(']');
        return sb.toString();
    }
}
