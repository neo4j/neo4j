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

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;

/**
 * The index configuration is an immutable map from Strings to Values.
 * <p>
 * Not all value types are supported, however. Only "storable" values are supported, with the additional restriction that temporal and spatial values are
 * <em>not</em> supported.
 */
public final class IndexConfig implements Serializable {
    private static final IndexConfig EMPTY = new IndexConfig();
    private static final Supplier<SortedMap<String, Value>> NEW_MAP =
            () -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private final SortedMap<String, Value> map;

    private IndexConfig() {
        this.map = Collections.emptySortedMap();
    }

    private IndexConfig(SortedMap<String, Value> map) {
        this.map = Collections.unmodifiableSortedMap(map);
    }

    public static IndexConfig empty() {
        return EMPTY;
    }

    public static IndexConfig with(String key, Value value) {
        SortedMap<String, Value> map = NEW_MAP.get();
        map.put(key, value);
        return new IndexConfig(map);
    }

    public static IndexConfig with(Map<String, Value> map) {
        SortedMap<String, Value> settings = NEW_MAP.get();
        for (Entry<String, Value> entry : map.entrySet()) {
            String settingName = entry.getKey();
            Value value = validate(entry.getValue());
            settings.put(settingName, value);
        }
        return new IndexConfig(settings);
    }

    private static Value validate(Value value) {
        ValueCategory category = value.valueGroup().category();
        return switch (category) {
            case GEOMETRY, GEOMETRY_ARRAY, TEMPORAL, TEMPORAL_ARRAY, UNKNOWN, NO_CATEGORY ->
                throw new IllegalArgumentException("Value type not support in index configuration: " + value + ".");
            // Otherwise everything is fine.
            default -> value;
        };
    }

    public IndexConfig withIfAbsent(String key, Value value) {
        if (map.containsKey(key)) {
            return this;
        }

        SortedMap<String, Value> copy = NEW_MAP.get();
        copy.putAll(map);
        copy.put(key, validate(value));
        return new IndexConfig(copy);
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

    public Set<String> settingNames() {
        return Collections.unmodifiableSet(map.keySet());
    }

    public Set<Entry<String, Value>> entries() {
        return Collections.unmodifiableSet(map.entrySet());
    }

    public SortedMap<String, Value> asMap() {
        return Collections.unmodifiableSortedMap(map);
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
        for (Entry<String, Value> entry : entries()) {
            sb.append(entry.getKey()).append(" -> ").append(entry.getValue()).append(", ");
        }
        if (!map.isEmpty()) {
            sb.setLength(sb.length() - 2);
        }
        sb.append(']');
        return sb.toString();
    }
}
