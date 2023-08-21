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
package org.neo4j.internal.schema.constraints;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Stream;

/**
 * An ordered set of {@link SchemaValueType}s, used to represent unions of types.
 * The order is defined in CIP-100 and implemented in terms of the natural ordering of {@link TypeRepresentation.Ordering}.
 */
public class PropertyTypeSet implements Iterable<SchemaValueType> {

    private final Set<SchemaValueType> lookup;
    private final List<SchemaValueType> types;
    private final boolean acceptsEmptyList;

    private PropertyTypeSet(Set<SchemaValueType> lookup, List<SchemaValueType> types, boolean acceptsEmptyList) {
        this.lookup = lookup;
        this.types = types;
        this.acceptsEmptyList = acceptsEmptyList;
    }

    public static PropertyTypeSet empty() {
        return new PropertyTypeSet(Set.of(), List.of(), false);
    }

    public static PropertyTypeSet of(Collection<SchemaValueType> types) {
        if (types.isEmpty()) {
            return empty();
        }

        var lookup = EnumSet.copyOf(types);
        var uniqueTypes = lookup.stream().sorted(TypeRepresentation::compare).toList();
        var acceptsEmptyList = types.stream().anyMatch(TypeRepresentation::isList);

        return new PropertyTypeSet(lookup, uniqueTypes, acceptsEmptyList);
    }

    public static PropertyTypeSet of(SchemaValueType... types) {
        return of(Arrays.asList(types));
    }

    /**
     * This method return a string version of the normalized type expression as defined by CIP-100.
     * @return A string representation of the normalized type expression
     */
    public String userDescription() {
        if (types.isEmpty()) {
            return "NOTHING";
        }

        var joiner = new StringJoiner(" | ");
        for (var type : types) {
            joiner.add(type.userDescription());
        }
        return joiner.toString();
    }

    @Override
    public int hashCode() {
        // Use the types' serialization as basis for hash code to make it stable in the face of changing type ordering
        return types.stream().mapToInt(type -> type.serialize().hashCode()).sum();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PropertyTypeSet that = (PropertyTypeSet) o;

        return types.equals(that.types);
    }

    public int size() {
        return types.size();
    }

    public boolean contains(TypeRepresentation repr) {
        if (repr instanceof SchemaValueType type) {
            return lookup.contains(type);
        }

        // All types are nullable
        if (repr == SpecialTypes.NULL) {
            return true;
        }

        // For list types, accept the empty list
        return acceptsEmptyList && repr == SpecialTypes.LIST_NOTHING;
    }

    public PropertyTypeSet union(PropertyTypeSet other) {
        return of(Stream.concat(stream(), other.stream()).toList());
    }

    public PropertyTypeSet intersection(PropertyTypeSet other) {
        return of(stream().filter(other.lookup::contains).toList());
    }

    public PropertyTypeSet difference(PropertyTypeSet other) {
        return of(stream().filter(v -> !other.lookup.contains(v)).toList());
    }

    public Stream<SchemaValueType> stream() {
        return types.stream();
    }

    @Override
    public Iterator<SchemaValueType> iterator() {
        return types.iterator();
    }

    public SchemaValueType[] values() {
        return types.toArray(SchemaValueType[]::new);
    }
}
