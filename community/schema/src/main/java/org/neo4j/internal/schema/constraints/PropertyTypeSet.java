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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Stream;

/**
 * An ordered set of {@link ConstrainableType}s, used to represent unions of types.
 * The order is defined in CIP-100 and implemented in terms of the natural ordering of {@link TypeRepresentation.Ordering}.
 */
public class PropertyTypeSet implements Iterable<ConstrainableType>, Serializable {
    private final Set<? extends ConstrainableType> lookup;
    private final List<? extends ConstrainableType> types;
    private final boolean acceptsEmptyList;

    private PropertyTypeSet(
            Set<? extends ConstrainableType> lookup,
            List<? extends ConstrainableType> types,
            boolean acceptsEmptyList) {
        this.lookup = lookup;
        this.types = types;
        this.acceptsEmptyList = acceptsEmptyList;
    }

    public static PropertyTypeSet empty() {
        return new PropertyTypeSet(Set.of(), List.of(), false);
    }

    public static PropertyTypeSet of(Collection<? extends ConstrainableType> types) {
        if (types.isEmpty()) {
            return empty();
        }

        Set<? extends ConstrainableType> lookup = Set.copyOf(types);
        List<? extends ConstrainableType> uniqueTypes =
                lookup.stream().sorted(TypeRepresentation::compare).toList();
        boolean acceptsEmptyList = types.stream().anyMatch(TypeRepresentation::isList);

        return new PropertyTypeSet(lookup, uniqueTypes, acceptsEmptyList);
    }

    public static PropertyTypeSet of(ConstrainableType... types) {
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

        StringJoiner joiner = new StringJoiner(" | ");
        for (ConstrainableType type : types) {
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
        if (repr instanceof ConstrainableType type) {
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

    public Stream<? extends ConstrainableType> stream() {
        return types.stream();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<ConstrainableType> iterator() {
        return (Iterator<ConstrainableType>) types.iterator();
    }

    public ConstrainableType[] values() {
        return types.toArray(ConstrainableType[]::new);
    }
}
