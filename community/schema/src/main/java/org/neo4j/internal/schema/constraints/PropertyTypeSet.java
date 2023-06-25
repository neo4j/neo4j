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
package org.neo4j.internal.schema.constraints;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * An ordered set of {@link SchemaValueType}s, used to represent unions of types.
 * The order is defined in CIP-100 and implemented in terms of the natural ordering of {@link TypeRepresentation.Ordering}.
 */
public class PropertyTypeSet implements Iterable<SchemaValueType> {

    private Set<SchemaValueType> set;

    private PropertyTypeSet(Collection<SchemaValueType> types) {
        set = new TreeSet<>(TypeRepresentation::compare);
        set.addAll(types);
    }

    public static PropertyTypeSet of(Collection<SchemaValueType> types) {
        return new PropertyTypeSet(types);
    }

    public static PropertyTypeSet of(SchemaValueType... types) {
        return of(Arrays.asList(types));
    }

    /**
     * This method return a string version of the normalized type expression as defined by CIP-100.
     * @return A string representation of the normalized type expression
     */
    public String userDescription() {
        var joiner =
                switch (set.size()) {
                    case 0 -> new StringJoiner("", "ANY", "");
                    case 1 -> new StringJoiner("");
                    default -> new StringJoiner(" | ", "ANY<", ">");
                };
        for (var value : set) {
            joiner.add(value.userDescription());
        }
        return joiner.toString();
    }

    @Override
    public int hashCode() {
        // Use the types' serialization as basis for hash code to make it stable in the face of changing type ordering
        return set.stream().mapToInt(type -> type.serialize().hashCode()).sum();
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

        return set.equals(that.set);
    }

    public int size() {
        return set.size();
    }

    public boolean contains(TypeRepresentation type) {
        if (type instanceof SchemaValueType constrainable) {
            return set.contains(constrainable);
        }
        // We won't allow unions with non-constrainable types
        return false;
    }

    public PropertyTypeSet union(PropertyTypeSet other) {
        return of(Stream.concat(set.stream(), other.set.stream()).toList());
    }

    public PropertyTypeSet intersection(PropertyTypeSet other) {
        return of(set.stream().filter(other.set::contains).toList());
    }

    public PropertyTypeSet difference(PropertyTypeSet other) {
        return of(set.stream().filter(v -> !other.set.contains(v)).toList());
    }

    public Stream<SchemaValueType> stream() {
        return set.stream();
    }

    @Override
    public Iterator<SchemaValueType> iterator() {
        return set.iterator();
    }

    public SchemaValueType[] values() {
        return set.stream().toArray(SchemaValueType[]::new);
    }
}
