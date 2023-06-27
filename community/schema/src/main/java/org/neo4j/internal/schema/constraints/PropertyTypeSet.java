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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Stream;

/**
 * An ordered set of {@link SchemaValueType}s, used to represent unions of types.
 * The order is defined in CIP-100 and implemented in terms of the natural ordering of {@link TypeRepresentation.Ordering}.
 */
public class PropertyTypeSet implements Iterable<SchemaValueType> {

    private final Set<TypeRepresentation> representation;
    private final int size;

    private PropertyTypeSet(Collection<SchemaValueType> types) {
        representation = makeRepresentation(types);
        size = types.size();
    }

    public static PropertyTypeSet of(Collection<SchemaValueType> types) {
        return new PropertyTypeSet(types);
    }

    public static PropertyTypeSet of(SchemaValueType... types) {
        return new PropertyTypeSet(Arrays.asList(types));
    }

    /**
     * This method return a string version of the normalized type expression as defined by CIP-100.
     * @return A string representation of the normalized type expression
     */
    public String userDescription() {
        var joiner =
                switch (size) {
                    case 0 -> new StringJoiner("", "ANY", "");
                    case 1 -> new StringJoiner("");
                    default -> new StringJoiner(" | ", "ANY<", ">");
                };
        for (var type : this) {
            joiner.add(type.userDescription());
        }
        return joiner.toString();
    }

    @Override
    public int hashCode() {
        // Use the types' serialization as basis for hash code to make it stable in the face of changing type ordering
        return stream().mapToInt(type -> type.serialize().hashCode()).sum();
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

        return representation.equals(that.representation);
    }

    public int size() {
        return size;
    }

    public boolean contains(TypeRepresentation type) {
        return representation.contains(type);
    }

    public PropertyTypeSet union(PropertyTypeSet other) {
        return of(Stream.concat(stream(), other.stream()).toList());
    }

    public PropertyTypeSet intersection(PropertyTypeSet other) {
        return of(stream().filter(other.representation::contains).toList());
    }

    public PropertyTypeSet difference(PropertyTypeSet other) {
        return of(stream().filter(v -> !other.representation.contains(v)).toList());
    }

    public Stream<SchemaValueType> stream() {
        return representation.stream()
                .filter(SchemaValueType.class::isInstance)
                .map(SchemaValueType.class::cast)
                .sorted(TypeRepresentation::compare);
    }

    @Override
    public Iterator<SchemaValueType> iterator() {
        return stream().iterator();
    }

    public SchemaValueType[] values() {
        return stream().toArray(SchemaValueType[]::new);
    }

    private static Set<TypeRepresentation> makeRepresentation(Collection<SchemaValueType> types) {
        Set<TypeRepresentation> out = new HashSet<>();
        for (var type : types) {
            if (TypeRepresentation.isList(type)) {
                out.add(SpecialTypes.LIST_NOTHING);
            }
            if (TypeRepresentation.isNullable(type)) {
                out.add(SpecialTypes.NULL);
            }
            out.add(type);
        }
        return out;
    }
}
