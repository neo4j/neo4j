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
import java.util.StringJoiner;
import java.util.TreeSet;
import org.neo4j.internal.schema.SchemaValueType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueRepresentation;

/**
 * An ordered set of {@link SchemaValueType}s, used to represent unions of types.
 * The order is defined in CIP-100 and implemented in terms of the natural ordering of {@link SchemaValueType}.
 */
public class PropertyTypeSet extends TreeSet<SchemaValueType> {
    /**
     * This method return a string version of the normalized type expression as defined by CIP-100.
     * @return A string representation of the normalized type expression
     */
    public String userDescription() {
        var joiner = size() > 1 ? new StringJoiner(" | ", "ANY<", ">") : new StringJoiner("");
        this.forEach(schemaValueType -> joiner.add(schemaValueType.userDescription()));
        return joiner.toString();
    }

    public static PropertyTypeSet of(SchemaValueType... types) {
        var set = new PropertyTypeSet();
        set.addAll(Arrays.asList(types));
        return set;
    }

    public boolean valueIsOfTypes(Value value) {
        final ValueRepresentation valueRepresentation = value.valueRepresentation();
        for (SchemaValueType valueType : this) {
            if (valueType.isAssignable(valueRepresentation)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        // Use the types' serialization as basis for hash code to make it stable in the face of changing type ordering
        return stream().mapToInt(type -> type.serialize().hashCode()).sum();
    }
}
