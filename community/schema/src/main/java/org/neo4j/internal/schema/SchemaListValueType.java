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
package org.neo4j.internal.schema;

import static org.neo4j.values.storable.ValueRepresentation.BOOLEAN;
import static org.neo4j.values.storable.ValueRepresentation.BOOLEAN_ARRAY;
import static org.neo4j.values.storable.ValueRepresentation.INT64;
import static org.neo4j.values.storable.ValueRepresentation.INT64_ARRAY;

import java.util.Map;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueRepresentation;

public record SchemaListValueType(UserFacingType userFacingType, SchemaScalarValueType contentType)
        implements SchemaValueType {

    @Override
    public boolean isAssignable(Value value) {
        if (value instanceof ArrayValue arrayValue) {
            ValueRepresentation scalarValue = ARRAY_TO_SCALAR_MAPPING.get(arrayValue.valueRepresentation());
            if (scalarValue == null) {
                throw new IllegalStateException("Unrecognized array value " + arrayValue.valueRepresentation());
            }

            return contentType.isAssignable(scalarValue);
        }

        return false;
    }

    enum UserFacingType {
        LIST,
        ARRAY
    }

    private static final Map<ValueRepresentation, ValueRepresentation> ARRAY_TO_SCALAR_MAPPING = Map.of(
            BOOLEAN_ARRAY, BOOLEAN,
            INT64_ARRAY, INT64);
}
