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

public class SchemaValueTypes {
    static final String DELIMITER = "-";

    public static SchemaValueType convertToSchemaValueType(String valueTypeRepresentation) {
        String[] split = valueTypeRepresentation.split(DELIMITER, 3);
        try {
            if (split.length == 1) {
                return SchemaScalarValueType.valueOf(valueTypeRepresentation);
            } else if (split.length == 2) {
                SchemaListValueType.UserFacingType userFacingType =
                        SchemaListValueType.UserFacingType.valueOf(split[0]);
                SchemaScalarValueType scalarValueType = SchemaScalarValueType.valueOf(split[1]);
                return new SchemaListValueType(userFacingType, scalarValueType);
            }
        } catch (IllegalArgumentException ignore) {
            // Type not found, but we want our own exception with the whole value
        }

        throw new IllegalArgumentException(
                "'%s' could not be converted to a schema value type".formatted(valueTypeRepresentation));
    }

    public static String convertToStringRepresentation(SchemaValueType schemaValueType) {
        return schemaValueType.stringRepresentation();
    }
}
