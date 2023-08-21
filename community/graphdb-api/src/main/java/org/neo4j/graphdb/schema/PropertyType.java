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
package org.neo4j.graphdb.schema;

import org.neo4j.annotations.api.PublicApi;

/**
 * Property type. Used with {@link ConstraintCreator#assertPropertyHasType(String, PropertyType...)} and
 * {@link ConstraintDefinition#getPropertyType()}
 * to specify which type a property must be and see which type a property is constrained to, respectively.
 */
@PublicApi
public enum PropertyType {
    BOOLEAN,
    STRING,
    INTEGER,
    FLOAT,
    DATE,
    LOCAL_TIME,
    ZONED_TIME,
    LOCAL_DATETIME,
    ZONED_DATETIME,
    DURATION,
    POINT,
    LIST_BOOLEAN_NOT_NULL,
    LIST_STRING_NOT_NULL,
    LIST_INTEGER_NOT_NULL,
    LIST_FLOAT_NOT_NULL,
    LIST_DATE_NOT_NULL,
    LIST_LOCAL_TIME_NOT_NULL,
    LIST_ZONED_TIME_NOT_NULL,
    LIST_LOCAL_DATETIME_NOT_NULL,
    LIST_ZONED_DATETIME_NOT_NULL,
    LIST_DURATION_NOT_NULL,
    LIST_POINT_NOT_NULL,
}
