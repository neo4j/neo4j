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

import org.neo4j.graphdb.schema.PropertyType;

/**
 * Note: ordering and name (user description) is defined by CIP-100.
 */
public enum SchemaValueType implements TypeRepresentation {
    BOOLEAN("BOOLEAN", Ordering.BOOLEAN_ORDER),
    STRING("STRING", Ordering.STRING_ORDER),
    INTEGER("INTEGER", Ordering.INTEGER_ORDER),
    FLOAT("FLOAT", Ordering.FLOAT_ORDER),
    DATE("DATE", Ordering.DATE_ORDER),
    LOCAL_TIME("LOCAL TIME", Ordering.LOCAL_TIME_ORDER),
    ZONED_TIME("ZONED TIME", Ordering.ZONED_TIME_ORDER),
    LOCAL_DATETIME("LOCAL DATETIME", Ordering.LOCAL_DATETIME_ORDER),
    ZONED_DATETIME("ZONED DATETIME", Ordering.ZONED_DATETIME_ORDER),
    DURATION("DURATION", Ordering.DURATION_ORDER),
    POINT("POINT", Ordering.POINT_ORDER),

    LIST_BOOLEAN("LIST<BOOLEAN NOT NULL>", Ordering.LIST_BOOLEAN_ORDER),
    LIST_STRING("LIST<STRING NOT NULL>", Ordering.LIST_STRING_ORDER),
    LIST_INTEGER("LIST<INTEGER NOT NULL>", Ordering.LIST_INTEGER_ORDER),
    LIST_FLOAT("LIST<FLOAT NOT NULL>", Ordering.LIST_FLOAT_ORDER),
    LIST_DATE("LIST<DATE NOT NULL>", Ordering.LIST_DATE_ORDER),
    LIST_LOCAL_TIME("LIST<LOCAL TIME NOT NULL>", Ordering.LIST_LOCAL_TIME_ORDER),
    LIST_ZONED_TIME("LIST<ZONED TIME NOT NULL>", Ordering.LIST_ZONED_TIME_ORDER),
    LIST_LOCAL_DATETIME("LIST<LOCAL DATETIME NOT NULL>", Ordering.LIST_LOCAL_DATETIME_ORDER),
    LIST_ZONED_DATETIME("LIST<ZONED DATETIME NOT NULL>", Ordering.LIST_ZONED_DATETIME_ORDER),
    LIST_DURATION("LIST<DURATION NOT NULL>", Ordering.LIST_DURATION_ORDER),
    LIST_POINT("LIST<POINT NOT NULL>", Ordering.LIST_POINT_ORDER);

    private final String userDescription;
    private final Ordering order;

    SchemaValueType(String userDescription, Ordering order) {
        this.userDescription = userDescription;
        this.order = order;
    }

    @Override
    public String userDescription() {
        return userDescription;
    }

    @Override
    public Ordering order() {
        return order;
    }

    public String serialize() {
        return this.name();
    }

    @Override
    public String toString() {
        return userDescription;
    }

    public static SchemaValueType fromPublicApi(PropertyType propertyType) {
        return switch (propertyType) {
            case BOOLEAN -> SchemaValueType.BOOLEAN;
            case STRING -> SchemaValueType.STRING;
            case INTEGER -> SchemaValueType.INTEGER;
            case FLOAT -> SchemaValueType.FLOAT;
            case DURATION -> SchemaValueType.DURATION;
            case DATE -> SchemaValueType.DATE;
            case ZONED_DATETIME -> SchemaValueType.ZONED_DATETIME;
            case LOCAL_DATETIME -> SchemaValueType.LOCAL_DATETIME;
            case ZONED_TIME -> SchemaValueType.ZONED_TIME;
            case LOCAL_TIME -> SchemaValueType.LOCAL_TIME;
            case POINT -> SchemaValueType.POINT;
            case LIST_BOOLEAN_NOT_NULL -> SchemaValueType.LIST_BOOLEAN;
            case LIST_STRING_NOT_NULL -> SchemaValueType.LIST_STRING;
            case LIST_INTEGER_NOT_NULL -> SchemaValueType.LIST_INTEGER;
            case LIST_FLOAT_NOT_NULL -> SchemaValueType.LIST_FLOAT;
            case LIST_DATE_NOT_NULL -> SchemaValueType.LIST_DATE;
            case LIST_LOCAL_TIME_NOT_NULL -> SchemaValueType.LIST_LOCAL_TIME;
            case LIST_ZONED_TIME_NOT_NULL -> SchemaValueType.LIST_ZONED_TIME;
            case LIST_LOCAL_DATETIME_NOT_NULL -> SchemaValueType.LIST_LOCAL_DATETIME;
            case LIST_ZONED_DATETIME_NOT_NULL -> SchemaValueType.LIST_ZONED_DATETIME;
            case LIST_DURATION_NOT_NULL -> SchemaValueType.LIST_DURATION;
            case LIST_POINT_NOT_NULL -> SchemaValueType.LIST_POINT;
        };
    }

    public PropertyType toPublicApi() {
        return switch (this) {
            case BOOLEAN -> PropertyType.BOOLEAN;
            case STRING -> PropertyType.STRING;
            case INTEGER -> PropertyType.INTEGER;
            case FLOAT -> PropertyType.FLOAT;
            case DURATION -> PropertyType.DURATION;
            case DATE -> PropertyType.DATE;
            case ZONED_DATETIME -> PropertyType.ZONED_DATETIME;
            case LOCAL_DATETIME -> PropertyType.LOCAL_DATETIME;
            case ZONED_TIME -> PropertyType.ZONED_TIME;
            case LOCAL_TIME -> PropertyType.LOCAL_TIME;
            case LIST_BOOLEAN -> PropertyType.LIST_BOOLEAN_NOT_NULL;
            case LIST_STRING -> PropertyType.LIST_STRING_NOT_NULL;
            case LIST_INTEGER -> PropertyType.LIST_INTEGER_NOT_NULL;
            case LIST_FLOAT -> PropertyType.LIST_FLOAT_NOT_NULL;
            case LIST_DATE -> PropertyType.LIST_DATE_NOT_NULL;
            case LIST_LOCAL_TIME -> PropertyType.LIST_LOCAL_TIME_NOT_NULL;
            case LIST_ZONED_TIME -> PropertyType.LIST_ZONED_TIME_NOT_NULL;
            case LIST_LOCAL_DATETIME -> PropertyType.LIST_LOCAL_DATETIME_NOT_NULL;
            case LIST_ZONED_DATETIME -> PropertyType.LIST_ZONED_DATETIME_NOT_NULL;
            case LIST_DURATION -> PropertyType.LIST_DURATION_NOT_NULL;
            case LIST_POINT -> PropertyType.LIST_POINT_NOT_NULL;
            case POINT -> PropertyType.POINT;
                // FIXME PTC remove default when we have all the types in Core API
            default -> throw new IllegalArgumentException(
                    "Property type '" + this.userDescription() + "' is not supported in Core API yet.");
        };
    }
}
