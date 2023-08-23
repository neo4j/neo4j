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

import java.util.Objects;
import java.util.Set;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanArray;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DateArray;
import org.neo4j.values.storable.DateTimeArray;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationArray;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LocalDateTimeArray;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeArray;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.ScalarValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeArray;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public sealed interface TypeRepresentation permits SchemaValueType, SpecialTypes {

    String userDescription();

    Ordering order();

    enum Ordering {
        NULL_ORDER,
        BOOLEAN_ORDER,
        STRING_ORDER,
        INTEGER_ORDER,
        FLOAT_ORDER,
        DATE_ORDER,
        LOCAL_TIME_ORDER,
        ZONED_TIME_ORDER,
        LOCAL_DATETIME_ORDER,
        ZONED_DATETIME_ORDER,
        DURATION_ORDER,
        POINT_ORDER,
        LIST_NOTHING_ORDER,
        LIST_BOOLEAN_ORDER,
        LIST_STRING_ORDER,
        LIST_INTEGER_ORDER,
        LIST_FLOAT_ORDER,
        LIST_DATE_ORDER,
        LIST_LOCAL_TIME_ORDER,
        LIST_ZONED_TIME_ORDER,
        LIST_LOCAL_DATETIME_ORDER,
        LIST_ZONED_DATETIME_ORDER,
        LIST_DURATION_ORDER,
        LIST_POINT_ORDER,

        LIST_ANY_ORDER,
        ANY_ORDER;
    }

    Set<SchemaValueType> CONSTRAINABLE_LIST_TYPES = Set.of(
            SchemaValueType.LIST_BOOLEAN,
            SchemaValueType.LIST_STRING,
            SchemaValueType.LIST_INTEGER,
            SchemaValueType.LIST_FLOAT,
            SchemaValueType.LIST_DATE,
            SchemaValueType.LIST_ZONED_TIME,
            SchemaValueType.LIST_LOCAL_TIME,
            SchemaValueType.LIST_LOCAL_DATETIME,
            SchemaValueType.LIST_ZONED_DATETIME,
            SchemaValueType.LIST_DURATION,
            SchemaValueType.LIST_POINT);

    static int compare(TypeRepresentation t1, TypeRepresentation t2) {
        return t1.order().compareTo(t2.order());
    }

    /**
     * Infer the TypeRepresentation of a Value
     * @param value {@link Value}
     * @return The inferred {@link TypeRepresentation}
     */
    static TypeRepresentation infer(Value value) {
        // Should always be the narrowest type in the type representation
        // that applies to the value
        if (value == null || value == Values.NO_VALUE) {
            return SpecialTypes.NULL;
        } else if (value instanceof ScalarValue) {
            if (value instanceof BooleanValue) {
                return SchemaValueType.BOOLEAN;
            } else if (value instanceof TextValue) {
                return SchemaValueType.STRING;
            } else if (value instanceof IntegralValue) {
                return SchemaValueType.INTEGER;
            } else if (value instanceof FloatingPointValue) {
                return SchemaValueType.FLOAT;
            } else if (value instanceof DateValue) {
                return SchemaValueType.DATE;
            } else if (value instanceof DurationValue) {
                return SchemaValueType.DURATION;
            } else if (value instanceof LocalDateTimeValue) {
                return SchemaValueType.LOCAL_DATETIME;
            } else if (value instanceof DateTimeValue) {
                return SchemaValueType.ZONED_DATETIME;
            } else if (value instanceof TimeValue) {
                return SchemaValueType.ZONED_TIME;
            } else if (value instanceof LocalTimeValue) {
                return SchemaValueType.LOCAL_TIME;
            } else if (value instanceof PointValue) {
                return SchemaValueType.POINT;
            }
        } else if (value instanceof ArrayValue array) {
            if (array.isEmpty()) {
                return SpecialTypes.LIST_NOTHING;
            } else if (value instanceof BooleanArray) {
                return SchemaValueType.LIST_BOOLEAN;
            } else if (value instanceof TextArray) {
                return SchemaValueType.LIST_STRING;
            } else if (value instanceof IntegralArray) {
                return SchemaValueType.LIST_INTEGER;
            } else if (value instanceof FloatingPointArray) {
                return SchemaValueType.LIST_FLOAT;
            } else if (value instanceof DateArray) {
                return SchemaValueType.LIST_DATE;
            } else if (value instanceof DurationArray) {
                return SchemaValueType.LIST_DURATION;
            } else if (value instanceof LocalDateTimeArray) {
                return SchemaValueType.LIST_LOCAL_DATETIME;
            } else if (value instanceof DateTimeArray) {
                return SchemaValueType.LIST_ZONED_DATETIME;
            } else if (value instanceof TimeArray) {
                return SchemaValueType.LIST_ZONED_TIME;
            } else if (value instanceof LocalTimeArray) {
                return SchemaValueType.LIST_LOCAL_TIME;
            } else if (value instanceof PointArray) {
                return SchemaValueType.LIST_POINT;
            }

            return SpecialTypes.LIST_ANY;
        }

        return SpecialTypes.ANY;
    }

    /**
     * Evaluate if a PropertyTypeSet disallows a certain value.
     *
     * @param set {@link PropertyTypeSet}
     * @param value {@link Value}
     * @return True if the inferred type of value does not belong to the set.
     */
    static boolean disallows(PropertyTypeSet set, Value value) {
        Objects.requireNonNull(set);
        return !set.contains(infer(value));
    }

    static boolean isList(SchemaValueType type) {
        return CONSTRAINABLE_LIST_TYPES.contains(type);
    }

    static boolean isNullable(TypeRepresentation type) {
        // All of the type constraints currently permits NULL values
        return true;
    }

    static boolean hasListTypes(PropertyTypeSet set) {
        return set.contains(SpecialTypes.LIST_NOTHING);
    }

    static boolean isUnion(PropertyTypeSet set) {
        return set.size() > 1;
    }

    /**
     * Evaluate if a PropertyTypeSet follows the business rules for the type constraints.
     *
     * @param set {@link PropertyTypeSet}
     * @throws IllegalArgumentException if the set violates the business rules.
     */
    static void validate(PropertyTypeSet set) {
        var size = set.size();

        if (size == 0) {
            throw new IllegalArgumentException("Unable to create property type constraint because the provided union '"
                    + set.userDescription() + "' is not legal: Must specify at least one property type.");
        }
    }
}
