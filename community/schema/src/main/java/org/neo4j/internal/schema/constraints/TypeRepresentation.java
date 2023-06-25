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

import java.util.Objects;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.ScalarValue;
import org.neo4j.values.storable.TextValue;
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
        LIST_ANY_ORDER,
        ANY_ORDER;
    }

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
        var type = infer(value);

        if (type == SpecialTypes.NULL) {
            // All of the type constraints currently permits NULL values
            return false;
        }

        return !set.contains(type);
    }
}
