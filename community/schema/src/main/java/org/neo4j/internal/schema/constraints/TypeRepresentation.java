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
import org.neo4j.values.storable.Float32Vector;
import org.neo4j.values.storable.Float64Vector;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.Int16Vector;
import org.neo4j.values.storable.Int32Vector;
import org.neo4j.values.storable.Int64Vector;
import org.neo4j.values.storable.Int8Vector;
import org.neo4j.values.storable.IntegralArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LocalDateTimeArray;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeArray;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeArray;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.UUIDArray;
import org.neo4j.values.storable.UUIDValue;
import org.neo4j.values.storable.Value;

public sealed interface TypeRepresentation permits ConstrainableType, SpecialTypes {

    String userDescription();

    Ordering order();

    enum Ordering {
        NULL_ORDER,
        BOOLEAN_ORDER,
        STRING_ORDER,
        UUID_ORDER,
        INTEGER_ORDER,
        FLOAT_ORDER,
        DATE_ORDER,
        LOCAL_TIME_ORDER,
        ZONED_TIME_ORDER,
        LOCAL_DATETIME_ORDER,
        ZONED_DATETIME_ORDER,
        DURATION_ORDER,
        POINT_ORDER,
        VECTOR_INT8_ORDER,
        VECTOR_INT16_ORDER,
        VECTOR_INT32_ORDER,
        VECTOR_INT64_ORDER,
        VECTOR_FLOAT32_ORDER,
        VECTOR_FLOAT64_ORDER,
        LIST_NOTHING_ORDER,
        LIST_BOOLEAN_ORDER,
        LIST_STRING_ORDER,
        LIST_UUID_ORDER,
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
        ANY_ORDER
    }

    Set<TypeRepresentation> CONSTRAINABLE_LIST_TYPES = Set.of(
            SchemaValueType.LIST_BOOLEAN,
            SchemaValueType.LIST_STRING,
            SchemaValueType.LIST_UUID,
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
        int order = t1.order().compareTo(t2.order());
        if (order == 0 && t1 instanceof VectorType v1 && t2 instanceof VectorType v2) {
            return Integer.compare(v1.dimensions(), v2.dimensions());
        }
        return order;
    }

    /**
     * Infer the TypeRepresentation of a Value
     * @param value {@link Value}
     * @return The inferred {@link TypeRepresentation}
     */
    static TypeRepresentation infer(Value value) {
        // Should always be the narrowest type in the type representation
        // that applies to the value
        return switch (value) {
            case null -> SpecialTypes.NULL;
            case NoValue ignored -> SpecialTypes.NULL;
            case BooleanValue ignored -> SchemaValueType.BOOLEAN;
            case TextValue ignored -> SchemaValueType.STRING;
            case IntegralValue ignored -> SchemaValueType.INTEGER;
            case UUIDValue ignored -> SchemaValueType.UUID;
            case FloatingPointValue ignored -> SchemaValueType.FLOAT;
            case DateValue ignored -> SchemaValueType.DATE;
            case DurationValue ignored -> SchemaValueType.DURATION;
            case LocalDateTimeValue ignored -> SchemaValueType.LOCAL_DATETIME;
            case DateTimeValue ignored -> SchemaValueType.ZONED_DATETIME;
            case TimeValue ignored -> SchemaValueType.ZONED_TIME;
            case LocalTimeValue ignored -> SchemaValueType.LOCAL_TIME;
            case PointValue ignored -> SchemaValueType.POINT;
            case Int8Vector int8Vector -> VectorType.int8Vector(int8Vector.dimensions());
            case Int16Vector int16Vector -> VectorType.int16Vector(int16Vector.dimensions());
            case Int32Vector int32Vector -> VectorType.int32Vector(int32Vector.dimensions());
            case Int64Vector int64Vector -> VectorType.int64Vector(int64Vector.dimensions());
            case Float32Vector float32Vector -> VectorType.float32Vector(float32Vector.dimensions());
            case Float64Vector float64Vector -> VectorType.float64Vector(float64Vector.dimensions());
            case ArrayValue arrayValue when arrayValue.isEmpty() -> SpecialTypes.LIST_NOTHING;
            case BooleanArray ignored -> SchemaValueType.LIST_BOOLEAN;
            case TextArray ignored -> SchemaValueType.LIST_STRING;
            case UUIDArray ignored -> SchemaValueType.LIST_UUID;
            case IntegralArray ignored -> SchemaValueType.LIST_INTEGER;
            case FloatingPointArray ignored -> SchemaValueType.LIST_FLOAT;
            case DateArray ignored -> SchemaValueType.LIST_DATE;
            case DurationArray ignored -> SchemaValueType.LIST_DURATION;
            case LocalDateTimeArray ignored -> SchemaValueType.LIST_LOCAL_DATETIME;
            case DateTimeArray ignored -> SchemaValueType.LIST_ZONED_DATETIME;
            case TimeArray ignored -> SchemaValueType.LIST_ZONED_TIME;
            case LocalTimeArray ignored -> SchemaValueType.LIST_LOCAL_TIME;
            case PointArray ignored -> SchemaValueType.LIST_POINT;
            case ArrayValue ignored -> SpecialTypes.LIST_ANY;
            default -> SpecialTypes.ANY;
        };
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

    static boolean isList(TypeRepresentation type) {
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

    static boolean hasVectorTypes(PropertyTypeSet set) {
        for (ConstrainableType member : set) {
            if (member instanceof VectorType) {
                return true;
            }
        }
        return false;
    }

    /**
     * Evaluate if a PropertyTypeSet follows the business rules for the type constraints.
     *
     * @param set {@link PropertyTypeSet}
     * @throws IllegalArgumentException if the set violates the business rules.
     */
    static void validate(PropertyTypeSet set) {
        int size = set.size();

        if (size == 0) {
            throw new IllegalArgumentException("Unable to create property type constraint because the provided union '"
                    + set.userDescription() + "' is not legal: Must specify at least one property type.");
        }
    }

    static ConstrainableType deserialize(String s) throws IllegalArgumentException {
        try {
            return SchemaValueType.deserialize(s);
        } catch (IllegalArgumentException exc) {
            return VectorType.deserialize(s);
        }
    }
}
