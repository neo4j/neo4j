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
package org.neo4j.values.utils;

import org.neo4j.values.AnyValue;
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
import org.neo4j.values.storable.FloatingPointVector;
import org.neo4j.values.storable.Int16Vector;
import org.neo4j.values.storable.Int32Vector;
import org.neo4j.values.storable.Int64Vector;
import org.neo4j.values.storable.Int8Vector;
import org.neo4j.values.storable.IntegralArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.IntegralVector;
import org.neo4j.values.storable.LocalDateTimeArray;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeArray;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.NonPrimitiveArray;
import org.neo4j.values.storable.NumberArray;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.ScalarValue;
import org.neo4j.values.storable.TemporalArray;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeArray;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.UUIDArray;
import org.neo4j.values.storable.UUIDValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueRepresentation;
import org.neo4j.values.storable.VectorValue;
import org.neo4j.values.virtual.ListValue;

/// This class duplicates much of `CypherTypeValueMapper` in `org.neo4j.values.utils`
/// because we don't have access to that class (and the Scala it uses) at this level.
public class ValueTypeNames {
    public static String nameOfType(Object object) {
        return switch (object) {
            case Value value -> ofRepresentation(value.valueRepresentation(), value);
            case ListValue list -> listOf(list.itemValueRepresentation());
            case AnyValue value -> value.getTypeName();
            case Class<?> type -> nameOfType(type);
            default -> nameOfType(object.getClass());
        };
    }

    @SuppressWarnings("unchecked")
    public static String nameOfType(Class<?> type) {
        if (Value.class.isAssignableFrom(type)) {
            return ofClass((Class<? extends Value>) type);
        }

        String simpleName = type.getSimpleName();
        return simpleName.isBlank() ? type.getTypeName() : simpleName;
    }

    public static String ofRepresentation(ValueRepresentation valueRepresentation) {
        return ofRepresentation(valueRepresentation, null);
    }

    public static String ofRepresentation(ValueRepresentation valueRepresentation, Value value) {
        return switch (valueRepresentation) {
            case UNKNOWN -> "UNKNOWN";
            case ANYTHING -> "ANY";
            case GEOMETRY_ARRAY -> listOf(ValueRepresentation.GEOMETRY);
            case ZONED_DATE_TIME_ARRAY -> listOf(ValueRepresentation.ZONED_DATE_TIME);
            case LOCAL_DATE_TIME_ARRAY -> listOf(ValueRepresentation.LOCAL_DATE_TIME);
            case DATE_ARRAY -> listOf(ValueRepresentation.DATE);
            case ZONED_TIME_ARRAY -> listOf(ValueRepresentation.ZONED_TIME);
            case LOCAL_TIME_ARRAY -> listOf(ValueRepresentation.LOCAL_TIME);
            case DURATION_ARRAY -> listOf(ValueRepresentation.DURATION);
            case TEXT_ARRAY -> listOf(ValueRepresentation.UTF16_TEXT);
            case BOOLEAN_ARRAY -> listOf(ValueRepresentation.BOOLEAN);
            case INT64_ARRAY -> listOf(ValueRepresentation.INT64);
            case INT32_ARRAY -> listOf(ValueRepresentation.INT32);
            case INT16_ARRAY -> listOf(ValueRepresentation.INT16);
            case INT8_ARRAY -> listOf(ValueRepresentation.INT8);
            case FLOAT64_ARRAY -> listOf(ValueRepresentation.FLOAT64);
            case FLOAT32_ARRAY -> listOf(ValueRepresentation.FLOAT32);
            case VECTOR_ARRAY -> listOf("VECTOR");
            case UUID_ARRAY -> listOf(ValueRepresentation.UUID);
            case GEOMETRY -> "POINT";
            case ZONED_DATE_TIME -> DateTimeValue.CYPHER_TYPE_NAME;
            case LOCAL_DATE_TIME -> LocalDateTimeValue.CYPHER_TYPE_NAME;
            case DATE -> DateValue.CYPHER_TYPE_NAME;
            case ZONED_TIME -> TimeValue.CYPHER_TYPE_NAME;
            case LOCAL_TIME -> LocalTimeValue.CYPHER_TYPE_NAME;
            case DURATION -> DurationValue.CYPHER_TYPE_NAME;
            case UTF16_TEXT, UTF8_TEXT -> "STRING";
            case BOOLEAN -> "BOOLEAN";
            case INT8, INT16, INT32, INT64 -> "INTEGER";
            case FLOAT32, FLOAT64 -> "FLOAT";
            case INT8_VECTOR -> vectorOf(Int8Vector.NESTED_TYPE_NAME, value);
            case INT16_VECTOR -> vectorOf(Int16Vector.NESTED_TYPE_NAME, value);
            case INT32_VECTOR -> vectorOf(Int32Vector.NESTED_TYPE_NAME, value);
            case INT64_VECTOR -> vectorOf("INTEGER", value);
            case FLOAT32_VECTOR -> vectorOf(Float32Vector.NESTED_TYPE_NAME, value);
            case FLOAT64_VECTOR -> vectorOf("FLOAT", value);
            case UUID -> UUIDValue.TYPE_NAME;
            case NO_VALUE -> "NULL";
        };
    }

    // reflective checks on Class cannot be done with a nice switch,
    // thus using nested branches to minimise checks
    private static String ofClass(Class<? extends Value> type) {
        if (NoValue.class.isAssignableFrom(type)) {
            return ofRepresentation(ValueRepresentation.NO_VALUE);
        }
        if (ArrayValue.class.isAssignableFrom(type)) {
            if (NonPrimitiveArray.class.isAssignableFrom(type)) {
                if (PointArray.class.isAssignableFrom(type)) {
                    return listOf(ValueRepresentation.GEOMETRY);
                }
                if (TemporalArray.class.isAssignableFrom(type)) {
                    if (DateTimeArray.class.isAssignableFrom(type)) {
                        return listOf(ValueRepresentation.ZONED_DATE_TIME);
                    }
                    if (LocalDateTimeArray.class.isAssignableFrom(type)) {
                        return listOf(ValueRepresentation.LOCAL_DATE_TIME);
                    }
                    if (DateArray.class.isAssignableFrom(type)) {
                        return listOf(ValueRepresentation.DATE);
                    }
                    if (TimeArray.class.isAssignableFrom(type)) {
                        return listOf(ValueRepresentation.ZONED_TIME);
                    }
                    if (LocalTimeArray.class.isAssignableFrom(type)) {
                        return listOf(ValueRepresentation.LOCAL_TIME);
                    }
                }
                if (DurationArray.class.isAssignableFrom(type)) {
                    return listOf(ValueRepresentation.DURATION);
                }
                if (UUIDArray.class.isAssignableFrom(type)) {
                    return listOf(ValueRepresentation.UUID);
                }
            }
            if (TextArray.class.isAssignableFrom(type)) {
                return listOf(ValueRepresentation.UTF16_TEXT);
            }
            if (BooleanArray.class.isAssignableFrom(type)) {
                return listOf(ValueRepresentation.BOOLEAN);
            }
            if (NumberArray.class.isAssignableFrom(type)) {
                if (IntegralArray.class.isAssignableFrom(type)) {
                    return listOf(ValueRepresentation.INT64);
                }
                if (FloatingPointArray.class.isAssignableFrom(type)) {
                    return listOf(ValueRepresentation.FLOAT64);
                }
            }
        }
        if (ScalarValue.class.isAssignableFrom(type)) {
            if (PointValue.class.isAssignableFrom(type)) {
                return ofRepresentation(ValueRepresentation.GEOMETRY);
            }
            if (TemporalValue.class.isAssignableFrom(type)) {
                if (DateTimeValue.class.isAssignableFrom(type)) {
                    return ofRepresentation(ValueRepresentation.ZONED_DATE_TIME);
                }
                if (LocalDateTimeValue.class.isAssignableFrom(type)) {
                    return ofRepresentation(ValueRepresentation.LOCAL_DATE_TIME);
                }
                if (DateValue.class.isAssignableFrom(type)) {
                    return ofRepresentation(ValueRepresentation.DATE);
                }
                if (TimeValue.class.isAssignableFrom(type)) {
                    return ofRepresentation(ValueRepresentation.ZONED_TIME);
                }
                if (LocalTimeValue.class.isAssignableFrom(type)) {
                    return ofRepresentation(ValueRepresentation.LOCAL_TIME);
                }
            }
            if (DurationValue.class.isAssignableFrom(type)) {
                return ofRepresentation(ValueRepresentation.DURATION);
            }
            if (TextValue.class.isAssignableFrom(type)) {
                return ofRepresentation(ValueRepresentation.UTF16_TEXT);
            }
            if (BooleanValue.class.isAssignableFrom(type)) {
                return ofRepresentation(ValueRepresentation.BOOLEAN);
            }
            if (NumberValue.class.isAssignableFrom(type)) {
                if (IntegralValue.class.isAssignableFrom(type)) {
                    return ofRepresentation(ValueRepresentation.INT64);
                }
                if (FloatingPointValue.class.isAssignableFrom(type)) {
                    return ofRepresentation(ValueRepresentation.FLOAT64);
                }
                return ofRepresentation(ValueRepresentation.INT64) + "|"
                        + ofRepresentation(ValueRepresentation.FLOAT64);
            }
            if (VectorValue.class.isAssignableFrom(type)) {
                if (IntegralVector.class.isAssignableFrom(type)) {
                    if (Int8Vector.class.isAssignableFrom(type)) {
                        return ofRepresentation(ValueRepresentation.INT8_VECTOR);
                    }
                    if (Int16Vector.class.isAssignableFrom(type)) {
                        return ofRepresentation(ValueRepresentation.INT16_VECTOR);
                    }
                    if (Int32Vector.class.isAssignableFrom(type)) {
                        return ofRepresentation(ValueRepresentation.INT32_VECTOR);
                    }
                    if (Int64Vector.class.isAssignableFrom(type)) {
                        return ofRepresentation(ValueRepresentation.INT64_VECTOR);
                    }
                }
                if (FloatingPointVector.class.isAssignableFrom(type)) {
                    if (Float32Vector.class.isAssignableFrom(type)) {
                        return ofRepresentation(ValueRepresentation.FLOAT32_VECTOR);
                    }
                    if (Float64Vector.class.isAssignableFrom(type)) {
                        return ofRepresentation(ValueRepresentation.FLOAT64_VECTOR);
                    }
                }
            }
        }
        return type.getSimpleName();
    }

    private static String listOf(ValueRepresentation memberRepresentation) {
        return listOf(ofRepresentation(memberRepresentation));
    }

    private static String listOf(String member) {
        return "LIST<%s>".formatted(member);
    }

    private static String vectorOf(String elementTypeName, Value value) {
        String dimensions = "";
        if (value instanceof VectorValue vector) {
            dimensions = String.valueOf(vector.dimensions());
        }
        return "VECTOR<%s>(%s)".formatted(elementTypeName, dimensions);
    }
}
