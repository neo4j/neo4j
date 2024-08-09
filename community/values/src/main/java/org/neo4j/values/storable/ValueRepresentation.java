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
package org.neo4j.values.storable;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;

/**
 * Enumerates the different Value types and facilitates creating arrays without resorting to reflection or
 * instance of checking at runtime.
 */
public enum ValueRepresentation {
    UNKNOWN(ValueGroup.UNKNOWN, false) {
        @Override
        public ValueRepresentation coerce(ValueRepresentation other) {
            return UNKNOWN;
        }
    },
    ANYTHING(ValueGroup.ANYTHING, false) {
        @Override
        public ValueRepresentation coerce(ValueRepresentation other) {
            return other;
        }
    },
    GEOMETRY_ARRAY(ValueGroup.GEOMETRY_ARRAY, false),
    ZONED_DATE_TIME_ARRAY(ValueGroup.ZONED_DATE_TIME_ARRAY, false),
    LOCAL_DATE_TIME_ARRAY(ValueGroup.LOCAL_DATE_TIME_ARRAY, false),
    DATE_ARRAY(ValueGroup.DATE_ARRAY, false),
    ZONED_TIME_ARRAY(ValueGroup.ZONED_TIME_ARRAY, false),
    LOCAL_TIME_ARRAY(ValueGroup.LOCAL_TIME_ARRAY, false),
    DURATION_ARRAY(ValueGroup.DURATION_ARRAY, false),
    TEXT_ARRAY(ValueGroup.TEXT_ARRAY, false),
    BOOLEAN_ARRAY(ValueGroup.BOOLEAN_ARRAY, false),
    INT64_ARRAY(ValueGroup.NUMBER_ARRAY, false),
    INT32_ARRAY(ValueGroup.NUMBER_ARRAY, false),
    INT16_ARRAY(ValueGroup.NUMBER_ARRAY, false),
    INT8_ARRAY(ValueGroup.NUMBER_ARRAY, false),
    FLOAT64_ARRAY(ValueGroup.NUMBER_ARRAY, false),
    FLOAT32_ARRAY(ValueGroup.NUMBER_ARRAY, false),
    GEOMETRY(ValueGroup.GEOMETRY, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            PointValue[] points = new PointValue[values.intSize()];
            int i = 0;
            PointValue first = null;
            for (AnyValue value : values) {
                PointValue current = getOrFail(value, PointValue.class);
                if (first == null) {
                    first = current;
                } else {
                    if (!first.getCoordinateReferenceSystem().equals(current.getCoordinateReferenceSystem())) {
                        throw new CypherTypeException(
                                "Collections containing point values with different CRS can not be stored in properties.");
                    } else if (first.coordinate().length != current.coordinate().length) {
                        throw new CypherTypeException(
                                "Collections containing point values with different dimensions can not be stored in properties.");
                    }
                }
                points[i++] = current;
            }
            return Values.pointArray(points);
        }
    },
    ZONED_DATE_TIME(ValueGroup.ZONED_DATE_TIME, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            ZonedDateTime[] temporals = new ZonedDateTime[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                temporals[i++] = (getOrFail(value, DateTimeValue.class)).temporal();
            }
            return Values.dateTimeArray(temporals);
        }
    },
    LOCAL_DATE_TIME(ValueGroup.LOCAL_DATE_TIME, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            LocalDateTime[] temporals = new LocalDateTime[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                temporals[i++] = getOrFail(value, LocalDateTimeValue.class).temporal();
            }
            return Values.localDateTimeArray(temporals);
        }
    },
    DATE(ValueGroup.DATE, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            LocalDate[] temporals = new LocalDate[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                temporals[i++] = getOrFail(value, DateValue.class).temporal();
            }
            return Values.dateArray(temporals);
        }
    },
    ZONED_TIME(ValueGroup.ZONED_TIME, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            OffsetTime[] temporals = new OffsetTime[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                temporals[i++] = ((TimeValue) value).temporal();
            }
            return Values.timeArray(temporals);
        }
    },
    LOCAL_TIME(ValueGroup.LOCAL_TIME, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            LocalTime[] temporals = new LocalTime[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                temporals[i++] = ((LocalTimeValue) value).temporal();
            }
            return Values.localTimeArray(temporals);
        }
    },
    DURATION(ValueGroup.DURATION, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            DurationValue[] temporals = new DurationValue[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                temporals[i++] = (DurationValue) value;
            }
            return Values.durationArray(temporals);
        }
    },
    UTF16_TEXT(ValueGroup.TEXT, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            String[] strings = new String[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                strings[i++] = ((TextValue) value).stringValue();
            }
            return Values.stringArray(strings);
        }

        @Override
        public ValueRepresentation coerce(ValueRepresentation other) {
            return switch (other) {
                case UTF8_TEXT, UTF16_TEXT, ANYTHING -> UTF16_TEXT;
                default -> UNKNOWN;
            };
        }
    },
    UTF8_TEXT(ValueGroup.TEXT, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            String[] strings = new String[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                strings[i++] = ((TextValue) value).stringValue();
            }
            return Values.stringArray(strings);
        }

        @Override
        public ValueRepresentation coerce(ValueRepresentation other) {
            return switch (other) {
                case UTF8_TEXT, ANYTHING -> UTF8_TEXT;
                case UTF16_TEXT -> UTF16_TEXT;
                default -> UNKNOWN;
            };
        }
    },
    BOOLEAN(ValueGroup.BOOLEAN, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            boolean[] bools = new boolean[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                bools[i++] = ((BooleanValue) value).booleanValue();
            }
            return Values.booleanArray(bools);
        }
    },
    INT64(ValueGroup.NUMBER, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            long[] longs = new long[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                longs[i++] = getOrFail(value, NumberValue.class).longValue();
            }
            return Values.longArray(longs);
        }

        @Override
        public ValueRepresentation coerce(ValueRepresentation other) {
            return switch (other) {
                case INT8, INT16, INT32, INT64, ANYTHING -> this;
                case FLOAT32, FLOAT64 -> FLOAT64;
                default -> ValueRepresentation.UNKNOWN;
            };
        }
    },
    INT32(ValueGroup.NUMBER, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            int[] ints = new int[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                ints[i++] = getOrFail(value, IntegralValue.class).intValue();
            }
            return Values.intArray(ints);
        }

        @Override
        public ValueRepresentation coerce(ValueRepresentation other) {
            return switch (other) {
                case INT8, INT16, INT32, ANYTHING -> this;
                case INT64 -> INT64;
                case FLOAT32, FLOAT64 -> FLOAT64;
                default -> ValueRepresentation.UNKNOWN;
            };
        }
    },
    INT16(ValueGroup.NUMBER, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            short[] shorts = new short[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                shorts[i++] = getOrFail(value, IntegralValue.class).shortValue();
            }
            return Values.shortArray(shorts);
        }

        @Override
        public ValueRepresentation coerce(ValueRepresentation other) {
            return switch (other) {
                case INT8, INT16, ANYTHING -> this;
                case INT32 -> INT32;
                case INT64 -> INT64;
                case FLOAT32 -> FLOAT32;
                case FLOAT64 -> FLOAT64;
                default -> ValueRepresentation.UNKNOWN;
            };
        }
    },

    INT8(ValueGroup.NUMBER, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            byte[] bytes = new byte[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                bytes[i++] = getOrFail(value, ByteValue.class).value();
            }
            return Values.byteArray(bytes);
        }

        @Override
        public ValueRepresentation coerce(ValueRepresentation other) {
            return switch (other) {
                case INT8, ANYTHING -> this;
                case INT16 -> INT16;
                case INT32 -> INT32;
                case INT64 -> INT64;
                case FLOAT32 -> FLOAT32;
                case FLOAT64 -> FLOAT64;
                default -> ValueRepresentation.UNKNOWN;
            };
        }
    },
    FLOAT64(ValueGroup.NUMBER, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            double[] doubles = new double[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                doubles[i++] = ((NumberValue) value).doubleValue();
            }
            return Values.doubleArray(doubles);
        }

        @Override
        public ValueRepresentation coerce(ValueRepresentation other) {
            return switch (other) {
                case INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, ANYTHING -> this;
                default -> ValueRepresentation.UNKNOWN;
            };
        }
    },
    FLOAT32(ValueGroup.NUMBER, true) {
        @Override
        public ArrayValue arrayOf(SequenceValue values) {
            float[] floats = new float[values.intSize()];
            int i = 0;
            for (AnyValue value : values) {
                NumberValue asNumberValue = getOrFail(value, NumberValue.class);
                if (asNumberValue instanceof FloatValue) {
                    floats[i] = ((FloatValue) asNumberValue).value();
                } else {
                    floats[i] = asNumberValue.longValue();
                }
                i++;
            }
            return Values.floatArray(floats);
        }

        @Override
        public ValueRepresentation coerce(ValueRepresentation other) {
            return switch (other) {
                case INT8, INT16, FLOAT32, ANYTHING -> this;
                case INT32, INT64, FLOAT64 -> FLOAT64;
                default -> ValueRepresentation.UNKNOWN;
            };
        }
    },
    NO_VALUE(ValueGroup.NO_VALUE, false);

    private final ValueGroup group;
    private final boolean canCreateArrayOf;

    ValueRepresentation(ValueGroup group, boolean canCreateArrayOf) {
        this.group = group;
        this.canCreateArrayOf = canCreateArrayOf;
    }

    public boolean canCreateArrayOfValueGroup() {
        return canCreateArrayOf;
    }

    public ValueGroup valueGroup() {
        return group;
    }

    /**
     * Creates an array of the corresponding type.
     *
     * NOTE: must call {@link #canCreateArrayOf} before calling this method.
     * NOTE: it is responsibility of the caller to make sure the provided values all are of the correct type
     *       if not a ClassCastException will be thrown.
     * @param values The values (of the correct type) to create the array of.
     * @return An array of the provided values.
     */
    public ArrayValue arrayOf(SequenceValue values) {
        // NOTE: coming here means that we know we'll fail, just a matter of finding an appropriate error message.
        AnyValue prev = null;
        for (AnyValue value : values) {
            if (value == Values.NO_VALUE) {
                throw new CypherTypeException("Collections containing null values can not be stored in properties.");
            } else if (value instanceof SequenceValue) {
                throw new CypherTypeException("Collections containing collections can not be stored in properties.");
            } else if (prev != null
                    && prev.valueRepresentation().valueGroup()
                            != (value.valueRepresentation().valueGroup())) {
                throw new CypherTypeException(
                        "Neo4j only supports a subset of Cypher types for storage as singleton or array properties. "
                                + "Please refer to section cypher/syntax/values of the manual for more details.");
            } else if (!value.valueRepresentation().canCreateArrayOfValueGroup()) {
                throw new CypherTypeException(String.format(
                        "Property values can only be of primitive types or arrays thereof. Encountered: %s.", value));
            }
            prev = value;
        }

        throw failure();
    }

    /**
     * Finds a representation which fits this and provided representation.
     * @param other the representation to coerce.
     * @return a representation that can handle both representations.
     */
    public ValueRepresentation coerce(ValueRepresentation other) {
        if (valueGroup() == other.valueGroup() || other.valueGroup() == ValueGroup.ANYTHING) {
            return this;
        } else if (valueGroup() == ValueGroup.ANYTHING) {
            return other;
        } else {
            return ValueRepresentation.UNKNOWN;
        }
    }

    private static <T> T getOrFail(AnyValue value, Class<T> type) {
        if (type.isAssignableFrom(value.getClass())) {
            return type.cast(value);
        } else if (value == Values.NO_VALUE) {
            throw new CypherTypeException("Collections containing null values can not be stored in properties.");
        } else if (value instanceof SequenceValue) {
            throw new CypherTypeException("Collections containing collections can not be stored in properties.");
        } else {
            throw failure();
        }
    }

    private static CypherTypeException failure() {
        return new CypherTypeException("Property values can only be of primitive types or arrays thereof");
    }
}
