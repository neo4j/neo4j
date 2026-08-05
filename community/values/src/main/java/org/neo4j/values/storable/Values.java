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

import static java.lang.String.format;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.graphdb.Vector;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.virtual.ListValue;

/**
 * Entry point to the values library.
 * <p>
 * The values library centers around the Value class, which represents a value in Neo4j. Values can be correctly
 * checked for equality over different primitive representations, including consistent hashCodes and sorting.
 * <p>
 * To create Values use the factory methods in the Values class.
 * <p>
 * Values come in two major categories: Storable and Virtual. Storable values are valid values for
 * node, relationship and graph properties. Virtual values are not supported as property values, but might be created
 * and returned as part of cypher execution. These include Node, Relationship and Path.
 */
@SuppressWarnings("WeakerAccess")
public final class Values {

    public static final Value NO_VALUE = NoValue.NO_VALUE;

    public static final Value MIN_GLOBAL = DateTimeValue.MIN_VALUE;
    public static final Value MAX_GLOBAL = Values.NO_VALUE;
    public static final Value MIN_NUMBER = Values.doubleValue(Double.NEGATIVE_INFINITY);
    public static final Value MAX_NUMBER = Values.doubleValue(Double.NaN);
    public static final Value ZERO_FLOAT = Values.doubleValue(0.0);
    public static final IntegralValue ZERO_INT = Values.longValue(0);
    public static final StringValue EMPTY_STRING = StringValue.EMPTY;
    public static final Value MIN_STRING = StringValue.EMPTY;
    public static final Value MAX_STRING = BooleanValue.FALSE;
    public static final BooleanValue TRUE = BooleanValue.TRUE;
    public static final BooleanValue FALSE = BooleanValue.FALSE;
    public static final DoubleValue E = Values.doubleValue(Math.E);
    public static final DoubleValue PI = Values.doubleValue(Math.PI);
    public static final DoubleValue NaN = Values.doubleValue(Double.NaN);
    public static final DoubleValue Infinity = Values.doubleValue(Double.POSITIVE_INFINITY);
    public static final DoubleValue NegInfinity = Values.doubleValue(Double.NEGATIVE_INFINITY);
    public static final ArrayValue EMPTY_SHORT_ARRAY = Values.shortArray(ArrayUtils.EMPTY_SHORT_ARRAY);
    public static final ArrayValue EMPTY_BOOLEAN_ARRAY = Values.booleanArray(ArrayUtils.EMPTY_BOOLEAN_ARRAY);
    public static final ArrayValue EMPTY_BYTE_ARRAY = Values.byteArray(ArrayUtils.EMPTY_BYTE_ARRAY);
    public static final ArrayValue EMPTY_CHAR_ARRAY = Values.charArray(ArrayUtils.EMPTY_CHAR_ARRAY);
    public static final ArrayValue EMPTY_INT_ARRAY = Values.intArray(ArrayUtils.EMPTY_INT_ARRAY);
    public static final ArrayValue EMPTY_LONG_ARRAY = Values.longArray(ArrayUtils.EMPTY_LONG_ARRAY);
    public static final ArrayValue EMPTY_FLOAT_ARRAY = Values.floatArray(ArrayUtils.EMPTY_FLOAT_ARRAY);
    public static final ArrayValue EMPTY_DOUBLE_ARRAY = Values.doubleArray(ArrayUtils.EMPTY_DOUBLE_ARRAY);
    public static final TextArray EMPTY_TEXT_ARRAY = new StringArray(new StringValue[0]);

    private Values() {}

    /**
     * Default value comparator. Will correctly compare all storable values and order the value groups according the
     * to orderability group.
     *
     * To get Comparability semantics, use .ternaryCompare
     */
    public static final ValueComparator COMPARATOR = new ValueComparator(ValueGroup::compareTo);

    public static boolean isBooleanValue(Object value) {
        return value instanceof BooleanValue;
    }

    public static boolean isArrayValue(Value value) {
        return value instanceof ArrayValue;
    }

    public static boolean isGeometryValue(Value value) {
        return value instanceof PointValue;
    }

    public static boolean isGeometryArray(Value value) {
        return value instanceof PointArray;
    }

    // DIRECT FACTORY METHODS

    public static StringValue utf8Value(String value) {
        return utf8Value(value.getBytes(StandardCharsets.UTF_8));
    }

    public static Value ut8fOrNoValue(String value) {
        return value == null ? NO_VALUE : utf8Value(value);
    }

    public static StringValue utf8Value(byte[] bytes) {
        return bytes.length == 0 ? EMPTY_STRING : utf8Value(bytes, 0, bytes.length);
    }

    public static StringValue utf8Value(byte[] bytes, int offset, int length) {
        return length == 0 ? EMPTY_STRING : new UTF8StringValue(bytes, offset, length);
    }

    public static StringValue stringValue(String value) {
        return value.isEmpty() ? EMPTY_STRING : new StringWrappingStringValue(value);
    }

    public static Value stringOrNoValue(String value) {
        return value == null ? NO_VALUE : stringValue(value);
    }

    public static NumberValue numberValue(Number number) {
        return switch (number) {
            case Long longNumber -> longValue(longNumber);
            case Integer intNumber -> intValue(intNumber);
            case Double doubleNumber -> doubleValue(doubleNumber);
            case Byte byteNumber -> byteValue(byteNumber);
            case Float floatNumber -> floatValue(floatNumber);
            case Short shortNumber -> shortValue(shortNumber);
            default -> throw new UnsupportedOperationException("Unsupported type of Number " + number);
        };
    }

    public static LongValue longValue(long value) {
        return new LongValue(value);
    }

    public static IntValue intValue(int value) {
        return new IntValue(value);
    }

    public static ShortValue shortValue(short value) {
        return new ShortValue(value);
    }

    public static ByteValue byteValue(byte value) {
        return new ByteValue(value);
    }

    public static BooleanValue booleanValue(boolean value) {
        return value ? BooleanValue.TRUE : BooleanValue.FALSE;
    }

    public static CharValue charValue(char value) {
        return new CharValue(value);
    }

    public static DoubleValue doubleValue(double value) {
        return new DoubleValue(value);
    }

    public static FloatValue floatValue(float value) {
        return new FloatValue(value);
    }

    /**
     * This method creates a copy of the input and converts all strings to StringValue.
     * It is preferable to use {@link #stringArray(StringValue...)} if you already have StringValues.
     */
    public static TextArray stringArray(String... value) {
        StringValue[] values = new StringValue[value.length];
        for (int i = 0; i < value.length; i++) {
            String s = value[i];
            values[i] = s == null ? null : stringValue(s);
        }

        return new StringArray(values);
    }

    public static TextArray stringArray(StringValue... value) {
        return new StringArray(value);
    }

    public static ByteArray byteArray(byte[] value) {
        return new ByteArray(value);
    }

    public static LongArray longArray(long[] value) {
        return new LongArray(value);
    }

    public static IntArray intArray(int[] value) {
        return new IntArray(value);
    }

    public static DoubleArray doubleArray(double[] value) {
        return new DoubleArray(value);
    }

    public static FloatArray floatArray(float[] value) {
        return new FloatArray(value);
    }

    public static BooleanArray booleanArray(boolean[] value) {
        return new BooleanArray(value);
    }

    public static CharArray charArray(char[] value) {
        return new CharArray(value);
    }

    public static ShortArray shortArray(short[] value) {
        return new ShortArray(value);
    }

    /**
     * Creates a PointValue, and enforces consistency between the CRS and coordinate dimensions.
     */
    public static PointValue pointValue(CoordinateReferenceSystem crs, double... coordinate) {
        return new PointValue(crs, coordinate);
    }

    public static PointValue point(Point point) {
        return switch (point) {
            case PointValue pointValue -> pointValue;
            default -> {
                double[] coords = point.getCoordinate().getCoordinateCopy();
                yield new PointValue(crs(point.getCRS()), coords);
            }
        };
    }

    public static PointValue minPointValue(PointValue reference) {
        return PointValue.minPointValueOf(reference.getCoordinateReferenceSystem());
    }

    public static PointValue maxPointValue(PointValue reference) {
        return PointValue.maxPointValueOf(reference.getCoordinateReferenceSystem());
    }

    public static PointArray pointArray(Point[] points) {
        PointValue[] values = new PointValue[points.length];
        for (int i = 0; i < points.length; i++) {
            values[i] = Values.point(points[i]);
        }
        return new PointArray(values);
    }

    public static PointArray pointArray(Value[] maybePoints) {
        PointValue[] values = new PointValue[maybePoints.length];
        for (int i = 0; i < maybePoints.length; i++) {
            Value maybePoint = maybePoints[i];
            if (!(maybePoint instanceof PointValue pointValue)) {
                throw new IllegalArgumentException(format(
                        "[%s:%s] is not a supported point value",
                        maybePoint, maybePoint.getClass().getName()));
            }
            values[i] = Values.point(pointValue);
        }
        return pointArray(values);
    }

    public static PointArray pointArray(PointValue[] points) {
        return new PointArray(points);
    }

    public static CoordinateReferenceSystem crs(CRS crs) {
        return CoordinateReferenceSystem.get(crs);
    }

    public static Value temporalValue(Temporal value) {
        return switch (value) {
            case ZonedDateTime zonedDateTime -> datetime(zonedDateTime);
            case OffsetDateTime offsetDateTime -> datetime(offsetDateTime);
            case LocalDateTime localDateTime -> localDateTime(localDateTime);
            case OffsetTime offsetTime -> time(offsetTime);
            case LocalDate localDate -> date(localDate);
            case LocalTime localTime -> localTime(localTime);
            case TemporalValue<?, ?> temporalValue -> temporalValue;
            case null -> NO_VALUE;
            default -> throw new UnsupportedOperationException("Unsupported type of Temporal " + value);
        };
    }

    public static DurationValue durationValue(TemporalAmount value) {
        return switch (value) {
            case Duration duration -> duration(duration);
            case Period period -> duration(period);
            case DurationValue durationValue -> durationValue;
            default -> {
                var duration = duration(0, 0, 0, 0);
                for (final var unit : value.getUnits()) {
                    duration = duration.plus(value.get(unit), unit);
                }
                yield duration;
            }
        };
    }

    public static DateTimeArray dateTimeArray(ZonedDateTime[] values) {
        return new DateTimeArray(values);
    }

    public static LocalDateTimeArray localDateTimeArray(LocalDateTime[] values) {
        return new LocalDateTimeArray(values);
    }

    public static LocalTimeArray localTimeArray(LocalTime[] values) {
        return new LocalTimeArray(values);
    }

    public static TimeArray timeArray(OffsetTime[] values) {
        return new TimeArray(values);
    }

    public static DateArray dateArray(LocalDate[] values) {
        return new DateArray(values);
    }

    public static DurationArray durationArray(DurationValue[] values) {
        return new DurationArray(values);
    }

    public static DurationArray durationArray(TemporalAmount[] values) {
        DurationValue[] durations = new DurationValue[values.length];
        for (int i = 0; i < values.length; i++) {
            durations[i] = durationValue(values[i]);
        }
        return new DurationArray(durations);
    }

    public static VectorValue vectorValue(Vector vector) {
        return switch (vector) {
            case VectorValue value -> value;
            default -> throw new UnsupportedOperationException("Unsupported type of Vector " + vector);
        };
    }

    public static VectorValue vectorValue(NumberArray array) {
        return switch (array) {
            case ByteArray byteArray -> int8Vector(byteArray.asObjectCopy());
            case ShortArray shortArray -> int16Vector(shortArray.asObjectCopy());
            case IntArray intArray -> int32Vector(intArray.asObjectCopy());
            case LongArray longArray -> int64Vector(longArray.asObjectCopy());
            case FloatArray floatArray -> float32Vector(floatArray.asObjectCopy());
            case DoubleArray doubleArray -> float64Vector(doubleArray.asObjectCopy());
        };
    }

    public static VectorValue vectorValue(AnyValue value) {
        return switch (value) {
            case VectorValue vector -> vector;
            case NumberArray array -> vectorValue(array);
            case SequenceValue sequence -> {
                ListValue list = sequence.asListValue();
                if (list.itemValueRepresentation().valueGroup() == ValueGroup.NUMBER
                        && list.toStorableArray() instanceof NumberArray array) {
                    yield vectorValue(array);
                }
                throw new UnsupportedOperationException("Unsupported type of SequenceValue " + value);
            }
            default -> throw new UnsupportedOperationException("Unsupported type of AnyValue " + value);
        };
    }

    public static Int64Vector int64Vector(long... coordinates) {
        VectorValue.ensureValidDimensions(coordinates.length);
        return new Int64Vector(coordinates);
    }

    public static Int32Vector int32Vector(int... coordinates) {
        VectorValue.ensureValidDimensions(coordinates.length);
        return new Int32Vector(coordinates);
    }

    public static Int16Vector int16Vector(short... coordinates) {
        VectorValue.ensureValidDimensions(coordinates.length);
        return new Int16Vector(coordinates);
    }

    public static Int8Vector int8Vector(byte... coordinates) {
        VectorValue.ensureValidDimensions(coordinates.length);
        return new Int8Vector(coordinates);
    }

    public static Float64Vector float64Vector(double... coordinates) {
        VectorValue.ensureValidDimensions(coordinates.length);
        VectorValue.ensureFiniteCoordinates(coordinates);
        return uncheckedFloat64Vector(coordinates);
    }

    public static Float64Vector uncheckedFloat64Vector(double[] coordinates) {
        return new Float64Vector(coordinates);
    }

    public static Float32Vector float32Vector(float... coordinates) {
        VectorValue.ensureValidDimensions(coordinates.length);
        VectorValue.ensureFiniteCoordinates(coordinates);
        return uncheckedFloat32Vector(coordinates);
    }

    public static Float32Vector uncheckedFloat32Vector(float[] coordinates) {
        return new Float32Vector(coordinates);
    }

    public static VectorArray vectorArray(SequenceValue sequence) {
        VectorValue[] vectors = new VectorValue[sequence.intSize()];
        int i = 0;
        for (AnyValue value : sequence) {
            vectors[i] = Values.vectorValue(value);
        }
        return vectorArray(vectors);
    }

    public static VectorArray vectorArray(VectorValue... vectors) {
        return new VectorArray(vectors);
    }

    public static UUIDValue uuidValue(long msb, long lsb) {
        return new UUIDValue(new UUID(msb, lsb));
    }

    public static UUIDValue uuidValue(UUID uuid) {
        return new UUIDValue(uuid);
    }

    public static UUIDValue uuidValue(String uuid) {
        return new UUIDValue(UUID.fromString(uuid));
    }

    public static UUIDArray uuidArray(UUID[] values) {
        return new UUIDArray(values);
    }

    public static UnsupportedValue unsupportedValue(String name, String minProtocolVersion, String message) {
        return new UnsupportedValue(name, minProtocolVersion, message);
    }

    // BOXED FACTORY METHODS

    /**
     * Generic value factory method.
     * <p>
     * Beware, this method is intended for converting externally supplied values to the internal Value type, and to
     * make testing convenient. Passing a Value as in parameter should never be needed, and will throw an
     * UnsupportedOperationException.
     * <p>
     * This method does defensive copying of arrays, while the explicit *Array() factory methods do not.
     *
     * @param value Object to convert to Value
     * @return the created Value
     */
    public static Value of(Object value) {
        return of(value, true);
    }

    public static Value of(Object value, boolean allowNull) {
        Value of = unsafeOf(value, allowNull);
        if (of != null) {
            return of;
        }
        Objects.requireNonNull(value);
        throw new IllegalArgumentException(format(
                "[%s:%s] is not a supported property value",
                value, value.getClass().getName()));
    }

    public static Value unsafeOf(Object value, boolean allowNull) {
        return switch (value) {
            case null -> {
                if (allowNull) {
                    yield NO_VALUE;
                }
                throw new IllegalArgumentException("[null] is not a supported property value");
            }
            case String string -> utf8Value(string.getBytes(StandardCharsets.UTF_8));
            case Object[] array -> arrayValue(array, true);
            case Boolean bool -> booleanValue(bool);
            case Number number -> numberValue(number);
            case Character character -> charValue(character);
            case Temporal temporal -> temporalValue(temporal);
            case TemporalAmount temporalAmount -> durationValue(temporalAmount);
            case byte[] byteArray -> byteArray(Arrays.copyOf(byteArray, byteArray.length));
            case long[] longArray -> longArray(Arrays.copyOf(longArray, longArray.length));
            case int[] intArray -> intArray(Arrays.copyOf(intArray, intArray.length));
            case double[] doubleArray -> doubleArray(Arrays.copyOf(doubleArray, doubleArray.length));
            case float[] floatArray -> floatArray(Arrays.copyOf(floatArray, floatArray.length));
            case boolean[] boolArray -> booleanArray(Arrays.copyOf(boolArray, boolArray.length));
            case char[] charArray -> charArray(Arrays.copyOf(charArray, charArray.length));
            case short[] shortArray -> shortArray(Arrays.copyOf(shortArray, shortArray.length));
            case Point point -> point(point);
            case Vector vector -> vectorValue(vector);
            case UUID uuid -> uuidValue(uuid);
            case Value ignored ->
                throw new UnsupportedOperationException(
                        "Converting a Value to a Value using Values.of() is not supported.");
            default -> null; // otherwise fail
        };
    }

    /**
     * Generic value factory method.
     * <p>
     * Converts an array of object values to the internal Value type. See {@link Values#of}.
     */
    public static Value[] values(Object... objects) {
        return Arrays.stream(objects).map(Values::of).toArray(Value[]::new);
    }

    public static Object[] asObjects(Value[] propertyValues) {
        Object[] legacy = new Object[propertyValues.length];

        for (int i = 0; i < propertyValues.length; i++) {
            legacy[i] = propertyValues[i].asObjectCopy();
        }

        return legacy;
    }

    public static ArrayValue arrayValue(Object[] value, boolean copyDefensively) {
        return switch (value) {
            case String[] array -> {
                if (copyDefensively) {
                    // If we copy anyway, we might as well make them all UTF-8
                    StringValue[] copy = new StringValue[value.length];
                    for (int i = 0; i < value.length; i++) {
                        String s = array[i];
                        copy[i] = s == null ? null : utf8Value(s);
                    }
                    yield stringArray(copy);
                } else {
                    yield stringArray(array);
                }
            }
            case Byte[] array -> byteArray(copy(array, new byte[array.length]));
            case Long[] array -> longArray(copy(array, new long[array.length]));
            case Integer[] array -> intArray(copy(array, new int[array.length]));
            case Double[] array -> doubleArray(copy(array, new double[array.length]));
            case Float[] array -> floatArray(copy(array, new float[array.length]));
            case Boolean[] array -> booleanArray(copy(array, new boolean[array.length]));
            case Character[] array -> charArray(copy(array, new char[array.length]));
            case Short[] array -> shortArray(copy(array, new short[array.length]));
            case PointValue[] array -> pointArray(copyDefensively ? copy(array, new PointValue[array.length]) : array);
            case Point[] array -> pointArray(array); // no need to copy here, pointArray copies
            case ZonedDateTime[] array ->
                dateTimeArray(copyDefensively ? copy(array, new ZonedDateTime[array.length]) : array);
            case LocalDateTime[] array ->
                localDateTimeArray(copyDefensively ? copy(array, new LocalDateTime[array.length]) : array);
            case OffsetTime[] array -> timeArray(copyDefensively ? copy(array, new OffsetTime[array.length]) : array);
            case LocalTime[] array ->
                localTimeArray(copyDefensively ? copy(array, new LocalTime[array.length]) : array);
            case LocalDate[] array -> dateArray(copyDefensively ? copy(array, new LocalDate[array.length]) : array);
            case TemporalAmount[] array -> durationArray(array); // no need to copy here, durationArray will copy
            case VectorValue[] array ->
                vectorArray(copyDefensively ? copy(array, new VectorValue[array.length]) : array);
            case UUID[] array -> uuidArray(array);
            default -> null;
        };
    }

    private static <T> T copy(Object[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException("Property array value elements may not be null.");
            }
            Array.set(target, i, value[i]);
        }
        return target;
    }

    public static Value minValue(ValueGroup valueGroup, Value value) {
        return switch (valueGroup) {
            case TEXT -> MIN_STRING;
            case NUMBER -> MIN_NUMBER;
            case GEOMETRY -> minPointValue((PointValue) value);
            case DATE -> DateValue.MIN_VALUE;
            case LOCAL_DATE_TIME -> LocalDateTimeValue.MIN_VALUE;
            case ZONED_DATE_TIME -> DateTimeValue.MIN_VALUE;
            case LOCAL_TIME -> LocalTimeValue.MIN_VALUE;
            case ZONED_TIME -> TimeValue.MIN_VALUE;
            default ->
                throw new IllegalStateException(
                        format("The minValue for valueGroup %s is not defined yet", valueGroup));
        };
    }

    public static Value maxValue(ValueGroup valueGroup, Value value) {
        return switch (valueGroup) {
            case TEXT -> MAX_STRING;
            case NUMBER -> MAX_NUMBER;
            case GEOMETRY -> maxPointValue((PointValue) value);
            case DATE -> DateValue.MAX_VALUE;
            case LOCAL_DATE_TIME -> LocalDateTimeValue.MAX_VALUE;
            case ZONED_DATE_TIME -> DateTimeValue.MAX_VALUE;
            case LOCAL_TIME -> LocalTimeValue.MAX_VALUE;
            case ZONED_TIME -> TimeValue.MAX_VALUE;
            default ->
                throw new IllegalStateException(
                        format("The maxValue for valueGroup %s is not defined yet", valueGroup));
        };
    }
}
