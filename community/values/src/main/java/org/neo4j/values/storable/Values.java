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
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Objects;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Point;

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
    public static final Value MIN_STRING = StringValue.EMPTY;
    public static final Value MAX_STRING = Values.booleanValue(false);
    public static final BooleanValue TRUE = Values.booleanValue(true);
    public static final BooleanValue FALSE = Values.booleanValue(false);
    public static final TextValue EMPTY_STRING = StringValue.EMPTY;
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
    public static final TextArray EMPTY_TEXT_ARRAY = Values.stringArray();

    private Values() {}

    /**
     * Default value comparator. Will correctly compare all storable values and order the value groups according the
     * to orderability group.
     *
     * To get Comparability semantics, use .ternaryCompare
     */
    public static final ValueComparator COMPARATOR = new ValueComparator(ValueGroup::compareTo);

    public static boolean isNumberValue(Object value) {
        return value instanceof NumberValue;
    }

    public static boolean isBooleanValue(Object value) {
        return value instanceof BooleanValue;
    }

    public static boolean isTextValue(Object value) {
        return value instanceof TextValue;
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

    public static boolean isTemporalValue(Value value) {
        return value instanceof TemporalValue || value instanceof DurationValue;
    }

    public static boolean isTemporalArray(Value value) {
        return value instanceof TemporalArray || value instanceof DurationArray;
    }

    public static double coerceToDouble(Value value) {
        if (value instanceof IntegralValue integralValue) {
            return integralValue.longValue();
        }
        if (value instanceof FloatingPointValue floatingPointValue) {
            return floatingPointValue.doubleValue();
        }
        throw new UnsupportedOperationException(format("Cannot coerce %s to double", value));
    }

    // DIRECT FACTORY METHODS

    public static TextValue utf8Value(String value) {
        return utf8Value(value.getBytes(StandardCharsets.UTF_8));
    }

    public static Value ut8fOrNoValue(String value) {
        if (value == null) {
            return NO_VALUE;
        } else {
            return utf8Value(value);
        }
    }

    public static TextValue utf8Value(byte[] bytes) {
        if (bytes.length == 0) {
            return EMPTY_STRING;
        }

        return utf8Value(bytes, 0, bytes.length);
    }

    public static TextValue utf8Value(byte[] bytes, int offset, int length) {
        if (length == 0) {
            return EMPTY_STRING;
        }

        return new UTF8StringValue(bytes, offset, length);
    }

    public static TextValue stringValue(String value) {
        if (value.isEmpty()) {
            return EMPTY_STRING;
        }
        return new StringWrappingStringValue(value);
    }

    public static Value stringOrNoValue(String value) {
        if (value == null) {
            return NO_VALUE;
        } else {
            return stringValue(value);
        }
    }

    public static NumberValue numberValue(Number number) {
        if (number instanceof Long longNumber) {
            return longValue(longNumber);
        }
        if (number instanceof Integer intNumber) {
            return intValue(intNumber);
        }
        if (number instanceof Double doubleNumber) {
            return doubleValue(doubleNumber);
        }
        if (number instanceof Byte byteNumber) {
            return byteValue(byteNumber);
        }
        if (number instanceof Float floatNumber) {
            return floatValue(floatNumber);
        }
        if (number instanceof Short shortNumber) {
            return shortValue(shortNumber);
        }

        throw new UnsupportedOperationException("Unsupported type of Number " + number);
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

    public static TextArray stringArray(String... value) {
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
        // An optimization could be to do an instanceof PointValue check here
        // and in that case just return the casted argument.
        double[] coords = point.getCoordinate().getCoordinateCopy();
        return new PointValue(crs(point.getCRS()), coords);
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
            if (!(maybePoint instanceof PointValue)) {
                throw new IllegalArgumentException(format(
                        "[%s:%s] is not a supported point value",
                        maybePoint, maybePoint.getClass().getName()));
            }
            values[i] = Values.point((PointValue) maybePoint);
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
        if (value instanceof ZonedDateTime zonedDateTime) {
            return datetime(zonedDateTime);
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return datetime(offsetDateTime);
        }
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime(localDateTime);
        }
        if (value instanceof OffsetTime offsetTime) {
            return time(offsetTime);
        }
        if (value instanceof LocalDate localDate) {
            return date(localDate);
        }
        if (value instanceof LocalTime localTime) {
            return localTime(localTime);
        }
        if (value instanceof TemporalValue temporalValue) {
            return temporalValue;
        }
        if (value == null) {
            return NO_VALUE;
        }

        throw new UnsupportedOperationException("Unsupported type of Temporal " + value);
    }

    public static DurationValue durationValue(TemporalAmount value) {
        if (value instanceof Duration duration) {
            return duration(duration);
        }
        if (value instanceof Period period) {
            return duration(period);
        }
        if (value instanceof DurationValue durationValue) {
            return durationValue;
        }
        DurationValue duration = duration(0, 0, 0, 0);
        for (TemporalUnit unit : value.getUnits()) {
            duration = duration.plus(value.get(unit), unit);
        }
        return duration;
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
        if (value == null) {
            if (allowNull) {
                return NO_VALUE;
            }
            throw new IllegalArgumentException("[null] is not a supported property value");
        }
        if (value instanceof String string) {
            return utf8Value(string.getBytes(StandardCharsets.UTF_8));
        }
        if (value instanceof Object[] array) {
            return arrayValue(array, true);
        }
        if (value instanceof Boolean bool) {
            return booleanValue(bool);
        }
        if (value instanceof Number number) {
            return numberValue(number);
        }
        if (value instanceof Character character) {
            return charValue(character);
        }
        if (value instanceof Temporal temporal) {
            return temporalValue(temporal);
        }
        if (value instanceof TemporalAmount temporalAmount) {
            return durationValue(temporalAmount);
        }
        if (value instanceof byte[] byteArray) {
            return byteArray(Arrays.copyOf(byteArray, byteArray.length));
        }
        if (value instanceof long[] longArray) {
            return longArray(Arrays.copyOf(longArray, longArray.length));
        }
        if (value instanceof int[] intArray) {
            return intArray(Arrays.copyOf(intArray, intArray.length));
        }
        if (value instanceof double[] doubleArray) {
            return doubleArray(Arrays.copyOf(doubleArray, doubleArray.length));
        }
        if (value instanceof float[] floatArray) {
            return floatArray(Arrays.copyOf(floatArray, floatArray.length));
        }
        if (value instanceof boolean[] boolArray) {
            return booleanArray(Arrays.copyOf(boolArray, boolArray.length));
        }
        if (value instanceof char[] charArray) {
            return charArray(Arrays.copyOf(charArray, charArray.length));
        }
        if (value instanceof short[] shortArray) {
            return shortArray(Arrays.copyOf(shortArray, shortArray.length));
        }
        if (value instanceof Point point) {
            return Values.point(point);
        }
        if (value instanceof Value) {
            throw new UnsupportedOperationException(
                    "Converting a Value to a Value using Values.of() is not supported.");
        }

        // otherwise fail
        return null;
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

    public static Value arrayValue(Object[] value, boolean copyDefensively) {
        if (value instanceof String[] array) {
            return stringArray(copyDefensively ? copy(value, new String[value.length]) : array);
        }
        if (value instanceof Byte[]) {
            return byteArray(copy(value, new byte[value.length]));
        }
        if (value instanceof Long[]) {
            return longArray(copy(value, new long[value.length]));
        }
        if (value instanceof Integer[]) {
            return intArray(copy(value, new int[value.length]));
        }
        if (value instanceof Double[]) {
            return doubleArray(copy(value, new double[value.length]));
        }
        if (value instanceof Float[]) {
            return floatArray(copy(value, new float[value.length]));
        }
        if (value instanceof Boolean[]) {
            return booleanArray(copy(value, new boolean[value.length]));
        }
        if (value instanceof Character[]) {
            return charArray(copy(value, new char[value.length]));
        }
        if (value instanceof Short[]) {
            return shortArray(copy(value, new short[value.length]));
        }
        if (value instanceof PointValue[] array) {
            return pointArray(copyDefensively ? copy(value, new PointValue[value.length]) : array);
        }
        if (value instanceof Point[] array) {
            // no need to copy here, since the pointArray(...) method will copy into a PointValue[]
            return pointArray(array);
        }
        if (value instanceof ZonedDateTime[] array) {
            return dateTimeArray(copyDefensively ? copy(value, new ZonedDateTime[value.length]) : array);
        }
        if (value instanceof LocalDateTime[] array) {
            return localDateTimeArray(copyDefensively ? copy(value, new LocalDateTime[value.length]) : array);
        }
        if (value instanceof LocalTime[] array) {
            return localTimeArray(copyDefensively ? copy(value, new LocalTime[value.length]) : array);
        }
        if (value instanceof OffsetTime[] array) {
            return timeArray(copyDefensively ? copy(value, new OffsetTime[value.length]) : array);
        }
        if (value instanceof LocalDate[] array) {
            return dateArray(copyDefensively ? copy(value, new LocalDate[value.length]) : array);
        }
        if (value instanceof TemporalAmount[] array) {
            // no need to copy here, since the durationArray(...) method will perform copying as appropriate
            return durationArray(array);
        }
        return null;
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
            default -> throw new IllegalStateException(
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
            default -> throw new IllegalStateException(
                    format("The maxValue for valueGroup %s is not defined yet", valueGroup));
        };
    }
}
