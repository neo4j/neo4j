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

import static java.lang.Math.abs;
import static java.time.LocalDate.ofEpochDay;
import static java.time.LocalTime.ofNanoOfDay;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.neo4j.internal.helpers.Numbers.ceilingPowerOfTwo;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortValue;

import java.lang.reflect.Array;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.list.primitive.LongList;

/**
 * Helper class that generates generator values of all supported types.
 * <p>
 * Generated values are always uniformly distributed in pseudorandom fashion.
 * <p>
 * Can generate both {@link Value} and "raw" instances. The "raw" type of a value type means
 * the corresponding Core API type if such type exists. For example, {@code String[]} is the raw type of {@link TextArray}.
 * <p>
 * The length of strings will be governed by {@link RandomValues.Configuration#stringMinLength()} and
 * {@link RandomValues.Configuration#stringMaxLength()} and
 * the length of arrays will be governed by {@link RandomValues.Configuration#arrayMinLength()} and
 * {@link RandomValues.Configuration#arrayMaxLength()}
 * unless method provide explicit arguments for those configurations in which case the provided argument will be used instead.
 */
public class RandomValues {
    public interface Configuration {
        int stringMinLength();

        int stringMaxLength();

        int arrayMinLength();

        int arrayMaxLength();

        int maxCodePoint();

        int minCodePoint();
    }

    public static class Default implements Configuration {
        @Override
        public int stringMinLength() {
            return 5;
        }

        @Override
        public int stringMaxLength() {
            return 20;
        }

        @Override
        public int arrayMinLength() {
            return 1;
        }

        @Override
        public int arrayMaxLength() {
            return 10;
        }

        @Override
        public int maxCodePoint() {
            return Character.MAX_CODE_POINT;
        }

        @Override
        public int minCodePoint() {
            return Character.MIN_CODE_POINT;
        }
    }

    public static final int MAX_BMP_CODE_POINT = 0xFFFF;
    public static final Configuration DEFAULT_CONFIGURATION = new Default();
    static final int MAX_ASCII_CODE_POINT = 0x7F;
    private static final ValueType[] ALL_TYPES = ValueType.values();
    private static final ValueType[] ARRAY_TYPES = ValueType.arrayTypes();
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private final Generator generator;
    private final Configuration configuration;

    private RandomValues(Generator generator) {
        this(generator, DEFAULT_CONFIGURATION);
    }

    private RandomValues(Generator generator, Configuration configuration) {
        this.generator = generator;
        this.configuration = configuration;
    }

    /**
     * Create a {@code RandomValues} with default configuration
     *
     * @return a {@code RandomValues} instance
     */
    public static RandomValues create() {
        return new RandomValues(new RandomGenerator(ThreadLocalRandom.current()));
    }

    /**
     * Create a {@code RandomValues} with the given configuration
     *
     * @return a {@code RandomValues} instance
     */
    public static RandomValues create(Configuration configuration) {
        return new RandomValues(new RandomGenerator(ThreadLocalRandom.current()), configuration);
    }

    /**
     * Create a {@code RandomValues} using the given {@link Random} with given configuration
     *
     * @return a {@code RandomValues} instance
     */
    public static RandomValues create(Random random, Configuration configuration) {
        return new RandomValues(new RandomGenerator(random), configuration);
    }

    /**
     * Create a {@code RandomValues} using the given {@link Random} with default configuration
     *
     * @return a {@code RandomValues} instance
     */
    public static RandomValues create(Random random) {
        return new RandomValues(new RandomGenerator(random));
    }

    /**
     * Create a {@code RandomValues} using the given {@link SplittableRandom} with given configuration
     *
     * @return a {@code RandomValues} instance
     */
    public static RandomValues create(SplittableRandom random, Configuration configuration) {
        return new RandomValues(new SplittableRandomGenerator(random), configuration);
    }

    /**
     * Create a {@code RandomValues} using the given {@link SplittableRandom} with default configuration
     *
     * @return a {@code RandomValues} instance
     */
    public static RandomValues create(SplittableRandom random) {
        return new RandomValues(new SplittableRandomGenerator(random));
    }

    /**
     * Returns the next {@link Value}, distributed uniformly among the supported Value types.
     *
     * @see RandomValues
     */
    public Value nextValue() {
        return nextValueOfTypes(ALL_TYPES);
    }

    /**
     * Returns the next {@link Value}, distributed uniformly among the provided value types.
     *
     * @see RandomValues
     */
    public Value nextValueOfTypes(ValueType... types) {
        return nextValueOfType(among(types));
    }

    /**
     * Returns the next size number of {@link Value}, distributed uniformly among the supported value types.
     *
     * @see RandomValues
     */
    public Value[] nextValues(int size) {
        return nextValuesOfTypes(size, ALL_TYPES);
    }

    /**
     * Returns the next size number of {@link Value}, distributed uniformly among the provided value types.
     *
     * @see RandomValues
     */
    public Value[] nextValuesOfTypes(int size, ValueType... types) {
        var values = new Value[size];
        for (int i = 0; i < size; i++) {
            values[i] = nextValueOfType(among(types));
        }
        return values;
    }

    public static ValueType[] including(Predicate<ValueType> include) {
        return Arrays.stream(ValueType.values()).filter(include).toArray(ValueType[]::new);
    }

    /**
     * Create an array containing all value types, excluding provided types.
     */
    public static ValueType[] excluding(ValueType... exclude) {
        return excluding(ValueType.values(), exclude);
    }

    public static ValueType[] excluding(ValueType[] among, ValueType... exclude) {
        return excluding(among, t -> ArrayUtils.contains(exclude, t));
    }

    public static <T> T[] excluding(T[] among, Predicate<T> exclude) {
        return Arrays.stream(among).filter(exclude.negate()).toArray(length ->
                (T[]) Array.newInstance(among.getClass().getComponentType(), length));
    }

    public static ValueType[] typesOfGroup(ValueGroup valueGroup) {
        return Arrays.stream(ValueType.values())
                .filter(t -> t.valueGroup == valueGroup)
                .toArray(ValueType[]::new);
    }

    /**
     * Returns the next {@link Value} of provided type.
     *
     * @see RandomValues
     */
    public Value nextValueOfType(ValueType type) {
        switch (type) {
            case BOOLEAN:
                return nextBooleanValue();
            case BYTE:
                return nextByteValue();
            case SHORT:
                return nextShortValue();
            case STRING:
                return nextTextValue();
            case INT:
                return nextIntValue();
            case LONG:
                return nextLongValue();
            case FLOAT:
                return nextFloatValue();
            case DOUBLE:
                return nextDoubleValue();
            case CHAR:
                return nextCharValue();
            case STRING_ALPHANUMERIC:
                return nextAlphaNumericTextValue();
            case STRING_ASCII:
                return nextAsciiTextValue();
            case STRING_BMP:
                return nextBasicMultilingualPlaneTextValue();
            case LOCAL_DATE_TIME:
                return nextLocalDateTimeValue();
            case DATE:
                return nextDateValue();
            case LOCAL_TIME:
                return nextLocalTimeValue();
            case PERIOD:
                return nextPeriod();
            case DURATION:
                return nextDuration();
            case TIME:
                return nextTimeValue();
            case DATE_TIME:
                return nextDateTimeValue();
            case CARTESIAN_POINT:
                return nextCartesianPoint();
            case CARTESIAN_POINT_3D:
                return nextCartesian3DPoint();
            case GEOGRAPHIC_POINT:
                return nextGeographicPoint();
            case GEOGRAPHIC_POINT_3D:
                return nextGeographic3DPoint();
            case BOOLEAN_ARRAY:
                return nextBooleanArray();
            case BYTE_ARRAY:
                return nextByteArray();
            case SHORT_ARRAY:
                return nextShortArray();
            case INT_ARRAY:
                return nextIntArray();
            case LONG_ARRAY:
                return nextLongArray();
            case FLOAT_ARRAY:
                return nextFloatArray();
            case DOUBLE_ARRAY:
                return nextDoubleArray();
            case CHAR_ARRAY:
                return nextCharArray();
            case STRING_ARRAY:
                return nextTextArray();
            case STRING_ALPHANUMERIC_ARRAY:
                return nextAlphaNumericTextArray();
            case STRING_ASCII_ARRAY:
                return nextAsciiTextArray();
            case STRING_BMP_ARRAY:
                return nextBasicMultilingualPlaneTextArray();
            case LOCAL_DATE_TIME_ARRAY:
                return nextLocalDateTimeArray();
            case DATE_ARRAY:
                return nextDateArray();
            case LOCAL_TIME_ARRAY:
                return nextLocalTimeArray();
            case PERIOD_ARRAY:
                return nextPeriodArray();
            case DURATION_ARRAY:
                return nextDurationArray();
            case TIME_ARRAY:
                return nextTimeArray();
            case DATE_TIME_ARRAY:
                return nextDateTimeArray();
            case CARTESIAN_POINT_ARRAY:
                return nextCartesianPointArray();
            case CARTESIAN_POINT_3D_ARRAY:
                return nextCartesian3DPointArray();
            case GEOGRAPHIC_POINT_ARRAY:
                return nextGeographicPointArray();
            case GEOGRAPHIC_POINT_3D_ARRAY:
                return nextGeographic3DPointArray();
            default:
                throw new IllegalArgumentException("Unknown value type: " + type);
        }
    }

    /**
     * Returns the next {@link ArrayValue}, distributed uniformly among all array types.
     *
     * @see RandomValues
     */
    public ArrayValue nextArray() {
        return (ArrayValue) nextValueOfType(among(ARRAY_TYPES));
    }

    /**
     * @see RandomValues
     */
    public BooleanValue nextBooleanValue() {
        return Values.booleanValue(generator.nextBoolean());
    }

    /**
     * @see RandomValues
     */
    public boolean nextBoolean() {
        return generator.nextBoolean();
    }

    /**
     * @see RandomValues
     */
    public ByteValue nextByteValue() {
        return byteValue((byte) generator.nextInt());
    }

    /**
     * Returns the next {@link ByteValue} between 0 (inclusive) and the specified value (exclusive)
     *
     * @param bound the upper bound (exclusive).  Must be positive.
     * @return {@link ByteValue}
     */
    public ByteValue nextByteValue(byte bound) {
        return byteValue((byte) generator.nextInt(bound));
    }

    /**
     * @see RandomValues
     */
    public ShortValue nextShortValue() {
        return shortValue((short) generator.nextInt());
    }

    /**
     * Returns the next {@link ShortValue} between 0 (inclusive) and the specified value (exclusive)
     *
     * @param bound the upper bound (exclusive).  Must be positive.
     * @return {@link ShortValue}
     */
    public ShortValue nextShortValue(short bound) {
        return shortValue((short) generator.nextInt(bound));
    }

    /**
     * @see RandomValues
     */
    public IntValue nextIntValue() {
        return intValue(generator.nextInt());
    }

    /**
     * @see RandomValues
     */
    public int nextInt() {
        return generator.nextInt();
    }

    /**
     * Returns the next {@link IntValue} between 0 (inclusive) and the specified value (exclusive)
     *
     * @param bound the upper bound (exclusive).  Must be positive.
     * @return {@link IntValue}
     * @see RandomValues
     */
    public IntValue nextIntValue(int bound) {
        return intValue(generator.nextInt(bound));
    }

    /**
     * Returns the next {@code int} between 0 (inclusive) and the specified value (exclusive)
     *
     * @param bound the upper bound (exclusive).  Must be positive.
     * @return {@code int}
     * @see RandomValues
     */
    public int nextInt(int bound) {
        return generator.nextInt(bound);
    }

    /**
     * Returns an {@code int} between the given lower bound (inclusive) and the upper bound (inclusive)
     *
     * @param min minimum value that can be chosen (inclusive)
     * @param max maximum value that can be chosen (inclusive)
     * @return an {@code int} in the given inclusive range.
     * @see RandomValues
     */
    public int intBetween(int min, int max) {
        return min + generator.nextInt(max - min + 1);
    }

    /**
     * @see RandomValues
     */
    public long nextLong() {
        return generator.nextLong();
    }

    /**
     * Returns the next {@code long} between 0 (inclusive) and the specified value (exclusive)
     *
     * @param bound the upper bound (exclusive).  Must be positive.
     * @return {@code long}
     * @see RandomValues
     */
    public long nextLong(long bound) {
        return abs(generator.nextLong()) % bound;
    }

    /**
     * Returns a {@code long} between the given lower bound (inclusive) and the upper bound (inclusive)
     *
     * @param min minimum value that can be chosen (inclusive)
     * @param max maximum value that can be chosen (inclusive)
     * @return a {@code long} in the given inclusive range.
     * @see RandomValues
     */
    private long longBetween(long min, long max) {
        return nextLong((max - min) + 1L) + min;
    }

    /**
     * @see RandomValues
     */
    public LongValue nextLongValue() {
        return longValue(generator.nextLong());
    }

    /**
     * Returns the next {@link LongValue} between 0 (inclusive) and the specified value (exclusive)
     *
     * @param bound the upper bound (exclusive).  Must be positive.
     * @return {@link LongValue}
     * @see RandomValues
     */
    public LongValue nextLongValue(long bound) {
        return longValue(nextLong(bound));
    }

    /**
     * Returns the next {@link LongValue} between the specified lower bound (inclusive) and the specified upper bound (inclusive)
     *
     * @param lower the lower bound (inclusive).
     * @param upper the upper bound (inclusive).
     * @return {@link LongValue}
     * @see RandomValues
     */
    public LongValue nextLongValue(long lower, long upper) {
        return longValue(nextLong((upper - lower) + 1L) + lower);
    }

    /**
     * Returns the next {@link FloatValue} between 0 (inclusive) and 1.0 (exclusive)
     *
     * @return {@link FloatValue}
     * @see RandomValues
     */
    public FloatValue nextFloatValue() {
        return floatValue(generator.nextFloat());
    }

    /**
     * Returns the next {@code float} between 0 (inclusive) and 1.0 (exclusive)
     *
     * @return {@code float}
     * @see RandomValues
     */
    public float nextFloat() {
        return generator.nextFloat();
    }

    /**
     * @see RandomValues
     */
    public DoubleValue nextDoubleValue() {
        return doubleValue(nextDouble());
    }

    /**
     * Returns the next {@code double} between 0 (inclusive) and 1.0 (exclusive)
     *
     * @return {@code float}
     * @see RandomValues
     */
    public double nextDouble() {
        return generator.nextDouble();
    }

    private double doubleBetween(double min, double max) {
        return nextDouble() * (max - min) + min;
    }

    /**
     * @see RandomValues
     */
    public NumberValue nextNumberValue() {
        int type = generator.nextInt(6);
        switch (type) {
            case 0:
                return nextByteValue();
            case 1:
                return nextShortValue();
            case 2:
                return nextIntValue();
            case 3:
                return nextLongValue();
            case 4:
                return nextFloatValue();
            case 5:
                return nextDoubleValue();
            default:
                throw new IllegalArgumentException("Unknown value type " + type);
        }
    }

    public CharValue nextCharValue() {
        return Values.charValue(nextCharRaw());
    }

    public char nextCharRaw() {
        int codePoint = bmpCodePoint();
        assert (codePoint & ~0xFFFF) == 0;
        return (char) codePoint;
    }

    /**
     * @return a {@link TextValue} consisting only of ascii alphabetic and numerical characters.
     * @see RandomValues
     */
    public TextValue nextAlphaNumericTextValue() {
        return nextAlphaNumericTextValue(minString(), maxString());
    }

    /**
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return a {@link TextValue} consisting only of ascii alphabetic and numerical characters.
     * @see RandomValues
     */
    public TextValue nextAlphaNumericTextValue(int minLength, int maxLength) {
        return nextTextValue(minLength, maxLength, this::alphaNumericCodePoint);
    }

    /**
     * @return a {@link TextValue} consisting only of ascii characters.
     * @see RandomValues
     */
    public TextValue nextAsciiTextValue() {
        return nextAsciiTextValue(minString(), maxString());
    }

    /**
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return a {@link TextValue} consisting only of ascii characters.
     * @see RandomValues
     */
    public TextValue nextAsciiTextValue(int minLength, int maxLength) {
        return nextTextValue(minLength, maxLength, this::asciiCodePoint);
    }

    /**
     * @return a {@link TextValue} consisting only of characters in the Basic Multilingual Plane(BMP).
     * @see RandomValues
     */
    public TextValue nextBasicMultilingualPlaneTextValue() {
        return nextTextValue(minString(), maxString(), this::bmpCodePoint);
    }

    /**
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return a {@link TextValue} consisting only of characters in the Basic Multilingual Plane(BMP).
     * @see RandomValues
     */
    public TextValue nextBasicMultilingualPlaneTextValue(int minLength, int maxLength) {
        return nextTextValue(minLength, maxLength, this::bmpCodePoint);
    }

    /**
     * @see RandomValues
     */
    public TextValue nextTextValue() {
        return nextTextValue(minString(), maxString());
    }

    /**
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return {@link TextValue}.
     * @see RandomValues
     */
    public TextValue nextTextValue(int minLength, int maxLength) {
        return nextTextValue(minLength, maxLength, this::nextValidCodePoint);
    }

    private TextValue nextTextValue(int minLength, int maxLength, CodePointFactory codePointFactory) {
        // todo should we generate UTF8StringValue or StringValue? Or maybe both? Randomly?
        //  If we change this to generate other string values (like UTF16 strings for example)
        //  we need to also update the ValueType -> ValueRepresentation mapping in ValueType.
        int length = intBetween(minLength, maxLength);
        UTF8StringValueBuilder builder = new UTF8StringValueBuilder(length > 0 ? ceilingPowerOfTwo(length) : 0);

        for (int i = 0; i < length; i++) {
            builder.addCodePoint(codePointFactory.generate());
        }
        return builder.build();
    }

    /**
     * Generate next code point that is valid for composition of a string.
     * Additional limitation on code point range is given by configuration.
     *
     * @return A pseudorandom valid code point
     */
    public int nextValidCodePoint() {
        return nextValidCodePoint(configuration.maxCodePoint());
    }

    /**
     * Generate next code point that is valid for composition of a string.
     * Additional limitation on code point range is given by method argument.
     *
     * @param maxCodePoint the maximum code point to consider
     * @return A pseudorandom valid code point
     */
    private int nextValidCodePoint(int maxCodePoint) {
        int codePoint;
        int type;
        do {
            codePoint = intBetween(configuration.minCodePoint(), maxCodePoint);
            type = Character.getType(codePoint);
        } while (type == Character.UNASSIGNED || type == Character.PRIVATE_USE || type == Character.SURROGATE);
        return codePoint;
    }

    /**
     * @return next code point limited to the ascii characters.
     */
    private int asciiCodePoint() {
        return nextValidCodePoint(MAX_ASCII_CODE_POINT);
    }

    /**
     * @return next code point limited to the alpha numeric characters.
     */
    private int alphaNumericCodePoint() {
        int nextInt = generator.nextInt(4);
        if (nextInt == 0) {
            return intBetween('A', 'Z');
        } else if (nextInt == 1) {
            return intBetween('a', 'z');
        } else {
            // We want digits being roughly as frequent as letters
            return intBetween('0', '9');
        }
    }

    /**
     * @return next code point limited to the Basic Multilingual Plane (BMP).
     */
    private int bmpCodePoint() {
        return nextValidCodePoint(MAX_BMP_CODE_POINT);
    }

    /**
     * @see RandomValues
     */
    public TimeValue nextTimeValue() {
        return time(nextTimeRaw());
    }

    /**
     * @see RandomValues
     */
    public LocalDateTimeValue nextLocalDateTimeValue() {
        return localDateTime(nextLocalDateTimeRaw());
    }

    /**
     * @see RandomValues
     */
    public DateValue nextDateValue() {
        return date(nextDateRaw());
    }

    /**
     * @see RandomValues
     */
    public LocalTimeValue nextLocalTimeValue() {
        return localTime(nextLocalTimeRaw());
    }

    /**
     * @see RandomValues
     */
    public DateTimeValue nextDateTimeValue() {
        return nextDateTimeValue(UTC);
    }

    /**
     * @see RandomValues
     */
    public DateTimeValue nextDateTimeValue(ZoneId zoneId) {
        return datetime(nextZonedDateTimeRaw(zoneId));
    }

    /**
     * @return next {@link DurationValue} based on java {@link Period} (years, months and days).
     * @see RandomValues
     */
    public DurationValue nextPeriod() {
        return duration(nextPeriodRaw());
    }

    /**
     * @return next {@link DurationValue} based on java {@link Duration} (seconds, nanos).
     * @see RandomValues
     */
    public DurationValue nextDuration() {
        return duration(nextDurationRaw());
    }

    /**
     * Returns a randomly selected temporal value spread uniformly over the supported types.
     *
     * @return a randomly selected temporal value
     */
    public Value nextTemporalValue() {
        int nextInt = generator.nextInt(6);
        switch (nextInt) {
            case 0:
                return nextDateValue();

            case 1:
                return nextLocalDateTimeValue();

            case 2:
                return nextDateTimeValue();

            case 3:
                return nextLocalTimeValue();

            case 4:
                return nextTimeValue();

            case 5:
                return nextDuration();

            default:
                throw new IllegalArgumentException(nextInt + " not a valid temporal type");
        }
    }

    /**
     * @return the next pseudorandom two-dimensional cartesian {@link PointValue}.
     * @see RandomValues
     */
    public PointValue nextCartesianPoint() {
        double x = randomCartesianCoordinate();
        double y = randomCartesianCoordinate();
        return Values.pointValue(CoordinateReferenceSystem.CARTESIAN, x, y);
    }

    /**
     * @return the next pseudorandom three-dimensional cartesian {@link PointValue}.
     * @see RandomValues
     */
    public PointValue nextCartesian3DPoint() {
        double x = randomCartesianCoordinate();
        double y = randomCartesianCoordinate();
        double z = randomCartesianCoordinate();
        return Values.pointValue(CoordinateReferenceSystem.CARTESIAN_3D, x, y, z);
    }

    /**
     * @return the next pseudorandom two-dimensional geographic {@link PointValue}.
     * @see RandomValues
     */
    public PointValue nextGeographicPoint() {
        double longitude = randomLongitude();
        double latitude = randomLatitude();
        return Values.pointValue(CoordinateReferenceSystem.WGS_84, longitude, latitude);
    }

    /**
     * @return the next pseudorandom three-dimensional geographic {@link PointValue}.
     * @see RandomValues
     */
    public PointValue nextGeographic3DPoint() {
        double longitude = randomLongitude();
        double latitude = randomLatitude();
        double z = randomCartesianCoordinate();
        return Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, longitude, latitude, z);
    }

    private double randomLatitude() {
        double spatialDefaultMinLatitude = -90;
        double spatialDefaultMaxLatitude = 90;
        return doubleBetween(spatialDefaultMinLatitude, spatialDefaultMaxLatitude);
    }

    private double randomLongitude() {
        double spatialDefaultMinLongitude = -180;
        double spatialDefaultMaxLongitude = 180;
        return doubleBetween(spatialDefaultMinLongitude, spatialDefaultMaxLongitude);
    }

    private double randomCartesianCoordinate() {
        double spatialDefaultMinExtent = -1000000;
        double spatialDefaultMaxExtent = 1000000;
        return doubleBetween(spatialDefaultMinExtent, spatialDefaultMaxExtent);
    }

    /**
     * Returns a randomly selected point value spread uniformly over the supported types of points.
     *
     * @return a randomly selected point value
     */
    public PointValue nextPointValue() {
        int nextInt = generator.nextInt(4);
        switch (nextInt) {
            case 0:
                return nextCartesianPoint();

            case 1:
                return nextCartesian3DPoint();

            case 2:
                return nextGeographicPoint();

            case 3:
                return nextGeographic3DPoint();

            default:
                throw new IllegalStateException(nextInt + " not a valid point type");
        }
    }

    public CharArray nextCharArray() {
        return Values.charArray(nextCharArrayRaw(minArray(), maxArray()));
    }

    private char[] nextCharArrayRaw(int minLength, int maxLength) {
        int length = intBetween(minLength, maxLength);
        char[] array = new char[length];
        for (int i = 0; i < length; i++) {
            array[i] = nextCharRaw();
        }
        return array;
    }

    /**
     * @see RandomValues
     */
    public DoubleArray nextDoubleArray() {
        double[] array = nextDoubleArrayRaw(minArray(), maxArray());
        return Values.doubleArray(array);
    }

    /**
     * @see RandomValues
     */
    public double[] nextDoubleArrayRaw(int minLength, int maxLength) {
        int length = intBetween(minLength, maxLength);
        double[] doubles = new double[length];
        for (int i = 0; i < length; i++) {
            doubles[i] = nextDouble();
        }
        return doubles;
    }

    /**
     * @see RandomValues
     */
    public FloatArray nextFloatArray() {
        float[] array = nextFloatArrayRaw(minArray(), maxArray());
        return Values.floatArray(array);
    }

    /**
     * @see RandomValues
     */
    public float[] nextFloatArrayRaw(int minLength, int maxLength) {
        int length = intBetween(minLength, maxLength);
        float[] floats = new float[length];
        for (int i = 0; i < length; i++) {
            floats[i] = generator.nextFloat();
        }
        return floats;
    }

    /**
     * @see RandomValues
     */
    public LongArray nextLongArray() {
        long[] array = nextLongArrayRaw(minArray(), maxArray());
        return Values.longArray(array);
    }

    /**
     * @see RandomValues
     */
    public long[] nextLongArrayRaw(int minLength, int maxLength) {
        int length = intBetween(minLength, maxLength);
        long[] longs = new long[length];
        for (int i = 0; i < length; i++) {
            longs[i] = generator.nextLong();
        }
        return longs;
    }

    /**
     * @see RandomValues
     */
    public IntArray nextIntArray() {
        int[] array = nextIntArrayRaw(minArray(), maxArray());
        return Values.intArray(array);
    }

    /**
     * @see RandomValues
     */
    public int[] nextIntArrayRaw(int minLength, int maxLength) {
        int length = intBetween(minLength, maxLength);
        int[] ints = new int[length];
        for (int i = 0; i < length; i++) {
            ints[i] = generator.nextInt();
        }
        return ints;
    }

    /**
     * @see RandomValues
     */
    public ByteArray nextByteArray() {
        return nextByteArray(minArray(), maxArray());
    }

    /**
     * @see RandomValues
     */
    public ByteArray nextByteArray(int minLength, int maxLength) {
        byte[] array = nextByteArrayRaw(minLength, maxLength);
        return Values.byteArray(array);
    }

    /**
     * @see RandomValues
     */
    public byte[] nextByteArrayRaw(int minLength, int maxLength) {
        int length = intBetween(minLength, maxLength);
        byte[] bytes = new byte[length];
        int index = 0;
        while (index < length) {
            // For each random int we get up to four random bytes
            int rand = nextInt();
            int numBytesToShift = Math.min(length - index, Integer.BYTES);

            // byte 4   byte 3   byte 2   byte 1
            // aaaaaaaa bbbbbbbb cccccccc dddddddd
            while (numBytesToShift > 0) {
                bytes[index++] = (byte) rand;
                numBytesToShift--;
                rand >>= Byte.SIZE;
            }
        }
        return bytes;
    }

    /**
     * @see RandomValues
     */
    public ShortArray nextShortArray() {
        short[] array = nextShortArrayRaw(minArray(), maxArray());
        return Values.shortArray(array);
    }

    /**
     * @see RandomValues
     */
    public short[] nextShortArrayRaw(int minLength, int maxLength) {
        int length = intBetween(minLength, maxLength);
        short[] shorts = new short[length];
        for (int i = 0; i < length; i++) {
            shorts[i] = (short) generator.nextInt();
        }
        return shorts;
    }

    /**
     * @see RandomValues
     */
    public BooleanArray nextBooleanArray() {
        boolean[] array = nextBooleanArrayRaw(minArray(), maxArray());
        return Values.booleanArray(array);
    }

    /**
     * @see RandomValues
     */
    public boolean[] nextBooleanArrayRaw(int minLength, int maxLength) {
        int length = intBetween(minLength, maxLength);
        boolean[] booleans = new boolean[length];
        for (int i = 0; i < length; i++) {
            booleans[i] = generator.nextBoolean();
        }
        return booleans;
    }

    /**
     * @return the next {@link TextArray} containing strings with only alpha-numeric characters.
     * @see RandomValues
     */
    public TextArray nextAlphaNumericTextArray() {
        String[] array = nextAlphaNumericStringArrayRaw(minArray(), maxArray(), minString(), maxString());
        return Values.stringArray(array);
    }

    /**
     * @return the next {@code String[]} containing strings with only alpha-numeric characters.
     * @see RandomValues
     */
    public String[] nextAlphaNumericStringArrayRaw(
            int minLength, int maxLength, int minStringLength, int maxStringLength) {
        return nextArray(
                String[]::new,
                () -> nextStringRaw(minStringLength, maxStringLength, this::alphaNumericCodePoint),
                minLength,
                maxLength);
    }

    /**
     * @return the next {@link TextArray} containing strings with only ascii characters.
     * @see RandomValues
     */
    private TextArray nextAsciiTextArray() {
        String[] array = nextArray(String[]::new, () -> nextStringRaw(this::asciiCodePoint), minArray(), maxArray());
        return Values.stringArray(array);
    }

    /**
     * @return the next {@link TextArray} containing strings with only characters in the Basic Multilingual Plane (BMP).
     * @see RandomValues
     */
    public TextArray nextBasicMultilingualPlaneTextArray() {
        String[] array = nextArray(
                String[]::new,
                () -> nextStringRaw(minString(), maxString(), this::bmpCodePoint),
                minArray(),
                maxArray());
        return Values.stringArray(array);
    }

    /**
     * @see RandomValues
     */
    public TextArray nextTextArray() {
        String[] array = nextStringArrayRaw(minArray(), maxArray(), minString(), maxString());
        return Values.stringArray(array);
    }

    /**
     * @see RandomValues
     */
    public String[] nextStringArrayRaw(int minLength, int maxLength, int minStringLength, int maxStringLength) {
        return nextArray(
                String[]::new,
                () -> nextStringRaw(minStringLength, maxStringLength, this::nextValidCodePoint),
                minLength,
                maxLength);
    }

    /**
     * @see RandomValues
     */
    public LocalTimeArray nextLocalTimeArray() {
        LocalTime[] array = nextLocalTimeArrayRaw(minArray(), maxArray());
        return Values.localTimeArray(array);
    }

    /**
     * @see RandomValues
     */
    public LocalTime[] nextLocalTimeArrayRaw(int minLength, int maxLength) {
        return nextArray(LocalTime[]::new, this::nextLocalTimeRaw, minLength, maxLength);
    }

    /**
     * @see RandomValues
     */
    public TimeArray nextTimeArray() {
        OffsetTime[] array = nextTimeArrayRaw(minArray(), maxArray());
        return Values.timeArray(array);
    }

    /**
     * @see RandomValues
     */
    public OffsetTime[] nextTimeArrayRaw(int minLength, int maxLength) {
        return nextArray(OffsetTime[]::new, this::nextTimeRaw, minLength, maxLength);
    }

    /**
     * @see RandomValues
     */
    public DateTimeArray nextDateTimeArray() {
        ZonedDateTime[] array = nextDateTimeArrayRaw(minArray(), maxArray());
        return Values.dateTimeArray(array);
    }

    /**
     * @see RandomValues
     */
    public ZonedDateTime[] nextDateTimeArrayRaw(int minLength, int maxLength) {
        return nextArray(ZonedDateTime[]::new, () -> nextZonedDateTimeRaw(UTC), minLength, maxLength);
    }

    /**
     * @see RandomValues
     */
    public LocalDateTimeArray nextLocalDateTimeArray() {
        return Values.localDateTimeArray(nextLocalDateTimeArrayRaw(minArray(), maxArray()));
    }

    /**
     * @see RandomValues
     */
    public LocalDateTime[] nextLocalDateTimeArrayRaw(int minLength, int maxLength) {
        return nextArray(LocalDateTime[]::new, this::nextLocalDateTimeRaw, minLength, maxLength);
    }

    /**
     * @see RandomValues
     */
    public DateArray nextDateArray() {
        return Values.dateArray(nextDateArrayRaw(minArray(), maxArray()));
    }

    /**
     * @see RandomValues
     */
    public LocalDate[] nextDateArrayRaw(int minLength, int maxLength) {
        return nextArray(LocalDate[]::new, this::nextDateRaw, minLength, maxLength);
    }

    /**
     * @return next {@link DurationArray} based on java {@link Period} (years, months and days).
     * @see RandomValues
     */
    private DurationArray nextPeriodArray() {
        return Values.durationArray(nextPeriodArrayRaw(minArray(), maxArray()));
    }

    /**
     * @see RandomValues
     */
    public Period[] nextPeriodArrayRaw(int minLength, int maxLength) {
        return nextArray(Period[]::new, this::nextPeriodRaw, minLength, maxLength);
    }

    /**
     * @return next {@link DurationValue} based on java {@link Duration} (seconds, nanos).
     * @see RandomValues
     */
    public DurationArray nextDurationArray() {
        return Values.durationArray(nextDurationArrayRaw(minArray(), maxArray()));
    }

    /**
     * @see RandomValues
     */
    public Duration[] nextDurationArrayRaw(int minLength, int maxLength) {
        return nextArray(Duration[]::new, this::nextDurationRaw, minLength, maxLength);
    }

    /**
     * @return the next random {@link PointArray}.
     * @see RandomValues
     */
    public PointArray nextPointArray() {
        int nextInt = generator.nextInt(4);
        switch (nextInt) {
            case 0:
                return nextCartesianPointArray();

            case 1:
                return nextCartesian3DPointArray();

            case 2:
                return nextGeographicPointArray();

            case 3:
                return nextGeographic3DPointArray();

            default:
                throw new IllegalStateException(nextInt + " not a valid point type");
        }
    }

    /**
     * @return the next {@link PointArray} containing two-dimensional cartesian {@link PointValue}.
     * @see RandomValues
     */
    public PointArray nextCartesianPointArray() {
        return nextCartesianPointArray(minArray(), maxArray());
    }

    /**
     * @return the next {@link PointArray} containing two-dimensional cartesian {@link PointValue}.
     * @see RandomValues
     */
    public PointArray nextCartesianPointArray(int minLength, int maxLength) {
        PointValue[] array = nextArray(PointValue[]::new, this::nextCartesianPoint, minLength, maxLength);
        return Values.pointArray(array);
    }

    /**
     * @return the next {@link PointArray} containing three-dimensional cartesian {@link PointValue}.
     * @see RandomValues
     */
    public PointArray nextCartesian3DPointArray() {
        return nextCartesian3DPointArray(minArray(), maxArray());
    }

    /**
     * @return the next {@link PointArray} containing three-dimensional cartesian {@link PointValue}.
     * @see RandomValues
     */
    public PointArray nextCartesian3DPointArray(int minLength, int maxLength) {
        PointValue[] array = nextArray(PointValue[]::new, this::nextCartesian3DPoint, minLength, maxLength);
        return Values.pointArray(array);
    }

    /**
     * @return the next {@link PointArray} containing two-dimensional geographic {@link PointValue}.
     * @see RandomValues
     */
    public PointArray nextGeographicPointArray() {
        return nextGeographicPointArray(minArray(), maxArray());
    }

    /**
     * @return the next {@link PointArray} containing two-dimensional geographic {@link PointValue}.
     * @see RandomValues
     */
    public PointArray nextGeographicPointArray(int minLength, int maxLength) {
        PointValue[] array = nextArray(PointValue[]::new, this::nextGeographicPoint, minLength, maxLength);
        return Values.pointArray(array);
    }

    /**
     * @return the next {@link PointArray} containing three-dimensional geographic {@link PointValue}.
     * @see RandomValues
     */
    public PointArray nextGeographic3DPointArray() {
        return nextGeographic3DPointArray(minArray(), maxArray());
    }

    /**
     * @return the next {@link PointArray} containing three-dimensional geographic {@link PointValue}.
     * @see RandomValues
     */
    public PointArray nextGeographic3DPointArray(int minLength, int maxLength) {
        PointValue[] points = nextArray(PointValue[]::new, this::nextGeographic3DPoint, minLength, maxLength);
        return Values.pointArray(points);
    }

    /**
     * Create an randomly sized array filled with elements provided by factory.
     *
     * @param arrayFactory creates array with length equal to provided argument.
     * @param elementFactory generating random values of some type.
     * @param minLength minimum length of array (inclusive).
     * @param maxLength maximum length of array (inclusive).
     * @param <T> Generic type of elements in array.
     * @return a new array created by arrayFactory, filled with elements created by elementFactory.
     */
    private <T> T[] nextArray(
            IntFunction<T[]> arrayFactory, ElementFactory<T> elementFactory, int minLength, int maxLength) {
        int length = intBetween(minLength, maxLength);
        T[] array = arrayFactory.apply(length);
        for (int i = 0; i < length; i++) {
            array[i] = elementFactory.generate();
        }
        return array;
    }

    /* Single raw element */

    private String nextStringRaw(CodePointFactory codePointFactory) {
        return nextStringRaw(minString(), maxString(), codePointFactory);
    }

    private String nextStringRaw(int minStringLength, int maxStringLength, CodePointFactory codePointFactory) {
        int length = intBetween(minStringLength, maxStringLength);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.appendCodePoint(codePointFactory.generate());
        }
        return sb.toString();
    }

    private LocalTime nextLocalTimeRaw() {
        return ofNanoOfDay(longBetween(LocalTime.MIN.toNanoOfDay(), LocalTime.MAX.toNanoOfDay()));
    }

    private LocalDateTime nextLocalDateTimeRaw() {
        return LocalDateTime.ofInstant(nextInstantRaw(), UTC);
    }

    private OffsetTime nextTimeRaw() {
        return OffsetTime.ofInstant(nextInstantRaw(), UTC);
    }

    private ZonedDateTime nextZonedDateTimeRaw(ZoneId utc) {
        return ZonedDateTime.ofInstant(nextInstantRaw(), utc);
    }

    private LocalDate nextDateRaw() {
        return ofEpochDay(longBetween(LocalDate.MIN.toEpochDay(), LocalDate.MAX.toEpochDay()));
    }

    private Instant nextInstantRaw() {
        return Instant.ofEpochSecond(
                longBetween(LocalDateTime.MIN.toEpochSecond(UTC), LocalDateTime.MAX.toEpochSecond(UTC)),
                nextLong(NANOS_PER_SECOND));
    }

    private Period nextPeriodRaw() {
        return Period.of(generator.nextInt(), generator.nextInt(12), generator.nextInt(28));
    }

    private Duration nextDurationRaw() {
        return Duration.ofSeconds(nextLong(DAYS.getDuration().getSeconds()), nextLong(NANOS_PER_SECOND));
    }

    /**
     * Returns a random element from the provided array.
     *
     * @param among the array to choose a random element from.
     * @return a random element of the provided array.
     */
    public <T> T among(T[] among) {
        return among[generator.nextInt(among.length)];
    }

    /**
     * Returns a random element from the provided array.
     *
     * @param among the array to choose a random element from.
     * @return a random element of the provided array.
     */
    public long among(long[] among) {
        return among[generator.nextInt(among.length)];
    }

    /**
     * Returns a random element from the provided array.
     *
     * @param among the array to choose a random element from.
     * @return a random element of the provided array.
     */
    public int among(int[] among) {
        return among[generator.nextInt(among.length)];
    }

    /**
     * Returns a random element of the provided list
     *
     * @param among the list to choose a random element from
     * @return a random element of the provided list
     */
    public <T> T among(List<T> among) {
        return among.get(generator.nextInt(among.size()));
    }

    /**
     * Picks a random element of the provided list and feeds it to the provided {@link Consumer}
     *
     * @param among the list to pick from
     * @param action the consumer to feed values to
     */
    public <T> void among(List<T> among, Consumer<T> action) {
        if (!among.isEmpty()) {
            T item = among(among);
            action.accept(item);
        }
    }

    public long among(LongList among) {
        return among.get(nextInt(among.size()));
    }

    public <T> T among(RichIterable<T> among) {
        int offset = nextInt(among.size());
        final var iterator = among.iterator();
        while (offset-- > 0) {
            iterator.next();
        }
        return iterator.next();
    }

    /**
     * Returns a random selection of the provided array.
     *
     * @param among the array to pick elements from
     * @param min the minimum number of elements to choose
     * @param max the maximum number of elements to choose
     * @param allowDuplicates if {@code true} the same element can be chosen multiple times
     * @return a random selection of the provided array.
     */
    @SuppressWarnings("unchecked")
    public <T> T[] selection(T[] among, int min, int max, boolean allowDuplicates) {
        assert min <= max;
        int diff = min == max ? 0 : generator.nextInt(max - min);
        int length = min + diff;
        assert allowDuplicates || length <= among.length
                : "Unique selection of " + length + " items cannot possibly be created from " + among.length + " items";
        T[] result = (T[]) Array.newInstance(among.getClass().getComponentType(), length);
        for (int i = 0; i < length; i++) {
            while (true) {
                T candidate = among(among);
                if (!allowDuplicates && ArrayUtils.contains(result, candidate)) { // Try again
                    continue;
                }
                result[i] = candidate;
                break;
            }
        }
        return result;
    }

    /**
     * Returns a random selection of the provided int array.
     *
     * @param among the array to pick elements from
     * @param min the minimum number of elements to choose
     * @param max the maximum number of elements to choose
     * @param allowDuplicates if {@code true} the same element can be chosen multiple times
     * @return a random selection of the provided int array.
     */
    public int[] selection(int[] among, int min, int max, boolean allowDuplicates) {
        return Arrays.stream(selection(IntStream.of(among).boxed().toArray(Integer[]::new), min, max, allowDuplicates))
                .mapToInt(v -> v)
                .toArray();
    }

    /**
     * Returns a random selection of the provided long array.
     *
     * @param among the array to pick elements from
     * @param min the minimum number of elements to choose
     * @param max the maximum number of elements to choose
     * @param allowDuplicates if {@code true} the same element can be chosen multiple times
     * @return a random selection of the provided long array.
     */
    public long[] selection(long[] among, int min, int max, boolean allowDuplicates) {
        return Arrays.stream(selection(LongStream.of(among).boxed().toArray(Long[]::new), min, max, allowDuplicates))
                .mapToLong(v -> v)
                .toArray();
    }

    private int maxArray() {
        return configuration.arrayMaxLength();
    }

    private int minArray() {
        return configuration.arrayMinLength();
    }

    private int maxString() {
        return configuration.stringMaxLength();
    }

    private int minString() {
        return configuration.stringMinLength();
    }

    @FunctionalInterface
    private interface ElementFactory<T> {
        T generate();
    }

    @FunctionalInterface
    private interface CodePointFactory {
        int generate();
    }
}
