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
import static org.neo4j.values.storable.ValueType.ALL_TYPES;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortValue;
import static org.neo4j.values.storable.VectorValue.MAX_VECTOR_DIMENSIONS;
import static org.neo4j.values.storable.VectorValue.MIN_VECTOR_DIMENSIONS;
import static org.neo4j.values.utils.TemporalUtil.NANOS_PER_SECOND;

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
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.block.predicate.Predicate;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.factory.SortedSets;
import org.eclipse.collections.api.factory.primitive.IntLists;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.primitive.ImmutableIntList;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.set.SetIterable;
import org.eclipse.collections.api.set.sorted.ImmutableSortedSet;
import org.eclipse.collections.impl.list.fixed.ArrayAdapter;
import org.eclipse.collections.impl.utility.Iterate;
import org.eclipse.collections.impl.utility.LazyIterate;

/**
 * Helper class that generates generator values of all supported types.
 * <p>
 * Generated values are always uniformly distributed in pseudorandom fashion.
 * <p>
 * Can generate both {@link Value} and "raw" instances. The "raw" type of a value type means
 * the corresponding Core API type if such type exists. For example, {@code String[]} is the raw type of {@link TextArray}.
 * <p>
 * The length of strings will be governed by {@link Configuration#stringMinLength()} and
 * {@link Configuration#stringMaxLength()} and
 * the length of arrays will be governed by {@link Configuration#arrayMinLength()} and
 * {@link Configuration#arrayMaxLength()}
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

        boolean includeVectorTypes();

        int maxVectorNumBytes();

        int minVectorDimensions();

        int maxVectorDimensions();

        IntList vectorDimensionChoices();

        SetIterable<ValueType> allowedTypes();
    }

    public static ConfigurationBuilder newConfigurationBuilder() {
        return new ConfigurationBuilder();
    }

    public static final Configuration DEFAULT_CONFIGURATION =
            newConfigurationBuilder().build();

    // see maxSizeInKey, and assume one property for this helper constant
    public static final int MAX_NUM_BYTES_IN_INDEX_KEY = maxSizeInIndexKey(1);

    public static final int MAX_BMP_CODE_POINT = 0xFFFF;
    static final int MAX_ASCII_CODE_POINT = 0x7F;

    private final Generator generator;
    private final Configuration configuration;

    private RandomValues(Generator generator) {
        this(generator, DEFAULT_CONFIGURATION);
    }

    private RandomValues(Generator generator, Configuration configuration) {
        this.generator = generator;
        this.configuration = configuration;
    }

    public Configuration configuration() {
        return configuration;
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
        return nextValueOfType(among(configuration.allowedTypes()));
    }

    /**
     * Returns the next {@link Value}, distributed uniformly among the provided value types.
     *
     * @see RandomValues
     */
    public Value nextValueOfTypes(ValueType... types) {
        assert types.length > 0 : "No value types provided";
        final ListIterable<ValueType> allowedTypes = ArrayAdapter.adapt(types).select(this::allowedType);
        assert !allowedTypes.isEmpty()
                : "No allowed types provided " + Arrays.toString(types) + " " + configuration.allowedTypes();
        return nextValueOfType(among(allowedTypes));
    }

    /**
     * Returns the next size number of {@link Value}, distributed uniformly among the supported value types.
     *
     * @see RandomValues
     */
    public Value[] nextValues(int size) {
        return nextValuesOfTypes(size, configuration.allowedTypes());
    }

    /**
     * Returns the next size number of {@link Value}, distributed uniformly among the provided value types.
     *
     * @see RandomValues
     */
    public Value[] nextValuesOfTypes(int size, ValueType... types) {
        return nextValuesOfTypes(size, Lists.mutable.wrapCopy(types));
    }

    /**
     * Returns the next size number of {@link Value}, distributed uniformly among the provided value types.
     *
     * @see RandomValues
     */
    public Value[] nextValuesOfTypes(int size, RichIterable<ValueType> types) {
        var allowedTypes = types.select(this::allowedType);

        var values = new Value[size];
        for (int i = 0; i < size; i++) {
            values[i] = nextValueOfType(among(allowedTypes));
        }
        return values;
    }

    public static ValueType[] including(Predicate<ValueType> include) {
        return Arrays.stream(ALL_TYPES).filter(include).toArray(ValueType[]::new);
    }

    /**
     * Create an array containing all value types, excluding provided types.
     */
    public static ValueType[] excluding(ValueType... exclude) {
        return excluding(ALL_TYPES, exclude);
    }

    public static ValueType[] excluding(ValueType[] among, ValueType... exclude) {
        return excluding(among, t -> ArrayUtils.contains(exclude, t));
    }

    public static <T> T[] excluding(T[] among, Predicate<T> exclude) {
        return Arrays.stream(among).filter(exclude.negate()).toArray(length ->
                (T[]) Array.newInstance(among.getClass().getComponentType(), length));
    }

    public static ValueType[] typesOfGroups(ValueGroup... valueGroups) {
        return Arrays.stream(ALL_TYPES)
                .filter(t -> ArrayUtils.contains(valueGroups, t.valueGroup))
                .toArray(ValueType[]::new);
    }

    public static ValueType[] typesOfCategories(ValueCategory... valueCategories) {
        return Arrays.stream(ALL_TYPES)
                .filter(t -> ArrayUtils.contains(valueCategories, t.valueGroup.category()))
                .toArray(ValueType[]::new);
    }

    /**
     * Returns the next {@link Value} of provided type.
     *
     * @see RandomValues
     */
    public Value nextValueOfType(ValueType type) {
        if (!allowedType(type)) {
            throw new IllegalStateException("%s is not configured to generate %s values."
                    .formatted(getClass().getSimpleName(), type));
        }

        return switch (type) {
            case BOOLEAN -> nextBooleanValue();
            case BYTE -> nextByteValue();
            case SHORT -> nextShortValue();
            case STRING -> nextTextValue();
            case INT -> nextIntValue();
            case LONG -> nextLongValue();
            case FLOAT -> nextFloatValue();
            case DOUBLE -> nextDoubleValue();
            case CHAR -> nextCharValue();
            case STRING_ALPHANUMERIC -> nextAlphaNumericTextValue();
            case STRING_ASCII -> nextAsciiTextValue();
            case STRING_BMP -> nextBasicMultilingualPlaneTextValue();
            case LOCAL_DATE_TIME -> nextLocalDateTimeValue();
            case DATE -> nextDateValue();
            case LOCAL_TIME -> nextLocalTimeValue();
            case PERIOD -> nextPeriod();
            case DURATION -> nextDuration();
            case TIME -> nextTimeValue();
            case DATE_TIME -> nextDateTimeValue();
            case CARTESIAN_POINT -> nextCartesianPoint();
            case CARTESIAN_POINT_3D -> nextCartesian3DPoint();
            case GEOGRAPHIC_POINT -> nextGeographicPoint();
            case GEOGRAPHIC_POINT_3D -> nextGeographic3DPoint();
            case BOOLEAN_ARRAY -> nextBooleanArray();
            case BYTE_ARRAY -> nextByteArray();
            case SHORT_ARRAY -> nextShortArray();
            case INT_ARRAY -> nextIntArray();
            case LONG_ARRAY -> nextLongArray();
            case FLOAT_ARRAY -> nextFloatArray();
            case DOUBLE_ARRAY -> nextDoubleArray();
            case CHAR_ARRAY -> nextCharArray();
            case STRING_ARRAY -> nextTextArray();
            case STRING_ALPHANUMERIC_ARRAY -> nextAlphaNumericTextArray();
            case STRING_ASCII_ARRAY -> nextAsciiTextArray();
            case STRING_BMP_ARRAY -> nextBasicMultilingualPlaneTextArray();
            case LOCAL_DATE_TIME_ARRAY -> nextLocalDateTimeArray();
            case DATE_ARRAY -> nextDateArray();
            case LOCAL_TIME_ARRAY -> nextLocalTimeArray();
            case PERIOD_ARRAY -> nextPeriodArray();
            case DURATION_ARRAY -> nextDurationArray();
            case TIME_ARRAY -> nextTimeArray();
            case DATE_TIME_ARRAY -> nextDateTimeArray();
            case CARTESIAN_POINT_ARRAY -> nextCartesianPointArray();
            case CARTESIAN_POINT_3D_ARRAY -> nextCartesian3DPointArray();
            case GEOGRAPHIC_POINT_ARRAY -> nextGeographicPointArray();
            case GEOGRAPHIC_POINT_3D_ARRAY -> nextGeographic3DPointArray();
            case INT8_VECTOR -> nextInt8Vector();
            case INT16_VECTOR -> nextInt16Vector();
            case INT32_VECTOR -> nextInt32Vector();
            case INT64_VECTOR -> nextInt64Vector();
            case FLOAT32_VECTOR -> nextFloat32Vector();
            case FLOAT64_VECTOR -> nextFloat64Vector();
            case VECTOR_ARRAY -> nextVectorArray();
            case UUID -> nextUUIDValue();
            case UUID_ARRAY -> nextUUIDArray();
        };
    }

    public Int8Vector nextInt8Vector(int minDim, int maxDim) {
        assert MIN_VECTOR_DIMENSIONS <= minDim && minDim <= maxDim && maxDim <= MAX_VECTOR_DIMENSIONS
                : "Require (%d,%d) in [%d, %d]".formatted(minDim, maxDim, MIN_VECTOR_DIMENSIONS, MAX_VECTOR_DIMENSIONS);
        return Values.int8Vector(nextByteArrayRaw(minDim, maxDim));
    }

    public Int8Vector nextInt8Vector() {
        final int dimension = chooseDimension(Byte.BYTES);
        return nextInt8Vector(dimension, dimension);
    }

    public Int16Vector nextInt16Vector(int minDim, int maxDim) {
        assert MIN_VECTOR_DIMENSIONS <= minDim && minDim <= maxDim && maxDim <= MAX_VECTOR_DIMENSIONS
                : "Require (%d,%d) in [%d, %d]".formatted(minDim, maxDim, MIN_VECTOR_DIMENSIONS, MAX_VECTOR_DIMENSIONS);
        return Values.int16Vector(nextShortArrayRaw(minDim, maxDim));
    }

    public Int16Vector nextInt16Vector() {
        final int dimension = chooseDimension(Short.BYTES);
        return nextInt16Vector(dimension, dimension);
    }

    public Int32Vector nextInt32Vector(int minDim, int maxDim) {
        assert MIN_VECTOR_DIMENSIONS <= minDim && minDim <= maxDim && maxDim <= MAX_VECTOR_DIMENSIONS
                : "Require (%d,%d) in [%d, %d]".formatted(minDim, maxDim, MIN_VECTOR_DIMENSIONS, MAX_VECTOR_DIMENSIONS);
        return Values.int32Vector(nextIntArrayRaw(minDim, maxDim));
    }

    public Int32Vector nextInt32Vector() {
        final int dimension = chooseDimension(Integer.BYTES);
        return nextInt32Vector(dimension, dimension);
    }

    public Int64Vector nextInt64Vector(int minDim, int maxDim) {
        assert MIN_VECTOR_DIMENSIONS <= minDim && minDim <= maxDim && maxDim <= MAX_VECTOR_DIMENSIONS
                : "Require (%d,%d) in [%d, %d]".formatted(minDim, maxDim, MIN_VECTOR_DIMENSIONS, MAX_VECTOR_DIMENSIONS);
        return Values.int64Vector(nextLongArrayRaw(minDim, maxDim));
    }

    public Int64Vector nextInt64Vector() {
        final int dimension = chooseDimension(Long.BYTES);
        return nextInt64Vector(dimension, dimension);
    }

    public Float32Vector nextFloat32Vector(int minDim, int maxDim) {
        assert MIN_VECTOR_DIMENSIONS <= minDim && minDim <= maxDim && maxDim <= MAX_VECTOR_DIMENSIONS
                : "Require (%d,%d) in [%d, %d]".formatted(minDim, maxDim, MIN_VECTOR_DIMENSIONS, MAX_VECTOR_DIMENSIONS);
        return Values.float32Vector(nextFloatArrayRaw(minDim, maxDim));
    }

    public Float32Vector nextFloat32Vector() {
        final int dimension = chooseDimension(Float.BYTES);
        return nextFloat32Vector(dimension, dimension);
    }

    public Float64Vector nextFloat64Vector(int minDim, int maxDim) {
        assert MIN_VECTOR_DIMENSIONS <= minDim && minDim <= maxDim && maxDim <= MAX_VECTOR_DIMENSIONS
                : "Require (%d,%d) in [%d, %d]".formatted(minDim, maxDim, MIN_VECTOR_DIMENSIONS, MAX_VECTOR_DIMENSIONS);
        return Values.float64Vector(nextDoubleArrayRaw(minDim, maxDim));
    }

    public Float64Vector nextFloat64Vector() {
        final int dimension = chooseDimension(Double.BYTES);
        return nextFloat64Vector(dimension, dimension);
    }

    public VectorArray nextVectorArray() {
        return nextVectorArray(minArray(), maxArray());
    }

    private int adaptDimensionsToVectorArrayItem(int dimensions) {
        return Math.clamp(dimensions / 100, Math.min(dimensions, 40), dimensions);
    }

    public VectorArray nextVectorArray(int minLength, int maxLength) {
        int length = intBetween(minLength, maxLength);
        int minDim = adaptDimensionsToVectorArrayItem(configuration.minVectorDimensions());
        int maxDim = adaptDimensionsToVectorArrayItem(configuration.maxVectorDimensions());
        VectorValue[] vectors = new VectorValue[length];
        for (int i = 0; i < length; i++) {
            vectors[i] = nextVectorValue(minDim, maxDim);
        }
        return Values.vectorArray(vectors);
    }

    public UUID nextUUID() {
        long msb = generator.nextLong();
        long lsb = generator.nextLong();

        // See javadoc for java.util.UUID. We need to set some parts of these bytes to recognizable values.
        // Basically the version and the variant.
        msb &= 0xffffffffffff0fffL;
        msb |= 0x0000000000004000L;
        lsb &= 0x3fffffffffffffffL;
        lsb |= 0x8000000000000000L;

        return new UUID(msb, lsb);
    }

    public UUIDValue nextUUIDValue() {
        return Values.uuidValue(nextUUID());
    }

    public UUIDArray nextUUIDArray() {
        return new UUIDArray(nextUUIDArrayRaw(minArray(), maxArray()));
    }

    public UUID[] nextUUIDArrayRaw(int minLength, int maxLength) {
        return nextArray(UUID[]::new, this::nextUUID, minLength, maxLength);
    }

    /**
     * Returns the next {@link ArrayValue}, distributed uniformly among all array types.
     *
     * @see RandomValues
     */
    public ArrayValue nextArray() {
        final ListIterable<ValueType> allowedTypes =
                ArrayAdapter.adapt(ValueType.ARRAY_TYPES).select(this::allowedType);
        return (ArrayValue) nextValueOfType(among(allowedTypes));
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
        return switch (type) {
            case 0 -> nextByteValue();
            case 1 -> nextShortValue();
            case 2 -> nextIntValue();
            case 3 -> nextLongValue();
            case 4 -> nextFloatValue();
            case 5 -> nextDoubleValue();
            default -> throw new IllegalArgumentException("Unknown value type " + type);
        };
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

    /**
     * @return {@link VectorValue}.
     * @see RandomValues
     */
    public Value nextVectorValue() {
        return nextValueOfTypes(typesOfCategories(ValueCategory.VECTOR));
    }

    public VectorValue nextVectorValue(int minDim, int maxDim) {
        final ValueType type = among(typesOfCategories(ValueCategory.VECTOR));
        return nextVectorValue(type, minDim, maxDim);
    }

    public VectorValue nextVectorValue(ValueType type, int minDim, int maxDim) {
        return switch (type) {
            case INT8_VECTOR -> nextInt8Vector(minDim, maxDim);
            case INT16_VECTOR -> nextInt16Vector(minDim, maxDim);
            case INT32_VECTOR -> nextInt32Vector(minDim, maxDim);
            case INT64_VECTOR -> nextInt64Vector(minDim, maxDim);
            case FLOAT32_VECTOR -> nextFloat32Vector(minDim, maxDim);
            case FLOAT64_VECTOR -> nextFloat64Vector(minDim, maxDim);
            default -> throw new IllegalStateException("Unexpected vector type: " + type);
        };
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
        return switch (nextInt) {
            case 0 -> nextDateValue();
            case 1 -> nextLocalDateTimeValue();
            case 2 -> nextDateTimeValue();
            case 3 -> nextLocalTimeValue();
            case 4 -> nextTimeValue();
            case 5 -> nextDuration();
            default -> throw new IllegalArgumentException(nextInt + " not a valid temporal type");
        };
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
        return switch (nextInt) {
            case 0 -> nextCartesianPoint();
            case 1 -> nextCartesian3DPoint();
            case 2 -> nextGeographicPoint();
            case 3 -> nextGeographic3DPoint();
            default -> throw new IllegalStateException(nextInt + " not a valid point type");
        };
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

    private static TextArray stringArrayAsTextArray(String[] array) {
        StringValue[] values = new StringValue[array.length];
        for (int i = 0; i < array.length; i++) {
            String s = array[i];
            values[i] = s == null ? null : Values.utf8Value(s);
        }
        return Values.stringArray(values);
    }

    /**
     * @return the next {@link TextArray} containing strings with only alpha-numeric characters.
     * @see RandomValues
     */
    public TextArray nextAlphaNumericTextArray() {
        String[] array = nextAlphaNumericStringArrayRaw(minArray(), maxArray(), minString(), maxString());
        return stringArrayAsTextArray(array);
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
        return stringArrayAsTextArray(array);
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
        return stringArrayAsTextArray(array);
    }

    /**
     * @see RandomValues
     */
    public TextArray nextTextArray() {
        String[] array = nextStringArrayRaw(minArray(), maxArray(), minString(), maxString());
        return stringArrayAsTextArray(array);
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
        return switch (nextInt) {
            case 0 -> nextCartesianPointArray();
            case 1 -> nextCartesian3DPointArray();
            case 2 -> nextGeographicPointArray();
            case 3 -> nextGeographic3DPointArray();
            default -> throw new IllegalStateException(nextInt + " not a valid point type");
        };
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

    public long among(LongIterable among) {
        if (among instanceof final LongList intList) {
            return among(intList);
        }

        int offset = nextInt(among.size());
        final var iterator = among.longIterator();
        while (offset-- > 0) {
            iterator.next();
        }
        return iterator.next();
    }

    public int among(IntList among) {
        return among.get(nextInt(among.size()));
    }

    public int among(IntIterable among) {
        if (among instanceof final IntList intList) {
            return among(intList);
        }

        int offset = nextInt(among.size());
        final var iterator = among.intIterator();
        while (offset-- > 0) {
            iterator.next();
        }
        return iterator.next();
    }

    public <T> T among(ListIterable<T> among) {
        return among.get(generator.nextInt(among.size()));
    }

    public <T> T among(RichIterable<T> among) {
        if (among instanceof final ListIterable<T> list) {
            return among(list);
        }

        int offset = nextInt(among.size());
        final var iterator = among.iterator();
        while (offset-- > 0) {
            iterator.next();
        }
        return iterator.next();
    }

    public <T> T among(Collection<T> among) {
        if (among instanceof final List<T> list) {
            return among(list);
        }

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

    // This information is duplicated and could end up out-of-sync, but since it is a major hassle to decouple this
    // everywhere the tradeoff is reasonable.
    // Account for some extra type information in the key, and a 20% buffer just in case.
    public static int maxSizeInIndexKey(int numberOfProperties) {
        // 8 + numberOfProperties * (1 + sizeInIndexKey) <= 8175
        return ((8175 - 8) / numberOfProperties - 1) * 8 / 10;
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

    private int chooseDimension(int size) {
        final IntList dimensions = configuration.vectorDimensionChoices();
        final int dimension;
        if (dimensions != null) {
            assert dimensions.notEmpty();
            dimension = among(dimensions);
        } else {
            final int min = minDimensions();
            final int max = Math.min(maxVectorNumBytes() / size, maxDimensions());
            assert min <= max : "Cannot choose dimensions from [" + min + " to " + max + "]";
            dimension = intBetween(min, max);
        }
        assert dimension * size <= maxVectorNumBytes();
        return dimension;
    }

    private int minDimensions() {
        return Math.max(configuration.minVectorDimensions(), MIN_VECTOR_DIMENSIONS);
    }

    private int maxDimensions() {
        return Math.min(configuration.maxVectorDimensions(), MAX_VECTOR_DIMENSIONS);
    }

    private int maxVectorNumBytes() {
        return configuration.maxVectorNumBytes();
    }

    private boolean allowedType(ValueType type) {
        return configuration.allowedTypes().contains(type);
    }

    @FunctionalInterface
    private interface ElementFactory<T> {
        T generate();
    }

    @FunctionalInterface
    private interface CodePointFactory {
        int generate();
    }

    public static final Predicate<ValueType> IS_VECTOR_TYPE = t -> {
        ValueCategory category = t.valueRepresentation.valueGroup().category();
        return category == ValueCategory.VECTOR || category == ValueCategory.VECTOR_ARRAY;
    };

    /**
     * An immutable and thread-safe configuration builder.
     */
    public static class ConfigurationBuilder {
        private final int stringMinLength;
        private final int stringMaxLength;
        private final int arrayMinLength;
        private final int arrayMaxLength;
        private final int minCodePoint;
        private final int maxCodePoint;
        private final int minVectorDimensions;
        private final int maxVectorDimensions;
        private final ImmutableIntList vectorDimensionChoices;
        private final int maxVectorNumBytes;
        private final ImmutableSortedSet<ValueType> allowedTypes;

        private ConfigurationBuilder() {
            this(
                    5,
                    20,
                    1,
                    10,
                    Character.MIN_CODE_POINT,
                    Character.MAX_CODE_POINT,
                    MIN_VECTOR_DIMENSIONS,
                    MAX_VECTOR_DIMENSIONS,
                    null,
                    MAX_VECTOR_DIMENSIONS * Double.BYTES,
                    SortedSets.immutable.ofAll(Arrays.asList(ValueType.ALL_TYPES)));
        }

        private ConfigurationBuilder(
                int stringMinLength,
                int stringMaxLength,
                int arrayMinLength,
                int arrayMaxLength,
                int minCodePoint,
                int maxCodePoint,
                int minVectorDimensions,
                int maxVectorDimensions,
                ImmutableIntList vectorDimensionChoices,
                int maxVectorNumBytes,
                ImmutableSortedSet<ValueType> allowedTypes) {
            this.stringMinLength = stringMinLength;
            this.stringMaxLength = stringMaxLength;
            this.arrayMinLength = arrayMinLength;
            this.arrayMaxLength = arrayMaxLength;
            this.minCodePoint = minCodePoint;
            this.maxCodePoint = maxCodePoint;
            this.minVectorDimensions = minVectorDimensions;
            this.maxVectorDimensions = maxVectorDimensions;
            this.vectorDimensionChoices = vectorDimensionChoices;
            this.maxVectorNumBytes = maxVectorNumBytes;
            this.allowedTypes = allowedTypes;
        }

        public Configuration build() {
            // Even if vector dimension choices explicitly set
            // Configuration interface can still query min/max vector dimensions
            int minVectorDimensions = this.minVectorDimensions;
            int maxVectorDimensions = this.maxVectorDimensions;

            if (vectorDimensionChoices != null) {
                minVectorDimensions = vectorDimensionChoices.getFirst();
                maxVectorDimensions = vectorDimensionChoices.getLast();
            }

            return new ConfigurationRecord(
                    stringMinLength,
                    stringMaxLength,
                    arrayMinLength,
                    arrayMaxLength,
                    minCodePoint,
                    maxCodePoint,
                    minVectorDimensions,
                    maxVectorDimensions,
                    vectorDimensionChoices,
                    maxVectorNumBytes,
                    allowedTypes);
        }

        public ConfigurationBuilder stringMinLength(int length) {
            return new ConfigurationBuilder(
                    length,
                    this.stringMaxLength,
                    this.arrayMinLength,
                    this.arrayMaxLength,
                    this.minCodePoint,
                    this.maxCodePoint,
                    this.minVectorDimensions,
                    this.maxVectorDimensions,
                    this.vectorDimensionChoices,
                    this.maxVectorNumBytes,
                    this.allowedTypes);
        }

        public ConfigurationBuilder stringMaxLength(int length) {
            return new ConfigurationBuilder(
                    this.stringMinLength,
                    length,
                    this.arrayMinLength,
                    this.arrayMaxLength,
                    this.minCodePoint,
                    this.maxCodePoint,
                    this.minVectorDimensions,
                    this.maxVectorDimensions,
                    this.vectorDimensionChoices,
                    this.maxVectorNumBytes,
                    this.allowedTypes);
        }

        public ConfigurationBuilder stringLength(int min, int max) {
            assert min <= max : "min must be greater or equal to max";
            return stringMinLength(min).stringMaxLength(max);
        }

        public ConfigurationBuilder stringLength(int length) {
            return stringLength(length, length);
        }

        public ConfigurationBuilder arrayMinLength(int length) {
            return new ConfigurationBuilder(
                    this.stringMinLength,
                    this.stringMaxLength,
                    length,
                    this.arrayMaxLength,
                    this.minCodePoint,
                    this.maxCodePoint,
                    this.minVectorDimensions,
                    this.maxVectorDimensions,
                    this.vectorDimensionChoices,
                    this.maxVectorNumBytes,
                    this.allowedTypes);
        }

        public ConfigurationBuilder arrayMaxLength(int length) {
            return new ConfigurationBuilder(
                    this.stringMinLength,
                    this.stringMaxLength,
                    this.arrayMinLength,
                    length,
                    this.minCodePoint,
                    this.maxCodePoint,
                    this.minVectorDimensions,
                    this.maxVectorDimensions,
                    this.vectorDimensionChoices,
                    this.maxVectorNumBytes,
                    this.allowedTypes);
        }

        public ConfigurationBuilder arrayLength(int min, int max) {
            assert min <= max : "min must be greater or equal to max";
            return arrayMinLength(min).arrayMaxLength(max);
        }

        public ConfigurationBuilder arrayLength(int length) {
            return arrayLength(length, length);
        }

        public ConfigurationBuilder minCodePoint(int codePoint) {
            return new ConfigurationBuilder(
                    this.stringMinLength,
                    this.stringMaxLength,
                    this.arrayMinLength,
                    this.arrayMaxLength,
                    codePoint,
                    this.maxCodePoint,
                    this.minVectorDimensions,
                    this.maxVectorDimensions,
                    this.vectorDimensionChoices,
                    this.maxVectorNumBytes,
                    this.allowedTypes);
        }

        public ConfigurationBuilder maxCodePoint(int codePoint) {
            return new ConfigurationBuilder(
                    this.stringMinLength,
                    this.stringMaxLength,
                    this.arrayMinLength,
                    this.arrayMaxLength,
                    this.minCodePoint,
                    codePoint,
                    this.minVectorDimensions,
                    this.maxVectorDimensions,
                    this.vectorDimensionChoices,
                    this.maxVectorNumBytes,
                    this.allowedTypes);
        }

        public ConfigurationBuilder codePoints(int min, int max) {
            assert min <= max : "min must be greater or equal to max";
            return minCodePoint(min).maxCodePoint(max);
        }

        public ConfigurationBuilder minVectorDimensions(int dimensions) {
            assert vectorDimensionChoices == null
                    : "cannot set minimum vector dimensions with explicit vector dimension choices";
            return new ConfigurationBuilder(
                    this.stringMinLength,
                    this.stringMaxLength,
                    this.arrayMinLength,
                    this.arrayMaxLength,
                    this.minCodePoint,
                    this.maxCodePoint,
                    dimensions,
                    this.maxVectorDimensions,
                    null,
                    this.maxVectorNumBytes,
                    this.allowedTypes);
        }

        public ConfigurationBuilder maxVectorDimensions(int dimensions) {
            assert vectorDimensionChoices == null
                    : "cannot set maximum vector dimensions with explicit vector dimension choices";
            return new ConfigurationBuilder(
                    this.stringMinLength,
                    this.stringMaxLength,
                    this.arrayMinLength,
                    this.arrayMaxLength,
                    this.minCodePoint,
                    this.maxCodePoint,
                    this.minVectorDimensions,
                    dimensions,
                    null,
                    this.maxVectorNumBytes,
                    this.allowedTypes);
        }

        public ConfigurationBuilder vectorDimensions(int min, int max) {
            assert min <= max : "min must be greater or equal to max";
            return minVectorDimensions(min).maxVectorDimensions(max);
        }

        public ConfigurationBuilder vectorDimensionChoices(int... dimensions) {
            final ImmutableIntList localDimensions;
            if (dimensions != null) {
                assert dimensions.length > 0 : "must provide at least one vector dimension";
                localDimensions =
                        IntLists.mutable.wrapCopy(dimensions).sortThis().toImmutable();
            } else {
                localDimensions = null;
            }

            return new ConfigurationBuilder(
                    this.stringMinLength,
                    this.stringMaxLength,
                    this.arrayMinLength,
                    this.arrayMaxLength,
                    this.minCodePoint,
                    this.maxCodePoint,
                    this.minVectorDimensions,
                    this.maxVectorDimensions,
                    localDimensions,
                    this.maxVectorNumBytes,
                    this.allowedTypes);
        }

        public ConfigurationBuilder maxVectorNumBytes(int maxVectorNumBytes) {
            return new ConfigurationBuilder(
                    this.stringMinLength,
                    this.stringMaxLength,
                    this.arrayMinLength,
                    this.arrayMaxLength,
                    this.minCodePoint,
                    this.maxCodePoint,
                    this.minVectorDimensions,
                    this.maxVectorDimensions,
                    this.vectorDimensionChoices,
                    maxVectorNumBytes,
                    this.allowedTypes);
        }

        public ConfigurationBuilder includeVectorTypes(boolean includeVectorTypes) {
            final var allowedTypes = includeVectorTypes
                    ? this.allowedTypes.newWithAll(LazyIterate.select(Arrays.asList(ALL_TYPES), IS_VECTOR_TYPE))
                    : this.allowedTypes.reject(IS_VECTOR_TYPE);
            return allowedTypes(allowedTypes);
        }

        public ConfigurationBuilder allowedTypes(ValueType... allowedTypes) {
            return allowedTypes(SortedSets.immutable.of(allowedTypes));
        }

        public ConfigurationBuilder allowedTypes(Iterable<ValueType> allowedTypes) {
            assert Iterate.notEmpty(allowedTypes) : "must provide at least one type";
            return new ConfigurationBuilder(
                    this.stringMinLength,
                    this.stringMaxLength,
                    this.arrayMinLength,
                    this.arrayMaxLength,
                    this.minCodePoint,
                    this.maxCodePoint,
                    this.minVectorDimensions,
                    this.maxVectorDimensions,
                    this.vectorDimensionChoices,
                    this.maxVectorNumBytes,
                    SortedSets.immutable.ofAll(allowedTypes));
        }

        public ConfigurationBuilder disallowedTypes(ValueType... disallowedTypes) {
            assert disallowedTypes != null && disallowedTypes.length > 0 : "must provide at least one type";
            ImmutableSet<ValueType> set = Sets.immutable.of(disallowedTypes);
            return disallowedTypes(set::contains);
        }

        public ConfigurationBuilder disallowedTypes(Predicate<ValueType> predicate) {
            return allowedTypes(this.allowedTypes.reject(predicate));
        }
    }

    private record ConfigurationRecord(
            int stringMinLength,
            int stringMaxLength,
            int arrayMinLength,
            int arrayMaxLength,
            int minCodePoint,
            int maxCodePoint,
            int minVectorDimensions,
            int maxVectorDimensions,
            ImmutableIntList vectorDimensionChoices,
            int maxVectorNumBytes,
            ImmutableSortedSet<ValueType> allowedTypes)
            implements Configuration {

        private ConfigurationRecord {
            // configuration invariants
            assert stringMinLength <= stringMaxLength : stringMinLength + "must be <= " + stringMaxLength;
            assert arrayMinLength <= arrayMaxLength : arrayMinLength + "must be <= " + arrayMaxLength;
            assert minCodePoint <= maxCodePoint : minCodePoint + "must be <= " + maxCodePoint;
            if (vectorDimensionChoices != null) {
                assert vectorDimensionChoices.notEmpty();
                assert vectorDimensionChoices.getFirst() == minVectorDimensions;
                assert vectorDimensionChoices.getLast() == maxVectorDimensions;
            }
            assert MIN_VECTOR_DIMENSIONS <= minVectorDimensions;
            assert minVectorDimensions <= maxVectorDimensions;
            assert maxVectorDimensions <= MAX_VECTOR_DIMENSIONS;
            assert maxVectorNumBytes <= maxVectorNumBytes * Double.BYTES;
            assert allowedTypes != null && allowedTypes.notEmpty();
        }

        @Override
        public boolean includeVectorTypes() {
            return allowedTypes.detect(IS_VECTOR_TYPE) != null;
        }
    }
}
