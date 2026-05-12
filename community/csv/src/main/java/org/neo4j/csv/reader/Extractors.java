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
package org.neo4j.csv.reader;

import static java.lang.Character.isWhitespace;
import static java.time.ZoneOffset.UTC;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BOOLEAN_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_DOUBLE_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_FLOAT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_SHORT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.internal.helpers.Numbers.getOverflowMessage;
import static org.neo4j.internal.helpers.Numbers.safeCastLongToByte;
import static org.neo4j.internal.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.internal.helpers.Numbers.safeCastLongToShort;

import java.lang.reflect.Array;
import java.nio.CharBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.CSVHeaderInformation;
import org.neo4j.values.storable.DateArray;
import org.neo4j.values.storable.DateTimeArray;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationArray;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Float32Vector;
import org.neo4j.values.storable.Float64Vector;
import org.neo4j.values.storable.Int16Vector;
import org.neo4j.values.storable.Int32Vector;
import org.neo4j.values.storable.Int64Vector;
import org.neo4j.values.storable.Int8Vector;
import org.neo4j.values.storable.LocalDateTimeArray;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeArray;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeArray;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;

/**
 * Common implementations of {@link Extractor}. Since array values can have a delimiter of user choice that isn't
 * an enum, but a regular class with a constructor where that delimiter can be specified.
 * <p>
 * {@link Extractor} instances are (should try to be) state-less and can therefore be used by multiple threads.
 *
 * <pre>
 * CharSeeker seeker = ...
 * Mark mark = new Mark();
 * Extractors extractors = new Extractors( ';' );
 * int boxFreeIntValue = (Integer) seeker.extract( mark, extractors.int_() );
 * </pre>
 * <p>
 * Custom {@link Extractor extractors} can also be implemented and used, as need arises
 * and {@link Extractors#add(Extractor) added} to an {@link Extractors} instance, where its
 * {@link Extractor#name() name} value is used as key for lookup in {@link #valueOf(String, CSVHeaderInformation)}.
 */
public final class Extractors {

    private static final int MAX_LONG_CHARS = String.valueOf(Long.MAX_VALUE).length() + 1;

    private final Map<String, Extractor<?>> instances = new HashMap<>();
    private final StringExtractor string;
    private final LongExtractor long_;
    private final IntExtractor int_;
    private final CharExtractor char_;
    private final ShortExtractor short_;
    private final ByteExtractor byte_;
    private final BooleanExtractor boolean_;
    private final FloatExtractor float_;
    private final DoubleExtractor double_;
    private final StringArrayExtractor stringArray;
    private final BooleanArrayExtractor booleanArray;
    private final ByteArrayExtractor byteArray;
    private final ShortArrayExtractor shortArray;
    private final IntArrayExtractor intArray;
    private final LongArrayExtractor longArray;
    private final FloatArrayExtractor floatArray;
    private final DoubleArrayExtractor doubleArray;
    private final PointExtractor point;
    private final PointArrayExtractor pointArray;
    private final DateExtractor date;
    private final DateArrayExtractor dateArray;
    private final TimeExtractor time;
    private final TimeArrayExtractor timeArray;
    private final DateTimeExtractor dateTime;
    private final DateTimeArrayExtractor dateTimeArray;
    private final LocalTimeExtractor localTime;
    private final LocalTimeArrayExtractor localTimeArray;
    private final LocalDateTimeExtractor localDateTime;
    private final LocalDateTimeArrayExtractor localDateTimeArray;
    private final DurationExtractor duration;
    private final TextValueExtractor textValue;
    private final DurationArrayExtractor durationArray;
    private final Int8VectorExtractor int8Vector;
    private final Int16VectorExtractor int16Vector;
    private final Int32VectorExtractor int32Vector;
    private final Int64VectorExtractor int64Vector;
    private final Float32VectorExtractor float32Vector;
    private final Float64VectorExtractor float64Vector;

    public Extractors() {
        this(';', ';');
    }

    public Extractors(char arrayDelimiter, char vectorDelimiter) {
        this(arrayDelimiter, vectorDelimiter, COMMAS.emptyQuotedStringsAsNull(), COMMAS.trimStrings(), inUTC);
    }

    public Extractors(char arrayDelimiter, char vectorDelimiter, boolean emptyStringsAsNull) {
        this(arrayDelimiter, vectorDelimiter, emptyStringsAsNull, COMMAS.trimStrings(), inUTC);
    }

    public Extractors(char arrayDelimiter, char vectorDelimiter, boolean emptyStringsAsNull, boolean trimStrings) {
        this(arrayDelimiter, vectorDelimiter, emptyStringsAsNull, trimStrings, inUTC);
    }

    /**
     * Why do we have a public constructor here and why isn't this class an enum?
     * It's because the array extractors can be configured with an array delimiter,
     * something that would be impossible otherwise. There's a {@link #valueOf(String, CSVHeaderInformation)}
     * method to keep the feel of an enum.
     */
    public Extractors(
            char arrayDelimiter,
            char vectorDelimiter,
            boolean emptyStringsAsNull,
            boolean trimStrings,
            Supplier<ZoneId> defaultTimeZone) {
        add(string = new StringExtractor(emptyStringsAsNull));
        add(long_ = new LongExtractor());
        add(int_ = new IntExtractor(long_));
        add(char_ = new CharExtractor(string));
        add(short_ = new ShortExtractor(long_));
        add(byte_ = new ByteExtractor(long_));
        add(boolean_ = new BooleanExtractor());
        add(double_ = new DoubleExtractor());
        add(float_ = new FloatExtractor(double_));
        add(stringArray = new StringArrayExtractor(arrayDelimiter, trimStrings));
        add(booleanArray = new BooleanArrayExtractor(arrayDelimiter));
        add(longArray = new LongArrayExtractor(arrayDelimiter));
        add(byteArray = new ByteArrayExtractor(arrayDelimiter));
        add(shortArray = new ShortArrayExtractor(arrayDelimiter));
        add(intArray = new IntArrayExtractor(arrayDelimiter));
        add(doubleArray = new DoubleArrayExtractor(arrayDelimiter));
        add(floatArray = new FloatArrayExtractor(arrayDelimiter));
        add(point = new PointExtractor());
        add(pointArray = new PointArrayExtractor(arrayDelimiter));
        add(date = new DateExtractor());
        add(dateArray = new DateArrayExtractor(arrayDelimiter));
        add(time = new TimeExtractor(defaultTimeZone));
        add(timeArray = new TimeArrayExtractor(arrayDelimiter, defaultTimeZone));
        add(dateTime = new DateTimeExtractor(defaultTimeZone));
        add(dateTimeArray = new DateTimeArrayExtractor(arrayDelimiter, defaultTimeZone));
        add(localTime = new LocalTimeExtractor());
        add(localTimeArray = new LocalTimeArrayExtractor(arrayDelimiter));
        add(localDateTime = new LocalDateTimeExtractor());
        add(localDateTimeArray = new LocalDateTimeArrayExtractor(arrayDelimiter));
        add(duration = new DurationExtractor());
        add(textValue = new TextValueExtractor(emptyStringsAsNull));
        add(durationArray = new DurationArrayExtractor(arrayDelimiter));
        add(int8Vector = new Int8VectorExtractor(vectorDelimiter));
        add(int16Vector = new Int16VectorExtractor(vectorDelimiter));
        add(int32Vector = new Int32VectorExtractor(vectorDelimiter));
        add(int64Vector = new Int64VectorExtractor(vectorDelimiter));
        add(float32Vector = new Float32VectorExtractor(vectorDelimiter));
        add(float64Vector = new Float64VectorExtractor(vectorDelimiter));
    }

    public void add(Extractor<?> extractor) {
        instances.put(extractor.name().toUpperCase(Locale.ROOT), extractor);
    }

    public Extractor<?> valueOf(String name, CSVHeaderInformation optionalParameter) {
        final var upperCaseName = name.toUpperCase(Locale.ROOT);
        Extractor<?> instance = VectorExtractor.COL_NAME.equals(upperCaseName)
                ? getVectorExtractor(optionalParameter)
                : instances.get(upperCaseName);
        if (instance == null) {
            throw new IllegalArgumentException("'" + name + "' is not a valid type.");
        }
        return instance;
    }

    private Extractor<?> getVectorExtractor(CSVHeaderInformation optionalParameter) {
        switch (optionalParameter) {
            case VectorExtractor.VectorCSVHeaderInformation vectorCSVHeaderInformation -> {
                final var baseExtractor =
                        switch (vectorCSVHeaderInformation.getCoordinateType()) {
                            case INTEGER8 -> instances.get(Int8VectorExtractor.NAME.toUpperCase(Locale.ROOT));
                            case INTEGER16 -> instances.get(Int16VectorExtractor.NAME.toUpperCase(Locale.ROOT));
                            case INTEGER32 -> instances.get(Int32VectorExtractor.NAME.toUpperCase(Locale.ROOT));
                            case INTEGER64 -> instances.get(Int64VectorExtractor.NAME.toUpperCase(Locale.ROOT));
                            case FLOAT32 -> instances.get(Float32VectorExtractor.NAME.toUpperCase(Locale.ROOT));
                            case FLOAT64 -> instances.get(Float64VectorExtractor.NAME.toUpperCase(Locale.ROOT));
                        };

                return ((VectorExtractor<?>) baseExtractor)
                        .getDimensionVerifyingExtractor(vectorCSVHeaderInformation.getDimensions());
            }
            case null ->
                throw new IllegalArgumentException(
                        "vector must specify dimensions and coordinate type, e.g.\"v:vector{dimensions:10, coordinateType:byte}\"");
            default -> throw new IllegalStateException("Wrong header information type: " + optionalParameter);
        }
    }

    public Extractor<String> string() {
        return string;
    }

    public Extractor<Long> long_() {
        return long_;
    }

    public Extractor<Integer> int_() {
        return int_;
    }

    public Extractor<Character> char_() {
        return char_;
    }

    public Extractor<Short> short_() {
        return short_;
    }

    public Extractor<Byte> byte_() {
        return byte_;
    }

    public Extractor<Boolean> boolean_() {
        return boolean_;
    }

    public Extractor<Float> float_() {
        return float_;
    }

    public Extractor<Double> double_() {
        return double_;
    }

    public Extractor<String[]> stringArray() {
        return stringArray;
    }

    public Extractor<boolean[]> booleanArray() {
        return booleanArray;
    }

    public Extractor<byte[]> byteArray() {
        return byteArray;
    }

    public Extractor<short[]> shortArray() {
        return shortArray;
    }

    public Extractor<int[]> intArray() {
        return intArray;
    }

    public Extractor<long[]> longArray() {
        return longArray;
    }

    public Extractor<float[]> floatArray() {
        return floatArray;
    }

    public Extractor<double[]> doubleArray() {
        return doubleArray;
    }

    public Extractor<PointValue> point() {
        return point;
    }

    public Extractor<PointArray> pointArray() {
        return pointArray;
    }

    public Extractor<DateValue> date() {
        return date;
    }

    public Extractor<DateArray> dateArray() {
        return dateArray;
    }

    public Extractor<TimeValue> time() {
        return time;
    }

    public Extractor<TimeArray> timeArray() {
        return timeArray;
    }

    public Extractor<DateTimeValue> dateTime() {
        return dateTime;
    }

    public Extractor<DateTimeArray> dateTimeArray() {
        return dateTimeArray;
    }

    public Extractor<LocalTimeValue> localTime() {
        return localTime;
    }

    public Extractor<LocalTimeArray> localTimeArray() {
        return localTimeArray;
    }

    public Extractor<LocalDateTimeValue> localDateTime() {
        return localDateTime;
    }

    public Extractor<LocalDateTimeArray> localDateTimeArray() {
        return localDateTimeArray;
    }

    public Extractor<DurationValue> duration() {
        return duration;
    }

    public Extractor<TextValue> textValue() {
        return textValue;
    }

    public Extractor<DurationArray> durationArray() {
        return durationArray;
    }

    public Extractor<Int8Vector> int8Vector() {
        return int8Vector;
    }

    public Extractor<Int16Vector> int16Vector() {
        return int16Vector;
    }

    public Extractor<Int32Vector> int32Vector() {
        return int32Vector;
    }

    public Extractor<Int64Vector> int64Vector() {
        return int64Vector;
    }

    public Extractor<Float32Vector> float32Vector() {
        return float32Vector;
    }

    public Extractor<Float64Vector> float64Vector() {
        return float64Vector;
    }

    private abstract static class AbstractExtractor<T> implements Extractor<T> {
        private final String name;
        private final Extractor<?> normalizedExtractor;

        AbstractExtractor(String name) {
            this(name, null);
        }

        AbstractExtractor(String name, Extractor<?> normalizedExtractor) {
            this.name = name;
            this.normalizedExtractor = normalizedExtractor;
        }

        @Override
        public T extract(char[] data, int offset, int length, boolean hadQuotes) {
            return extract(data, offset, length, hadQuotes, null);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Extractor<?> normalize() {
            return normalizedExtractor != null ? normalizedExtractor : this;
        }

        @Override
        public boolean isEmpty(Object value) {
            return value == null || value == Values.NO_VALUE;
        }

        @Override
        public boolean equals(Object o) {
            return this == o || this.getClass().equals(o.getClass());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass());
        }
    }

    private static final class StringExtractor extends AbstractExtractor<String> {
        private final boolean emptyStringsAsNull;

        public StringExtractor(boolean emptyStringsAsNull) {
            super(String.class.getSimpleName());
            this.emptyStringsAsNull = emptyStringsAsNull;
        }

        @Override
        public Class<?> extractedClass() {
            return String.class;
        }

        @Override
        public String extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            if (length == 0 && (!hadQuotes || emptyStringsAsNull)) {
                return null;
            }
            return new String(data, offset, length);
        }
    }

    private static final class LongExtractor extends AbstractExtractor<Long> {
        LongExtractor() {
            super(long.class.getSimpleName());
        }

        @Override
        public Class<?> extractedClass() {
            return long.class;
        }

        @Override
        public Long extract(char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            return length == 0 ? null : extractLong(data, offset, length);
        }
    }

    private static final class IntExtractor extends AbstractExtractor<Integer> {
        IntExtractor(LongExtractor longExtractor) {
            super(int.class.getSimpleName(), longExtractor);
        }

        @Override
        public Class<?> extractedClass() {
            return int.class;
        }

        @Override
        public Integer extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            return length == 0 ? null : safeCastLongToInt(extractLong(data, offset, length));
        }
    }

    private static final class ShortExtractor extends AbstractExtractor<Short> {
        ShortExtractor(LongExtractor longExtractor) {
            super(short.class.getSimpleName(), longExtractor);
        }

        @Override
        public Short extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            return length == 0 ? null : safeCastLongToShort(extractLong(data, offset, length));
        }

        @Override
        public Class<?> extractedClass() {
            return short.class;
        }
    }

    private static final class ByteExtractor extends AbstractExtractor<Byte> {
        ByteExtractor(LongExtractor longExtractor) {
            super(byte.class.getSimpleName(), longExtractor);
        }

        @Override
        public Byte extract(char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            return length == 0 ? null : safeCastLongToByte(extractLong(data, offset, length));
        }

        @Override
        public Class<?> extractedClass() {
            return byte.class;
        }
    }

    private static final class BooleanExtractor extends AbstractExtractor<Boolean> {
        BooleanExtractor() {
            super(boolean.class.getSimpleName());
        }

        @Override
        public Boolean extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            return length == 0 ? null : extractBoolean(data, offset, length);
        }

        @Override
        public Class<?> extractedClass() {
            return boolean.class;
        }
    }

    private static final class CharExtractor extends AbstractExtractor<Character> {
        CharExtractor(StringExtractor stringExtractor) {
            super(char.class.getSimpleName(), stringExtractor);
        }

        @Override
        public Character extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            if (length > 1) {
                throw new IllegalStateException("Was told to extract a character, but length:" + length);
            }
            return length == 0 ? null : data[offset];
        }

        @Override
        public Class<?> extractedClass() {
            return char.class;
        }
    }

    private static final class FloatExtractor extends AbstractExtractor<Float> {
        FloatExtractor(DoubleExtractor doubleExtractor) {
            super(float.class.getSimpleName(), doubleExtractor);
        }

        @Override
        public Float extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            if (length == 0) {
                return null;
            }
            try {
                // TODO Figure out a way to do this conversion without round tripping to String
                // parseFloat automatically handles leading/trailing whitespace so no need for us to do it
                return Float.parseFloat(String.valueOf(data, offset, length));
            } catch (NumberFormatException ignored) {
                throw new NumberFormatException("Not a number: \"" + String.valueOf(data, offset, length) + "\"");
            }
        }

        @Override
        public Class<?> extractedClass() {
            return float.class;
        }
    }

    private static final class DoubleExtractor extends AbstractExtractor<Double> {
        DoubleExtractor() {
            super(double.class.getSimpleName());
        }

        @Override
        public Double extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            if (length == 0) {
                return null;
            }
            try {
                // TODO Figure out a way to do this conversion without round tripping to String
                // parseDouble automatically handles leading/trailing whitespace so no need for us to do it
                return Double.parseDouble(String.valueOf(data, offset, length));
            } catch (NumberFormatException ignored) {
                throw new NumberFormatException("Not a number: \"" + String.valueOf(data, offset, length) + "\"");
            }
        }

        @Override
        public Class<?> extractedClass() {
            return double.class;
        }
    }

    /**
     * Base class for ArrayExtractors that produces a final type T while parsing individual elements into an
     * intermediary type E.
     */
    private abstract static class ArrayExtractor<E, T> extends AbstractExtractor<T> {
        protected final char arrayDelimiter;

        ArrayExtractor(char arrayDelimiter, Class<T> arrayType) {
            this(arrayDelimiter, arrayType, null);
        }

        ArrayExtractor(char arrayDelimiter, Class<T> arrayType, Extractor<?> normalizedExtractor) {
            this(arrayDelimiter, arrayType.getSimpleName(), normalizedExtractor);
        }

        ArrayExtractor(char arrayDelimiter, String componentTypeName, Extractor<?> normalizedExtractor) {
            super(componentTypeName, normalizedExtractor);
            this.arrayDelimiter = arrayDelimiter;
        }

        @Override
        public final T extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            if (numberOfValues <= 0) {
                return emptyElement();
            }

            var hasStartBracket = false;
            var charIndex = 0;
            if (allowBracketsToBeStripped() && data[offset] == '[') {
                hasStartBracket = true;
                charIndex++;
            }

            E values = createInternalArray(numberOfValues);
            for (int arrayIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, hasStartBracket, offset + charIndex, length - charIndex);
                parseAndStoreElement(data, offset, charIndex, numberOfChars, optionalData, values, arrayIndex);
                charIndex += numberOfChars;
            }

            return convertListToArrayValue(values);
        }

        protected abstract T emptyElement();

        protected abstract E createInternalArray(int size);

        // We use a parse and store pattern, so we can avoid boxing/unboxing and unnecessary copying to a final list
        // if we can avoid
        protected abstract void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                E dest,
                int destIndex);

        protected abstract T convertListToArrayValue(E values);

        public boolean allowBracketsToBeStripped() {
            return false;
        }

        private int charsToNextDelimiter(char[] data, boolean hasStartBracket, int offset, int length) {
            for (int i = 0; i < length; i++) {
                if (data[offset + i] == arrayDelimiter) {
                    return i;
                }
            }

            if (hasStartBracket) {
                if (data[offset + length - 1] != ']') {
                    throw new IllegalStateException(
                            "Array content expected between '[' and ']' but no terminal ']' character found");
                }
                return length - 1;
            }
            return length;
        }

        private int numberOfValues(char[] data, int offset, int length) {
            int count = length > 0 ? 1 : 0;
            for (int i = 0; i < length; i++) {
                if (data[offset + i] == arrayDelimiter) {
                    count++;
                }
            }
            return count;
        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj != null && getClass().equals(obj.getClass());
        }

        @Override
        public boolean isEmpty(Object value) {
            return super.isEmpty(value) || (value.getClass().isArray() && Array.getLength(value) == 0);
        }
    }

    private static final class StringArrayExtractor extends ArrayExtractor<String[], String[]> {
        private final boolean trimStrings;

        StringArrayExtractor(char arrayDelimiter, boolean trimStrings) {
            super(arrayDelimiter, String[].class);
            this.trimStrings = trimStrings;
        }

        @Override
        protected String[] emptyElement() {
            return EMPTY_STRING_ARRAY;
        }

        @Override
        protected String[] createInternalArray(int size) {
            return new String[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                String[] dest,
                int destIndex) {
            String value = new String(data, offset + charIndex, numberOfChars);
            if (trimStrings) {
                value = value.trim();
            }
            dest[destIndex] = value;
        }

        @Override
        protected String[] convertListToArrayValue(String[] values) {
            return values;
        }

        @Override
        public Class<?> extractedClass() {
            return String[].class;
        }
    }

    private static final class ByteArrayExtractor extends ArrayExtractor<byte[], byte[]> {
        ByteArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, byte[].class);
        }

        @Override
        public boolean allowBracketsToBeStripped() {
            return true;
        }

        @Override
        protected byte[] emptyElement() {
            return EMPTY_BYTE_ARRAY;
        }

        @Override
        protected byte[] createInternalArray(int size) {
            return new byte[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                byte[] dest,
                int destIndex) {
            dest[destIndex] = safeCastLongToByte(extractLong(data, offset + charIndex, numberOfChars));
        }

        @Override
        protected byte[] convertListToArrayValue(byte[] values) {
            return values;
        }

        @Override
        public Class<?> extractedClass() {
            return byte[].class;
        }
    }

    private static final class Int8VectorExtractor extends ArrayExtractor<byte[], Int8Vector>
            implements VectorExtractor<Int8Vector> {

        public static final String NAME = "Int8Vector";

        Int8VectorExtractor(char vectorDelimiter) {
            super(vectorDelimiter, NAME, null);
        }

        @Override
        public Extractor<Int8Vector> getDimensionVerifyingExtractor(int expectedDimensions) {
            return new DimensionVerifyingVectorExtractorWrapper<>(this, expectedDimensions);
        }

        @Override
        public boolean allowBracketsToBeStripped() {
            return true;
        }

        @Override
        protected Int8Vector emptyElement() {
            return null;
        }

        @Override
        protected byte[] createInternalArray(int size) {
            return new byte[size];
        }

        @Override
        public boolean isEmpty(Object value) {
            return VectorExtractor.super.isEmpty(value);
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                byte[] dest,
                int destIndex) {
            dest[destIndex] = safeCastLongToByte(extractLong(data, offset + charIndex, numberOfChars));
        }

        @Override
        protected Int8Vector convertListToArrayValue(byte[] values) {
            return Values.int8Vector(values);
        }

        @Override
        public Class<?> extractedClass() {
            return Int8Vector.class;
        }
    }

    private static final class Int16VectorExtractor extends ArrayExtractor<short[], Int16Vector>
            implements VectorExtractor<Int16Vector> {

        public static final String NAME = "Int16Vector";

        Int16VectorExtractor(char vectorDelimiter) {
            super(vectorDelimiter, NAME, null);
        }

        @Override
        public Extractor<Int16Vector> getDimensionVerifyingExtractor(int expectedDimensions) {
            return new DimensionVerifyingVectorExtractorWrapper<>(this, expectedDimensions);
        }

        @Override
        protected Int16Vector emptyElement() {
            return null;
        }

        @Override
        public boolean allowBracketsToBeStripped() {
            return true;
        }

        @Override
        protected short[] createInternalArray(int size) {
            return new short[size];
        }

        @Override
        public boolean isEmpty(Object value) {
            return VectorExtractor.super.isEmpty(value);
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                short[] dest,
                int destIndex) {
            dest[destIndex] = safeCastLongToShort(extractLong(data, offset + charIndex, numberOfChars));
        }

        @Override
        protected Int16Vector convertListToArrayValue(short[] values) {
            return Values.int16Vector(values);
        }

        @Override
        public Class<?> extractedClass() {
            return Int16Vector.class;
        }
    }

    private static final class Int32VectorExtractor extends ArrayExtractor<int[], Int32Vector>
            implements VectorExtractor<Int32Vector> {

        public static final String NAME = "Int32Vector";

        Int32VectorExtractor(char vectorDelimiter) {
            super(vectorDelimiter, NAME, null);
        }

        @Override
        public Extractor<Int32Vector> getDimensionVerifyingExtractor(int expectedDimensions) {
            return new DimensionVerifyingVectorExtractorWrapper<>(this, expectedDimensions);
        }

        @Override
        protected Int32Vector emptyElement() {
            return null;
        }

        @Override
        public boolean allowBracketsToBeStripped() {
            return true;
        }

        @Override
        protected int[] createInternalArray(int size) {
            return new int[size];
        }

        @Override
        public Class<?> extractedClass() {
            return Int32Vector.class;
        }

        @Override
        public boolean isEmpty(Object value) {
            return VectorExtractor.super.isEmpty(value);
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                int[] dest,
                int destIndex) {
            dest[destIndex] = safeCastLongToInt(extractLong(data, offset + charIndex, numberOfChars));
        }

        @Override
        protected Int32Vector convertListToArrayValue(int[] values) {
            return Values.int32Vector(values);
        }
    }

    private static final class Int64VectorExtractor extends ArrayExtractor<long[], Int64Vector>
            implements VectorExtractor<Int64Vector> {

        public static final String NAME = "Int64Vector";

        Int64VectorExtractor(char vectorDelimiter) {
            super(vectorDelimiter, NAME, null);
        }

        @Override
        public Extractor<Int64Vector> getDimensionVerifyingExtractor(int expectedDimensions) {
            return new DimensionVerifyingVectorExtractorWrapper<>(this, expectedDimensions);
        }

        @Override
        protected Int64Vector emptyElement() {
            return null;
        }

        @Override
        public boolean allowBracketsToBeStripped() {
            return true;
        }

        @Override
        protected long[] createInternalArray(int size) {
            return new long[size];
        }

        @Override
        public Class<?> extractedClass() {
            return Int64Vector.class;
        }

        @Override
        public boolean isEmpty(Object value) {
            return VectorExtractor.super.isEmpty(value);
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                long[] dest,
                int destIndex) {
            dest[destIndex] = extractLong(data, offset + charIndex, numberOfChars);
        }

        @Override
        protected Int64Vector convertListToArrayValue(long[] values) {
            return Values.int64Vector(values);
        }
    }

    private static final class Float32VectorExtractor extends ArrayExtractor<float[], Float32Vector>
            implements VectorExtractor<Float32Vector> {

        public static final String NAME = "Float32Vector";

        Float32VectorExtractor(char vectorDelimiter) {
            super(vectorDelimiter, NAME, null);
        }

        @Override
        public Extractor<Float32Vector> getDimensionVerifyingExtractor(int expectedDimensions) {
            return new DimensionVerifyingVectorExtractorWrapper<>(this, expectedDimensions);
        }

        @Override
        protected Float32Vector emptyElement() {
            return null;
        }

        @Override
        public boolean allowBracketsToBeStripped() {
            return true;
        }

        @Override
        protected float[] createInternalArray(int size) {
            return new float[size];
        }

        @Override
        public Class<?> extractedClass() {
            return Float32Vector.class;
        }

        @Override
        public boolean isEmpty(Object value) {
            return VectorExtractor.super.isEmpty(value);
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                float[] dest,
                int destIndex) {
            dest[destIndex] = Float.parseFloat(String.valueOf(data, offset + charIndex, numberOfChars));
        }

        @Override
        protected Float32Vector convertListToArrayValue(float[] values) {
            return Values.float32Vector(values);
        }
    }

    private static final class Float64VectorExtractor extends ArrayExtractor<double[], Float64Vector>
            implements VectorExtractor<Float64Vector> {

        public static final String NAME = "Float64Vector";

        Float64VectorExtractor(char vectorDelimiter) {
            super(vectorDelimiter, NAME, null);
        }

        @Override
        public Extractor<Float64Vector> getDimensionVerifyingExtractor(int expectedDimensions) {
            return new DimensionVerifyingVectorExtractorWrapper<>(this, expectedDimensions);
        }

        @Override
        protected Float64Vector emptyElement() {
            return null;
        }

        @Override
        public boolean allowBracketsToBeStripped() {
            return true;
        }

        @Override
        protected double[] createInternalArray(int size) {
            return new double[size];
        }

        @Override
        public Class<?> extractedClass() {
            return Float64Vector.class;
        }

        @Override
        public boolean isEmpty(Object value) {
            return VectorExtractor.super.isEmpty(value);
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                double[] dest,
                int destIndex) {
            dest[destIndex] = Double.parseDouble(String.valueOf(data, offset + charIndex, numberOfChars));
        }

        @Override
        protected Float64Vector convertListToArrayValue(double[] values) {
            return Values.float64Vector(values);
        }
    }

    private static final class DimensionVerifyingVectorExtractorWrapper<E, T extends VectorValue>
            extends ArrayExtractor<E, T> {

        private final ArrayExtractor<E, T> delegate;
        private final int expectedDimensions;

        DimensionVerifyingVectorExtractorWrapper(ArrayExtractor<E, T> delegate, int expectedDimensions) {
            super(delegate.arrayDelimiter, delegate.name(), null);
            this.delegate = delegate;
            this.expectedDimensions = expectedDimensions;
        }

        @Override
        protected T emptyElement() {
            return delegate.emptyElement();
        }

        @Override
        public boolean allowBracketsToBeStripped() {
            return true;
        }

        @Override
        protected E createInternalArray(int size) {
            return delegate.createInternalArray(size);
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                E dest,
                int destIndex) {
            delegate.parseAndStoreElement(data, offset, charIndex, numberOfChars, optionalData, dest, destIndex);
        }

        @Override
        protected T convertListToArrayValue(E values) {
            final var vectorValue = delegate.convertListToArrayValue(values);
            if (vectorValue.dimensions() != expectedDimensions) {
                throw new IllegalArgumentException("Header specified %d dimensions, but vector has %d dimensions: %s"
                        .formatted(expectedDimensions, vectorValue.dimensions(), vectorValue));
            }
            return vectorValue;
        }

        @Override
        public Class<?> extractedClass() {
            return delegate.extractedClass();
        }

        @Override
        public boolean isEmpty(Object value) {
            return delegate.isEmpty(value);
        }
    }

    private static final class ShortArrayExtractor extends ArrayExtractor<short[], short[]> {
        ShortArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, short[].class);
        }

        @Override
        protected short[] emptyElement() {
            return EMPTY_SHORT_ARRAY;
        }

        @Override
        public boolean allowBracketsToBeStripped() {
            return true;
        }

        @Override
        protected short[] createInternalArray(int size) {
            return new short[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                short[] dest,
                int destIndex) {
            dest[destIndex] = safeCastLongToShort(extractLong(data, offset + charIndex, numberOfChars));
        }

        @Override
        protected short[] convertListToArrayValue(short[] values) {
            return values;
        }

        @Override
        public Class<?> extractedClass() {
            return short[].class;
        }
    }

    private static final class IntArrayExtractor extends ArrayExtractor<int[], int[]> {
        IntArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, int[].class);
        }

        @Override
        protected int[] emptyElement() {
            return EMPTY_INT_ARRAY;
        }

        @Override
        public boolean allowBracketsToBeStripped() {
            return true;
        }

        @Override
        protected int[] createInternalArray(int size) {
            return new int[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                int[] dest,
                int destIndex) {
            dest[destIndex] = safeCastLongToInt(extractLong(data, offset + charIndex, numberOfChars));
        }

        @Override
        protected int[] convertListToArrayValue(int[] values) {
            return values;
        }

        @Override
        public Class<?> extractedClass() {
            return int[].class;
        }
    }

    private static final class LongArrayExtractor extends ArrayExtractor<long[], long[]> {
        LongArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, long[].class);
        }

        @Override
        protected long[] emptyElement() {
            return EMPTY_LONG_ARRAY;
        }

        @Override
        public boolean allowBracketsToBeStripped() {
            return true;
        }

        @Override
        protected long[] createInternalArray(int size) {
            return new long[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                long[] dest,
                int destIndex) {
            dest[destIndex] = extractLong(data, offset + charIndex, numberOfChars);
        }

        @Override
        protected long[] convertListToArrayValue(long[] values) {
            return values;
        }

        @Override
        public Class<?> extractedClass() {
            return long[].class;
        }
    }

    private static final class FloatArrayExtractor extends ArrayExtractor<float[], float[]> {
        FloatArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, float[].class);
        }

        @Override
        protected float[] emptyElement() {
            return EMPTY_FLOAT_ARRAY;
        }

        @Override
        public boolean allowBracketsToBeStripped() {
            return true;
        }

        @Override
        protected float[] createInternalArray(int size) {
            return new float[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                float[] dest,
                int destIndex) {
            // TODO Figure out a way to do this conversion without round tripping to String
            // parseFloat automatically handles leading/trailing whitespace so no need for us to do it
            dest[destIndex] = Float.parseFloat(String.valueOf(data, offset + charIndex, numberOfChars));
        }

        @Override
        protected float[] convertListToArrayValue(float[] values) {
            return values;
        }

        @Override
        public Class<?> extractedClass() {
            return float[].class;
        }
    }

    private static final class DoubleArrayExtractor extends ArrayExtractor<double[], double[]> {

        DoubleArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, double[].class);
        }

        @Override
        protected double[] emptyElement() {
            return EMPTY_DOUBLE_ARRAY;
        }

        @Override
        public boolean allowBracketsToBeStripped() {
            return true;
        }

        @Override
        protected double[] createInternalArray(int size) {
            return new double[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                double[] dest,
                int destIndex) {
            // TODO Figure out a way to do this conversion without round tripping to String
            // parseFloat automatically handles leading/trailing whitespace so no need for us to do it
            dest[destIndex] = Double.parseDouble(String.valueOf(data, offset + charIndex, numberOfChars));
        }

        @Override
        protected double[] convertListToArrayValue(double[] values) {
            return values;
        }

        @Override
        public Class<?> extractedClass() {
            return double[].class;
        }
    }

    private static final class BooleanArrayExtractor extends ArrayExtractor<boolean[], boolean[]> {
        BooleanArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, boolean[].class);
        }

        @Override
        protected boolean[] emptyElement() {
            return EMPTY_BOOLEAN_ARRAY;
        }

        @Override
        public boolean allowBracketsToBeStripped() {
            return true;
        }

        @Override
        protected boolean[] createInternalArray(int size) {
            return new boolean[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                boolean[] dest,
                int destIndex) {
            dest[destIndex] = extractBoolean(data, offset + charIndex, numberOfChars);
        }

        @Override
        protected boolean[] convertListToArrayValue(boolean[] values) {
            return values;
        }

        @Override
        public Class<?> extractedClass() {
            return boolean[].class;
        }
    }

    private abstract static class ArrayAnyValueExtractor<E, T extends ArrayValue> extends ArrayExtractor<E, T> {
        ArrayAnyValueExtractor(char arrayDelimiter, String valueTypeName) {
            super(arrayDelimiter, valueTypeName + "[]", null);
        }

        @Override
        public boolean isEmpty(Object value) {
            return super.isEmpty(value) || ((ArrayValue) value).isEmpty();
        }
    }

    private static final class PointExtractor extends AbstractExtractor<PointValue> {
        public static final String NAME = "Point";

        PointExtractor() {
            super(NAME);
        }

        @Override
        public PointValue extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            if (length == 0) {
                return null;
            }
            return PointValue.parse(CharBuffer.wrap(data, offset, length), optionalData);
        }

        @Override
        public Class<?> extractedClass() {
            return PointValue.class;
        }
    }

    private static final class PointArrayExtractor extends ArrayAnyValueExtractor<PointValue[], PointArray> {
        private static final PointArray EMPTY = Values.pointArray(new Point[0]);

        PointArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, PointExtractor.NAME);
        }

        @Override
        protected PointArray emptyElement() {
            return EMPTY;
        }

        @Override
        protected PointValue[] createInternalArray(int size) {
            return new PointValue[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                PointValue[] dest,
                int destIndex) {
            dest[destIndex] = PointValue.parse(CharBuffer.wrap(data, offset + charIndex, numberOfChars), optionalData);
        }

        @Override
        protected PointArray convertListToArrayValue(PointValue[] values) {
            return Values.pointArray(values);
        }

        @Override
        public Class<?> extractedClass() {
            return PointArray.class;
        }
    }

    private static final class DateExtractor extends AbstractExtractor<DateValue> {
        public static final String NAME = "Date";

        DateExtractor() {
            super(NAME);
        }

        @Override
        public DateValue extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            if (length == 0) {
                return null;
            }
            return DateValue.parse(CharBuffer.wrap(data, offset, length));
        }

        @Override
        public Class<?> extractedClass() {
            return DateValue.class;
        }
    }

    private static final class DateArrayExtractor extends ArrayAnyValueExtractor<LocalDate[], DateArray> {
        private static final DateArray EMPTY = Values.dateArray(new LocalDate[0]);

        DateArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, DateExtractor.NAME);
        }

        @Override
        protected DateArray emptyElement() {
            return EMPTY;
        }

        @Override
        protected LocalDate[] createInternalArray(int size) {
            return new LocalDate[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                LocalDate[] dest,
                int destIndex) {
            dest[destIndex] = DateValue.parse(CharBuffer.wrap(data, offset + charIndex, numberOfChars))
                    .asObjectCopy();
        }

        @Override
        protected DateArray convertListToArrayValue(LocalDate[] values) {
            return Values.dateArray(values);
        }

        @Override
        public Class<?> extractedClass() {
            return DateArray.class;
        }
    }

    private static final class TimeExtractor extends AbstractExtractor<TimeValue> {
        public static final String NAME = "Time";

        private final Supplier<ZoneId> defaultTimeZone;

        TimeExtractor(Supplier<ZoneId> defaultTimeZone) {
            super(NAME);
            this.defaultTimeZone = defaultTimeZone;
        }

        @Override
        public TimeValue extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            if (length == 0) {
                return null;
            }
            return TimeValue.parse(CharBuffer.wrap(data, offset, length), defaultTimeZone, optionalData);
        }

        @Override
        public Class<?> extractedClass() {
            return TimeValue.class;
        }
    }

    private static final class TimeArrayExtractor extends ArrayAnyValueExtractor<OffsetTime[], TimeArray> {
        private static final TimeArray EMPTY = Values.timeArray(new OffsetTime[0]);

        private final Supplier<ZoneId> defaultTimeZone;

        TimeArrayExtractor(char arrayDelimiter, Supplier<ZoneId> defaultTimeZone) {
            super(arrayDelimiter, TimeExtractor.NAME);
            this.defaultTimeZone = defaultTimeZone;
        }

        @Override
        protected TimeArray emptyElement() {
            return EMPTY;
        }

        @Override
        protected OffsetTime[] createInternalArray(int size) {
            return new OffsetTime[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                OffsetTime[] dest,
                int destIndex) {
            dest[destIndex] = TimeValue.parse(
                            CharBuffer.wrap(data, offset + charIndex, numberOfChars), defaultTimeZone, optionalData)
                    .asObjectCopy();
        }

        @Override
        protected TimeArray convertListToArrayValue(OffsetTime[] values) {
            return Values.timeArray(values);
        }

        @Override
        public Class<?> extractedClass() {
            return TimeArray.class;
        }
    }

    private static final class DateTimeExtractor extends AbstractExtractor<DateTimeValue> {
        public static final String NAME = "DateTime";

        private final Supplier<ZoneId> defaultTimeZone;

        DateTimeExtractor(Supplier<ZoneId> defaultTimeZone) {
            super(NAME);
            this.defaultTimeZone = defaultTimeZone;
        }

        @Override
        public DateTimeValue extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            if (length == 0) {
                return null;
            }
            return DateTimeValue.parse(CharBuffer.wrap(data, offset, length), defaultTimeZone, optionalData);
        }

        @Override
        public Class<?> extractedClass() {
            return DateTimeValue.class;
        }
    }

    private static final class DateTimeArrayExtractor extends ArrayAnyValueExtractor<ZonedDateTime[], DateTimeArray> {
        private static final DateTimeArray EMPTY = Values.dateTimeArray(new ZonedDateTime[0]);

        private final Supplier<ZoneId> defaultTimeZone;

        DateTimeArrayExtractor(char arrayDelimiter, Supplier<ZoneId> defaultTimeZone) {
            super(arrayDelimiter, DateTimeExtractor.NAME);
            this.defaultTimeZone = defaultTimeZone;
        }

        @Override
        protected DateTimeArray emptyElement() {
            return EMPTY;
        }

        @Override
        protected ZonedDateTime[] createInternalArray(int size) {
            return new ZonedDateTime[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                ZonedDateTime[] dest,
                int destIndex) {
            dest[destIndex] = DateTimeValue.parse(
                            CharBuffer.wrap(data, offset + charIndex, numberOfChars), defaultTimeZone, optionalData)
                    .asObjectCopy();
        }

        @Override
        protected DateTimeArray convertListToArrayValue(ZonedDateTime[] values) {
            return Values.dateTimeArray(values);
        }

        @Override
        public Class<?> extractedClass() {
            return DateTimeArray.class;
        }
    }

    private static final class LocalTimeExtractor extends AbstractExtractor<LocalTimeValue> {
        public static final String NAME = "LocalTime";

        LocalTimeExtractor() {
            super(NAME);
        }

        @Override
        public LocalTimeValue extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            if (length == 0) {
                return null;
            }
            return LocalTimeValue.parse(CharBuffer.wrap(data, offset, length));
        }

        @Override
        public Class<?> extractedClass() {
            return LocalTimeValue.class;
        }
    }

    private static final class LocalTimeArrayExtractor extends ArrayAnyValueExtractor<LocalTime[], LocalTimeArray> {
        private static final LocalTimeArray EMPTY = Values.localTimeArray(new LocalTime[0]);

        LocalTimeArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, LocalTimeExtractor.NAME);
        }

        @Override
        protected LocalTimeArray emptyElement() {
            return EMPTY;
        }

        @Override
        protected LocalTime[] createInternalArray(int size) {
            return new LocalTime[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                LocalTime[] dest,
                int destIndex) {
            dest[destIndex] = LocalTimeValue.parse(CharBuffer.wrap(data, offset + charIndex, numberOfChars))
                    .asObjectCopy();
        }

        @Override
        protected LocalTimeArray convertListToArrayValue(LocalTime[] values) {
            return Values.localTimeArray(values);
        }

        @Override
        public Class<?> extractedClass() {
            return LocalTimeArray.class;
        }
    }

    private static final class LocalDateTimeExtractor extends AbstractExtractor<LocalDateTimeValue> {
        public static final String NAME = "LocalDateTime";

        LocalDateTimeExtractor() {
            super(NAME);
        }

        @Override
        public LocalDateTimeValue extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            if (length == 0) {
                return null;
            }
            return LocalDateTimeValue.parse(CharBuffer.wrap(data, offset, length));
        }

        @Override
        public Class<?> extractedClass() {
            return LocalDateTimeValue.class;
        }
    }

    private static final class LocalDateTimeArrayExtractor
            extends ArrayAnyValueExtractor<LocalDateTime[], LocalDateTimeArray> {
        private static final LocalDateTimeArray EMPTY = Values.localDateTimeArray(new LocalDateTime[0]);

        LocalDateTimeArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, LocalDateTimeExtractor.NAME);
        }

        @Override
        protected LocalDateTimeArray emptyElement() {
            return EMPTY;
        }

        @Override
        protected LocalDateTime[] createInternalArray(int size) {
            return new LocalDateTime[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                LocalDateTime[] dest,
                int destIndex) {
            dest[destIndex] = LocalDateTimeValue.parse(CharBuffer.wrap(data, offset + charIndex, numberOfChars))
                    .asObjectCopy();
        }

        @Override
        protected LocalDateTimeArray convertListToArrayValue(LocalDateTime[] values) {
            return Values.localDateTimeArray(values);
        }

        @Override
        public Class<?> extractedClass() {
            return LocalDateTimeArray.class;
        }
    }

    private static final class DurationExtractor extends AbstractExtractor<DurationValue> {
        public static final String NAME = "Duration";

        DurationExtractor() {
            super(NAME);
        }

        @Override
        public DurationValue extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            if (length == 0) {
                return null;
            }
            return DurationValue.parse(CharBuffer.wrap(data, offset, length));
        }

        @Override
        public Class<?> extractedClass() {
            return DurationValue.class;
        }
    }

    private static final class TextValueExtractor extends AbstractExtractor<TextValue> {
        public static final String NAME = "TextValue";

        private final boolean emptyStringsAsNull;

        TextValueExtractor(boolean emptyStringsAsNull) {
            super(NAME);
            this.emptyStringsAsNull = emptyStringsAsNull;
        }

        @Override
        public TextValue extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            if (length == 0 && (!hadQuotes || emptyStringsAsNull)) {
                return null;
            }
            return Values.utf8Value(new String(data, offset, length));
        }

        @Override
        public Class<?> extractedClass() {
            return TextValue.class;
        }
    }

    private static final class DurationArrayExtractor extends ArrayAnyValueExtractor<DurationValue[], DurationArray> {
        private static final DurationArray EMPTY = Values.durationArray(new DurationValue[0]);

        DurationArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, DurationExtractor.NAME);
        }

        @Override
        protected DurationArray emptyElement() {
            return EMPTY;
        }

        @Override
        protected DurationValue[] createInternalArray(int size) {
            return new DurationValue[size];
        }

        @Override
        protected void parseAndStoreElement(
                char[] data,
                int offset,
                int charIndex,
                int numberOfChars,
                CSVHeaderInformation optionalData,
                DurationValue[] dest,
                int destIndex) {
            dest[destIndex] = DurationValue.parse(CharBuffer.wrap(data, offset + charIndex, numberOfChars));
        }

        @Override
        protected DurationArray convertListToArrayValue(DurationValue[] values) {
            return Values.durationArray(values);
        }

        @Override
        public Class<?> extractedClass() {
            return DurationArray.class;
        }
    }

    private static final Supplier<ZoneId> inUTC = () -> UTC;

    private static long extractLong(char[] data, int originalOffset, int fullLength) {
        int offset = originalOffset;
        int length = fullLength;

        // Leading whitespace can be ignored
        while (length > 0 && isWhitespace(data[offset])) {
            offset++;
            length--;
        }
        // Trailing whitespace can be ignored
        while (length > 0 && isWhitespace(data[offset + length - 1])) {
            length--;
        }

        var unitFactor = 1;
        if (length > 0 && data[offset] == '-') {
            unitFactor = -1;
            offset++;
            length--;
        }

        if (length < 1) {
            throw new NumberFormatException(
                    "Not an integer: \"" + String.valueOf(data, originalOffset, fullLength) + "\"");
        }

        // Leading zeros can be ignored
        while (length > 0 && data[offset] == '0') {
            offset++;
            length--;
        }

        long result = 0;
        for (int i = 0; i < length && i < MAX_LONG_CHARS; i++) {
            result = (result * 10) + (digit(data, offset + i, originalOffset, fullLength) * unitFactor);
        }

        // overflow occurred if positive numbers (unitFactor == 1) went negative
        // or negative numbers (unitFactor == -1) went positive
        if (unitFactor == 1 ? result < 0 : result > 0) {
            throw new ArithmeticException(getOverflowMessage(String.valueOf(data, originalOffset, fullLength), "long"));
        }

        return result;
    }

    private static int digit(char[] data, int index, int offsetStart, int length) {
        var digit = data[index] - '0';
        if ((digit < 0) || (digit > 9)) {
            throw new NumberFormatException("Not an integer: \"" + String.valueOf(data, offsetStart, length) + "\"");
        }
        return digit;
    }

    private static final char[] BOOLEAN_TRUE_CHARACTERS;
    private static final char[] BOOLEAN_TRUE_CHARACTERS_UPPER;

    static {
        String trueStr = Boolean.TRUE.toString();
        BOOLEAN_TRUE_CHARACTERS = new char[trueStr.length()];
        trueStr.getChars(0, BOOLEAN_TRUE_CHARACTERS.length, BOOLEAN_TRUE_CHARACTERS, 0);
        BOOLEAN_TRUE_CHARACTERS_UPPER = new char[trueStr.length()];
        trueStr.toUpperCase().getChars(0, BOOLEAN_TRUE_CHARACTERS_UPPER.length, BOOLEAN_TRUE_CHARACTERS_UPPER, 0);
    }

    private static boolean extractBoolean(char[] data, int originalOffset, int fullLength) {
        int offset = originalOffset;
        int length = fullLength;
        // Leading whitespace can be ignored
        while (length > 0 && isWhitespace(data[offset])) {
            offset++;
            length--;
        }
        // Trailing whitespace can be ignored
        while (length > 0 && isWhitespace(data[offset + length - 1])) {
            length--;
        }

        // See if the rest case-insensitively match "true"
        if (length != BOOLEAN_TRUE_CHARACTERS.length) {
            return false;
        }

        for (int i = 0; i < BOOLEAN_TRUE_CHARACTERS.length; i++) {
            char c = data[offset + i];
            if (c != BOOLEAN_TRUE_CHARACTERS[i] && c != BOOLEAN_TRUE_CHARACTERS_UPPER[i]) {
                return false;
            }
        }

        return true;
    }
}
