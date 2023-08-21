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
import static java.lang.reflect.Modifier.isStatic;
import static java.time.ZoneOffset.UTC;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_DOUBLE_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.internal.helpers.Numbers.safeCastLongToByte;
import static org.neo4j.internal.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.internal.helpers.Numbers.safeCastLongToShort;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
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
import org.neo4j.values.storable.LocalDateTimeArray;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeArray;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeArray;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Common implementations of {@link Extractor}. Since array values can have a delimiter of user choice that isn't
 * an enum, but a regular class with a constructor where that delimiter can be specified.
 *
 * {@link Extractor} instances are (should try to be) state-less and can therefore be used by multiple threads.
 *
 * <pre>
 * CharSeeker seeker = ...
 * Mark mark = new Mark();
 * Extractors extractors = new Extractors( ';' );
 * int boxFreeIntValue = (Integer) seeker.extract( mark, extractors.int_() );
 * </pre>
 *
 * Custom {@link Extractor extractors} can also be implemented and used, as need arises
 * and {@link Extractors#add(Extractor) added} to an {@link Extractors} instance, where its
 * {@link Extractor#toString() toString} value is used as key for lookup in {@link #valueOf(String)}.
 */
public class Extractors {
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
    private final Extractor<String[]> stringArray;
    private final Extractor<boolean[]> booleanArray;
    private final Extractor<byte[]> byteArray;
    private final Extractor<short[]> shortArray;
    private final Extractor<int[]> intArray;
    private final Extractor<long[]> longArray;
    private final Extractor<float[]> floatArray;
    private final Extractor<double[]> doubleArray;
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

    public Extractors() {
        this(';');
    }

    public Extractors(char arrayDelimiter) {
        this(arrayDelimiter, COMMAS.emptyQuotedStringsAsNull(), COMMAS.trimStrings(), inUTC);
    }

    public Extractors(char arrayDelimiter, boolean emptyStringsAsNull) {
        this(arrayDelimiter, emptyStringsAsNull, COMMAS.trimStrings(), inUTC);
    }

    public Extractors(char arrayDelimiter, boolean emptyStringsAsNull, boolean trimStrings) {
        this(arrayDelimiter, emptyStringsAsNull, trimStrings, inUTC);
    }

    /**
     * Why do we have a public constructor here and why isn't this class an enum?
     * It's because the array extractors can be configured with an array delimiter,
     * something that would be impossible otherwise. There's an equivalent {@link #valueOf(String)}
     * method to keep the feel of an enum.
     */
    public Extractors(
            char arrayDelimiter, boolean emptyStringsAsNull, boolean trimStrings, Supplier<ZoneId> defaultTimeZone) {
        try {
            for (Field field : getClass().getDeclaredFields()) {
                if (isStatic(field.getModifiers())) {
                    Object value = field.get(null);
                    if (value instanceof Extractor) {
                        instances.put(field.getName(), (Extractor<?>) value);
                    }
                }
            }

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
            add(byteArray = new ByteArrayExtractor(arrayDelimiter));
            add(shortArray = new ShortArrayExtractor(arrayDelimiter));
            add(intArray = new IntArrayExtractor(arrayDelimiter));
            add(longArray = new LongArrayExtractor(arrayDelimiter));
            add(floatArray = new FloatArrayExtractor(arrayDelimiter));
            add(doubleArray = new DoubleArrayExtractor(arrayDelimiter));
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
        } catch (IllegalAccessException e) {
            throw new Error("Bug in reflection code gathering all extractors");
        }
    }

    public void add(Extractor<?> extractor) {
        instances.put(extractor.name().toUpperCase(Locale.ROOT), extractor);
    }

    public Extractor<?> valueOf(String name) {
        Extractor<?> instance = instances.get(name.toUpperCase(Locale.ROOT));
        if (instance == null) {
            throw new IllegalArgumentException("'" + name + "'");
        }
        return instance;
    }

    public Extractor<String> string() {
        return string;
    }

    public LongExtractor long_() {
        return long_;
    }

    public IntExtractor int_() {
        return int_;
    }

    public CharExtractor char_() {
        return char_;
    }

    public ShortExtractor short_() {
        return short_;
    }

    public ByteExtractor byte_() {
        return byte_;
    }

    public BooleanExtractor boolean_() {
        return boolean_;
    }

    public FloatExtractor float_() {
        return float_;
    }

    public DoubleExtractor double_() {
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

    public PointExtractor point() {
        return point;
    }

    public PointArrayExtractor pointArray() {
        return pointArray;
    }

    public DateExtractor date() {
        return date;
    }

    public DateArrayExtractor dateArray() {
        return dateArray;
    }

    public TimeExtractor time() {
        return time;
    }

    public TimeArrayExtractor timeArray() {
        return timeArray;
    }

    public DateTimeExtractor dateTime() {
        return dateTime;
    }

    public DateTimeArrayExtractor dateTimeArray() {
        return dateTimeArray;
    }

    public LocalTimeExtractor localTime() {
        return localTime;
    }

    public LocalTimeArrayExtractor localTimeArray() {
        return localTimeArray;
    }

    public LocalDateTimeExtractor localDateTime() {
        return localDateTime;
    }

    public LocalDateTimeArrayExtractor localDateTimeArray() {
        return localDateTimeArray;
    }

    public DurationExtractor duration() {
        return duration;
    }

    public TextValueExtractor textValue() {
        return textValue;
    }

    public DurationArrayExtractor durationArray() {
        return durationArray;
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

    public static class StringExtractor extends AbstractExtractor<String> {
        private final boolean emptyStringsAsNull;

        public StringExtractor(boolean emptyStringsAsNull) {
            super(String.class.getSimpleName());
            this.emptyStringsAsNull = emptyStringsAsNull;
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

    public static class LongExtractor extends AbstractExtractor<Long> {
        LongExtractor() {
            super(Long.TYPE.getSimpleName());
        }

        @Override
        public Long extract(char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            return length == 0 ? null : extractLong(data, offset, length);
        }
    }

    public static class IntExtractor extends AbstractExtractor<Integer> {
        IntExtractor(LongExtractor longExtractor) {
            super(Integer.TYPE.toString(), longExtractor);
        }

        @Override
        public Integer extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            return length == 0 ? null : safeCastLongToInt(extractLong(data, offset, length));
        }
    }

    public static class ShortExtractor extends AbstractExtractor<Short> {
        ShortExtractor(LongExtractor longExtractor) {
            super(Short.TYPE.getSimpleName(), longExtractor);
        }

        @Override
        public Short extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            return length == 0 ? null : safeCastLongToShort(extractLong(data, offset, length));
        }
    }

    public static class ByteExtractor extends AbstractExtractor<Byte> {
        ByteExtractor(LongExtractor longExtractor) {
            super(Byte.TYPE.getSimpleName(), longExtractor);
        }

        @Override
        public Byte extract(char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            return length == 0 ? null : safeCastLongToByte(extractLong(data, offset, length));
        }
    }

    private static final char[] BOOLEAN_MATCH;

    static {
        BOOLEAN_MATCH = new char[Boolean.TRUE.toString().length()];
        Boolean.TRUE.toString().getChars(0, BOOLEAN_MATCH.length, BOOLEAN_MATCH, 0);
    }

    public static class BooleanExtractor extends AbstractExtractor<Boolean> {
        BooleanExtractor() {
            super(Boolean.TYPE.getSimpleName());
        }

        @Override
        public Boolean extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            return length == 0 ? null : extractBoolean(data, offset, length);
        }
    }

    public static class CharExtractor extends AbstractExtractor<Character> {
        CharExtractor(StringExtractor stringExtractor) {
            super(Character.TYPE.getSimpleName(), stringExtractor);
        }

        @Override
        public Character extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            if (length > 1) {
                throw new IllegalStateException("Was told to extract a character, but length:" + length);
            }
            return length == 0 ? null : data[offset];
        }
    }

    public static class FloatExtractor extends AbstractExtractor<Float> {
        FloatExtractor(DoubleExtractor doubleExtractor) {
            super(Float.TYPE.getSimpleName(), doubleExtractor);
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
    }

    public static class DoubleExtractor extends AbstractExtractor<Double> {
        DoubleExtractor() {
            super(Double.TYPE.getSimpleName());
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
    }

    private abstract static class ArrayExtractor<T> extends AbstractExtractor<T> {
        protected final char arrayDelimiter;

        ArrayExtractor(char arrayDelimiter, String componentTypeName) {
            super(componentTypeName + "[]");
            this.arrayDelimiter = arrayDelimiter;
        }

        protected int charsToNextDelimiter(char[] data, int offset, int length) {
            for (int i = 0; i < length; i++) {
                if (data[offset + i] == arrayDelimiter) {
                    return i;
                }
            }
            return length;
        }

        protected int numberOfValues(char[] data, int offset, int length) {
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

    private static class StringArrayExtractor extends ArrayExtractor<String[]> {
        private final boolean trimStrings;

        StringArrayExtractor(char arrayDelimiter, boolean trimStrings) {
            super(arrayDelimiter, String.class.getSimpleName());
            this.trimStrings = trimStrings;
        }

        @Override
        public String[] extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            String[] value = numberOfValues > 0 ? new String[numberOfValues] : EMPTY_STRING_ARRAY;
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                value[arrayIndex] = new String(data, offset + charIndex, numberOfChars);
                if (trimStrings) {
                    value[arrayIndex] = value[arrayIndex].trim();
                }
                charIndex += numberOfChars;
            }
            return value;
        }
    }

    private static class ByteArrayExtractor extends ArrayExtractor<byte[]> {
        ByteArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, Byte.TYPE.getSimpleName());
        }

        @Override
        public byte[] extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            byte[] value = numberOfValues > 0 ? new byte[numberOfValues] : EMPTY_BYTE_ARRAY;
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                value[arrayIndex] = safeCastLongToByte(extractLong(data, offset + charIndex, numberOfChars));
                charIndex += numberOfChars;
            }
            return value;
        }
    }

    private static class ShortArrayExtractor extends ArrayExtractor<short[]> {
        private static final short[] EMPTY = new short[0];

        ShortArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, Short.TYPE.getSimpleName());
        }

        @Override
        public short[] extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            short[] value = numberOfValues > 0 ? new short[numberOfValues] : EMPTY;
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                value[arrayIndex] = safeCastLongToShort(extractLong(data, offset + charIndex, numberOfChars));
                charIndex += numberOfChars;
            }
            return value;
        }
    }

    private static class IntArrayExtractor extends ArrayExtractor<int[]> {
        IntArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, Integer.TYPE.getSimpleName());
        }

        @Override
        public int[] extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            int[] value = numberOfValues > 0 ? new int[numberOfValues] : EMPTY_INT_ARRAY;
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                value[arrayIndex] = safeCastLongToInt(extractLong(data, offset + charIndex, numberOfChars));
                charIndex += numberOfChars;
            }
            return value;
        }
    }

    private static class LongArrayExtractor extends ArrayExtractor<long[]> {
        LongArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, Long.TYPE.getSimpleName());
        }

        @Override
        public long[] extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            long[] value = numberOfValues > 0 ? new long[numberOfValues] : EMPTY_LONG_ARRAY;
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                value[arrayIndex] = extractLong(data, offset + charIndex, numberOfChars);
                charIndex += numberOfChars;
            }
            return value;
        }
    }

    private static class FloatArrayExtractor extends ArrayExtractor<float[]> {
        private static final float[] EMPTY = new float[0];

        FloatArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, Float.TYPE.getSimpleName());
        }

        @Override
        public float[] extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            float[] value = numberOfValues > 0 ? new float[numberOfValues] : EMPTY;
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                // TODO Figure out a way to do this conversion without round tripping to String
                // parseFloat automatically handles leading/trailing whitespace so no need for us to do it
                value[arrayIndex] = Float.parseFloat(String.valueOf(data, offset + charIndex, numberOfChars));
                charIndex += numberOfChars;
            }
            return value;
        }
    }

    private static class DoubleArrayExtractor extends ArrayExtractor<double[]> {

        DoubleArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, Double.TYPE.getSimpleName());
        }

        @Override
        public double[] extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            double[] value = numberOfValues > 0 ? new double[numberOfValues] : EMPTY_DOUBLE_ARRAY;
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                // TODO Figure out a way to do this conversion without round tripping to String
                // parseDouble automatically handles leading/trailing whitespace so no need for us to do it
                value[arrayIndex] = Double.parseDouble(String.valueOf(data, offset + charIndex, numberOfChars));
                charIndex += numberOfChars;
            }
            return value;
        }
    }

    private static class BooleanArrayExtractor extends ArrayExtractor<boolean[]> {
        private static final boolean[] EMPTY = new boolean[0];

        BooleanArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, Boolean.TYPE.getSimpleName());
        }

        @Override
        public boolean[] extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            boolean[] value = numberOfValues > 0 ? new boolean[numberOfValues] : EMPTY;
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                value[arrayIndex] = extractBoolean(data, offset + charIndex, numberOfChars);
                charIndex += numberOfChars;
            }
            return value;
        }
    }

    private abstract static class ArrayAnyValueExtractor<T extends ArrayValue> extends ArrayExtractor<T> {
        ArrayAnyValueExtractor(char arrayDelimiter, String componentTypeName) {
            super(arrayDelimiter, componentTypeName);
        }

        @Override
        public boolean isEmpty(Object value) {
            return super.isEmpty(value) || ((ArrayValue) value).length() == 0;
        }
    }

    public static class PointExtractor extends AbstractExtractor<PointValue> {
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
    }

    public static class PointArrayExtractor extends ArrayAnyValueExtractor<PointArray> {
        private static final PointArray EMPTY = Values.pointArray(new Point[0]);

        PointArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, PointExtractor.NAME);
        }

        @Override
        public PointArray extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            if (numberOfValues <= 0) {
                return EMPTY;
            }
            var localValue = new PointValue[numberOfValues];
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                localValue[arrayIndex] =
                        PointValue.parse(CharBuffer.wrap(data, offset + charIndex, numberOfChars), optionalData);
                charIndex += numberOfChars;
            }
            return Values.pointArray(localValue);
        }
    }

    public static class DateExtractor extends AbstractExtractor<DateValue> {
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
    }

    public static class DateArrayExtractor extends ArrayAnyValueExtractor<DateArray> {
        private static final DateArray EMPTY = Values.dateArray(new LocalDate[0]);

        DateArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, DateExtractor.NAME);
        }

        @Override
        public DateArray extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            if (numberOfValues <= 0) {
                return EMPTY;
            }

            var localValue = new LocalDate[numberOfValues];
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                localValue[arrayIndex] = DateValue.parse(CharBuffer.wrap(data, offset + charIndex, numberOfChars))
                        .asObjectCopy();
                charIndex += numberOfChars;
            }
            return Values.dateArray(localValue);
        }
    }

    public static class TimeExtractor extends AbstractExtractor<TimeValue> {
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
    }

    public static class TimeArrayExtractor extends ArrayAnyValueExtractor<TimeArray> {
        private static final TimeArray EMPTY = Values.timeArray(new OffsetTime[0]);

        private final Supplier<ZoneId> defaultTimeZone;

        TimeArrayExtractor(char arrayDelimiter, Supplier<ZoneId> defaultTimeZone) {
            super(arrayDelimiter, TimeExtractor.NAME);
            this.defaultTimeZone = defaultTimeZone;
        }

        @Override
        public TimeArray extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            if (numberOfValues <= 0) {
                return EMPTY;
            }

            var localValue = new OffsetTime[numberOfValues];
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                localValue[arrayIndex] = TimeValue.parse(
                                CharBuffer.wrap(data, offset + charIndex, numberOfChars), defaultTimeZone, optionalData)
                        .asObjectCopy();
                charIndex += numberOfChars;
            }
            return Values.timeArray(localValue);
        }
    }

    public static class DateTimeExtractor extends AbstractExtractor<DateTimeValue> {
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
    }

    public static class DateTimeArrayExtractor extends ArrayAnyValueExtractor<DateTimeArray> {
        private static final DateTimeArray EMPTY = Values.dateTimeArray(new ZonedDateTime[0]);

        private final Supplier<ZoneId> defaultTimeZone;

        DateTimeArrayExtractor(char arrayDelimiter, Supplier<ZoneId> defaultTimeZone) {
            super(arrayDelimiter, DateTimeExtractor.NAME);
            this.defaultTimeZone = defaultTimeZone;
        }

        @Override
        public DateTimeArray extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            if (numberOfValues <= 0) {
                return EMPTY;
            }

            var localValue = new ZonedDateTime[numberOfValues];
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                localValue[arrayIndex] = DateTimeValue.parse(
                                CharBuffer.wrap(data, offset + charIndex, numberOfChars), defaultTimeZone, optionalData)
                        .asObjectCopy();
                charIndex += numberOfChars;
            }
            return Values.dateTimeArray(localValue);
        }
    }

    public static class LocalTimeExtractor extends AbstractExtractor<LocalTimeValue> {
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
    }

    public static class LocalTimeArrayExtractor extends ArrayAnyValueExtractor<LocalTimeArray> {
        private static final LocalTimeArray EMPTY = Values.localTimeArray(new LocalTime[0]);

        LocalTimeArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, LocalTimeExtractor.NAME);
        }

        @Override
        public LocalTimeArray extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            if (numberOfValues <= 0) {
                return EMPTY;
            }

            var localValue = new LocalTime[numberOfValues];
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                localValue[arrayIndex] = LocalTimeValue.parse(CharBuffer.wrap(data, offset + charIndex, numberOfChars))
                        .asObjectCopy();
                charIndex += numberOfChars;
            }
            return Values.localTimeArray(localValue);
        }
    }

    public static class LocalDateTimeExtractor extends AbstractExtractor<LocalDateTimeValue> {
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
    }

    public static class LocalDateTimeArrayExtractor extends ArrayAnyValueExtractor<LocalDateTimeArray> {
        private static final LocalDateTimeArray EMPTY = Values.localDateTimeArray(new LocalDateTime[0]);

        LocalDateTimeArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, LocalDateTimeExtractor.NAME);
        }

        @Override
        public LocalDateTimeArray extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            if (numberOfValues <= 0) {
                return EMPTY;
            }

            var localValue = new LocalDateTime[numberOfValues];
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                localValue[arrayIndex] = LocalDateTimeValue.parse(
                                CharBuffer.wrap(data, offset + charIndex, numberOfChars))
                        .asObjectCopy();
                charIndex += numberOfChars;
            }
            return Values.localDateTimeArray(localValue);
        }
    }

    public static class DurationExtractor extends AbstractExtractor<DurationValue> {
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
    }

    public static class TextValueExtractor extends AbstractExtractor<Value> {
        public static final String NAME = "TextValue";

        private final boolean emptyStringsAsNull;

        TextValueExtractor(boolean emptyStringsAsNull) {
            super(NAME);
            this.emptyStringsAsNull = emptyStringsAsNull;
        }

        @Override
        public Value extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            if (length == 0 && (!hadQuotes || emptyStringsAsNull)) {
                return Values.NO_VALUE;
            }
            return Values.utf8Value(new String(data, offset, length));
        }
    }

    public static class DurationArrayExtractor extends ArrayAnyValueExtractor<DurationArray> {
        private static final DurationArray EMPTY = Values.durationArray(new DurationValue[0]);

        DurationArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, DurationExtractor.NAME);
        }

        @Override
        public DurationArray extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            int numberOfValues = numberOfValues(data, offset, length);
            if (numberOfValues <= 0) {
                return EMPTY;
            }

            var localValue = new DurationValue[numberOfValues];
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
                localValue[arrayIndex] = DurationValue.parse(CharBuffer.wrap(data, offset + charIndex, numberOfChars));
                charIndex += numberOfChars;
            }
            return Values.durationArray(localValue);
        }
    }

    private static final Supplier<ZoneId> inUTC = () -> UTC;

    private static long extractLong(char[] data, int originalOffset, int fullLength) {
        long result = 0;
        boolean negate = false;
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

        if (length > 0 && data[offset] == '-') {
            negate = true;
            offset++;
            length--;
        }

        if (length < 1) {
            throw new NumberFormatException(
                    "Not an integer: \"" + String.valueOf(data, originalOffset, fullLength) + "\"");
        }

        try {
            for (int i = 0; i < length; i++) {
                result = result * 10 + digit(data[offset + i]);
            }
        } catch (NumberFormatException ignored) {
            throw new NumberFormatException(
                    "Not an integer: \"" + String.valueOf(data, originalOffset, fullLength) + "\"");
        }

        return negate ? -result : result;
    }

    private static int digit(char ch) {
        int digit = ch - '0';
        if ((digit < 0) || (digit > 9)) {
            throw new NumberFormatException();
        }
        return digit;
    }

    private static final char[] BOOLEAN_TRUE_CHARACTERS;

    static {
        BOOLEAN_TRUE_CHARACTERS = new char[Boolean.TRUE.toString().length()];
        Boolean.TRUE.toString().getChars(0, BOOLEAN_TRUE_CHARACTERS.length, BOOLEAN_TRUE_CHARACTERS, 0);
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

        // See if the rest exactly match "true"
        if (length != BOOLEAN_TRUE_CHARACTERS.length) {
            return false;
        }

        for (int i = 0; i < BOOLEAN_TRUE_CHARACTERS.length && i < length; i++) {
            if (data[offset + i] != BOOLEAN_TRUE_CHARACTERS[i]) {
                return false;
            }
        }

        return true;
    }
}
