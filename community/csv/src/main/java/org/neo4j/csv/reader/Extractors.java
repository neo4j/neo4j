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
 * {@link Extractor#name() name} value is used as key for lookup in {@link #valueOf(String)}.
 */
public final class Extractors {
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

    public Extractor<Value> textValue() {
        return textValue;
    }

    public Extractor<DurationArray> durationArray() {
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

    private static final class StringExtractor extends AbstractExtractor<String> {
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

    private static final class LongExtractor extends AbstractExtractor<Long> {
        LongExtractor() {
            super(long.class.getSimpleName());
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
    }

    private static final class ByteExtractor extends AbstractExtractor<Byte> {
        ByteExtractor(LongExtractor longExtractor) {
            super(byte.class.getSimpleName(), longExtractor);
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

    private static final class BooleanExtractor extends AbstractExtractor<Boolean> {
        BooleanExtractor() {
            super(boolean.class.getSimpleName());
        }

        @Override
        public Boolean extract(
                char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData) {
            return length == 0 ? null : extractBoolean(data, offset, length);
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

            E values = createInternalArray(numberOfValues);
            for (int arrayIndex = 0, charIndex = 0; arrayIndex < numberOfValues; arrayIndex++, charIndex++) {
                int numberOfChars = charsToNextDelimiter(data, offset + charIndex, length - charIndex);
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

        private int charsToNextDelimiter(char[] data, int offset, int length) {
            for (int i = 0; i < length; i++) {
                if (data[offset + i] == arrayDelimiter) {
                    return i;
                }
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
    }

    private static final class ByteArrayExtractor extends ArrayExtractor<byte[], byte[]> {
        ByteArrayExtractor(char arrayDelimiter) {
            super(arrayDelimiter, byte[].class);
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
    }

    private static final class TextValueExtractor extends AbstractExtractor<Value> {
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
