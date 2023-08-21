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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.ArrayUtil.array;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.csv.reader.Extractors.IntExtractor;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

class ExtractorsTest {
    @Test
    void shouldExtractStringArray() {
        // GIVEN
        Extractors extractors = new Extractors(',');
        String data = "abcde,fghijkl,mnopq";

        // WHEN
        @SuppressWarnings("unchecked")
        Extractor<String[]> extractor = (Extractor<String[]>) extractors.valueOf("STRING[]");
        var extractedValue = extractor.extract(data.toCharArray(), 0, data.length(), false);

        // THEN
        assertArrayEquals(new String[] {"abcde", "fghijkl", "mnopq"}, extractedValue);
    }

    @Test
    void shouldExtractLongArray() {
        // GIVEN
        Extractors extractors = new Extractors(',');
        long[] longData = new long[] {123, 4567, 987654321};
        String data = toString(longData, ',');

        // WHEN
        @SuppressWarnings("unchecked")
        Extractor<long[]> extractor = (Extractor<long[]>) extractors.valueOf("long[]");
        var extractedValue = extractor.extract(data.toCharArray(), 0, data.length(), false);

        // THEN
        assertArrayEquals(longData, extractedValue);
    }

    @Test
    void shouldExtractBooleanArray() {
        // GIVEN
        Extractors extractors = new Extractors(',');
        boolean[] booleanData = new boolean[] {true, false, true};
        String data = toString(booleanData, ',');

        // WHEN
        Extractor<boolean[]> extractor = extractors.booleanArray();
        var extractedValue = extractor.extract(data.toCharArray(), 0, data.length(), false);

        // THEN
        assertBooleanArrayEquals(booleanData, extractedValue);
    }

    @Test
    void shouldExtractDoubleArray() {
        // GIVEN
        Extractors extractors = new Extractors(',');
        double[] doubleData = new double[] {123.123, 4567.4567, 987654321.0987};
        String data = toString(doubleData, ',');

        // WHEN
        Extractor<double[]> extractor = extractors.doubleArray();
        var extractedValue = extractor.extract(data.toCharArray(), 0, data.length(), false);

        // THEN
        assertArrayEquals(doubleData, extractedValue, 0.001);
    }

    @Test
    void shouldFailExtractingLongArrayWhereAnyValueIsEmpty() {
        // GIVEN
        Extractors extractors = new Extractors();
        long[] longData = new long[] {112233, 4455, 66778899};
        String data = toString(longData, ';') + ";";

        // WHEN extracting long[] from "<number>;<number>...;" i.e. ending with a delimiter
        assertThrows(
                NumberFormatException.class,
                () -> extractors.longArray().extract(data.toCharArray(), 0, data.length(), false));
    }

    @Test
    void shouldFailExtractingLongArrayWhereAnyValueIsntReallyANumber() {
        // GIVEN
        Extractors extractors = new Extractors();

        // WHEN extracting long[] from "<number>;<number>...;" i.e. ending with a delimiter
        String data = "123;456;abc;789";
        assertThrows(
                NumberFormatException.class,
                () -> extractors.valueOf("long[]").extract(data.toCharArray(), 0, data.length(), false));
    }

    @Test
    void shouldExtractPoint() {
        // GIVEN
        Extractors extractors = new Extractors(',');
        PointValue value = Values.pointValue(CoordinateReferenceSystem.WGS_84, 13.2, 56.7);

        // WHEN
        char[] asChars = "Point{latitude: 56.7, longitude: 13.2}".toCharArray();
        Extractors.PointExtractor extractor = extractors.point();
        String headerInfo = "{crs:WGS-84}";
        var extractedValue =
                extractor.extract(asChars, 0, asChars.length, false, PointValue.parseHeaderInformation(headerInfo));

        // THEN
        assertEquals(value, extractedValue);
    }

    @Test
    void shouldExtractNegativeInt() {
        // GIVEN
        Extractors extractors = new Extractors(',');
        int value = -1234567;

        // WHEN
        char[] asChars = String.valueOf(value).toCharArray();
        IntExtractor extractor = extractors.int_();
        var extractedValue = extractor.extract(asChars, 0, asChars.length, false);

        // THEN
        assertEquals(value, extractedValue);
    }

    @Test
    void shouldExtractEmptyStringForEmptyArrayString() {
        // GIVEN
        Extractors extractors = new Extractors(',');
        String value = "";

        // WHEN
        Extractor<String[]> extractor = extractors.stringArray();
        var extractedValue = extractor.extract(value.toCharArray(), 0, value.length(), false);

        // THEN
        assertEquals(0, extractedValue.length);
    }

    @Test
    void shouldExtractEmptyLongArrayForEmptyArrayString() {
        // GIVEN
        Extractors extractors = new Extractors(',');
        String value = "";

        // WHEN
        Extractor<long[]> extractor = extractors.longArray();
        var extractedValue = extractor.extract(value.toCharArray(), 0, value.length(), false);

        // THEN
        assertEquals(0, extractedValue.length);
    }

    @Test
    void shouldExtractTwoEmptyStringsForSingleDelimiterInArrayString() {
        // GIVEN
        Extractors extractors = new Extractors(',');
        String value = ",";

        // WHEN
        Extractor<String[]> extractor = extractors.stringArray();
        var extractedValue = extractor.extract(value.toCharArray(), 0, value.length(), false);

        // THEN
        assertArrayEquals(new String[] {"", ""}, extractedValue);
    }

    @Test
    void shouldExtractEmptyStringForEmptyQuotedString() {
        // GIVEN
        Extractors extractors = new Extractors(',');
        String value = "";

        // WHEN
        Extractor<String> extractor = extractors.string();
        var extractedValue = extractor.extract(value.toCharArray(), 0, value.length(), true);

        // THEN
        assertEquals("", extractedValue);
    }

    @Test
    void shouldExtractNullForEmptyQuotedStringIfConfiguredTo() {
        // GIVEN
        Extractors extractors = new Extractors(';', true);
        Extractor<String> extractor = extractors.string();

        // WHEN
        var extractedValue = extractor.extract(new char[0], 0, 0, true);

        // THEN
        assertNull(extractedValue);
    }

    @Test
    void shouldTrimStringArrayIfConfiguredTo() {
        // GIVEN
        Extractors extractors = new Extractors(';', true, true);
        String value = "ab;cd ; ef; gh ";

        // WHEN
        char[] asChars = value.toCharArray();
        Extractor<String[]> extractor = extractors.stringArray();
        var extractedValue = extractor.extract(asChars, 0, asChars.length, true);

        // THEN
        assertArrayEquals(new String[] {"ab", "cd", "ef", "gh"}, extractedValue);
    }

    @Test
    void shouldNotTrimStringIfNotConfiguredTo() {
        // GIVEN
        Extractors extractors = new Extractors(';', true, false);
        String value = "ab;cd ; ef; gh ";

        // WHEN
        char[] asChars = value.toCharArray();
        Extractor<String[]> extractor = extractors.stringArray();
        var extractedValue = extractor.extract(asChars, 0, asChars.length, true);

        // THEN
        assertArrayEquals(new String[] {"ab", "cd ", " ef", " gh "}, extractedValue);
    }

    @Test
    void shouldNormalizeNumberTypes() {
        Extractors extractors = new Extractors();
        assertSame(extractors.long_(), extractors.byte_().normalize());
        assertSame(extractors.long_(), extractors.short_().normalize());
        assertSame(extractors.long_(), extractors.int_().normalize());
        assertSame(extractors.double_(), extractors.float_().normalize());
    }

    @Test
    void shouldExtractPointArray() {
        // GIVEN
        Extractors extractors = new Extractors();
        char[] asChars = "{latitude: 56.7, longitude: 13.2};{latitude: 0.7, longitude: 0.25}".toCharArray();
        var extractor = extractors.pointArray();
        String headerInfo = "{crs:WGS-84}";

        // WHEN
        var extractedValue =
                extractor.extract(asChars, 0, asChars.length, false, PointValue.parseHeaderInformation(headerInfo));

        var value = Values.pointArray(array(
                Values.pointValue(CoordinateReferenceSystem.WGS_84, 13.2, 56.7),
                Values.pointValue(CoordinateReferenceSystem.WGS_84, 0.25, 0.7)));
        // THEN
        assertEquals(value, extractedValue);
    }

    @Test
    void shouldExtractDateArray() {
        // GIVEN
        Extractors extractors = new Extractors();
        var asChars = "1985-4-20;2030-12-12".toCharArray();
        var extractor = extractors.dateArray();

        // WHEN
        var extractedValue = extractor.extract(asChars, 0, asChars.length, false);

        var value = Values.dateArray(array(LocalDate.of(1985, 4, 20), LocalDate.of(2030, 12, 12)));
        // THEN
        assertEquals(value, extractedValue);
    }

    @Test
    void shouldExtractTimeArray() {
        // GIVEN
        Extractors extractors = new Extractors();
        var asChars = "2:41:34;18:3:51".toCharArray();
        var extractor = extractors.timeArray();
        String headerInformation = "{timezone:+10:00}";

        // WHEN
        var extractedValue = extractor.extract(
                asChars, 0, asChars.length, false, TimeValue.parseHeaderInformation(headerInformation));

        var value = Values.timeArray(array(
                OffsetTime.of(2, 41, 34, 0, ZoneOffset.ofHours(10)),
                OffsetTime.of(18, 3, 51, 0, ZoneOffset.ofHours(10))));
        // THEN
        assertEquals(value, extractedValue);
    }

    @Test
    void shouldExtractDateTimeArray() {
        // GIVEN
        Extractors extractors = new Extractors();
        var asChars = "1985-4-20T2:41:34;2030-12-12T18:3:51".toCharArray();
        var extractor = extractors.dateTimeArray();
        String headerInformation = "{timezone:+10:00}";

        // WHEN
        var extractedValue = extractor.extract(
                asChars, 0, asChars.length, false, TimeValue.parseHeaderInformation(headerInformation));

        var value = Values.dateTimeArray(array(
                ZonedDateTime.of(1985, 4, 20, 2, 41, 34, 0, ZoneOffset.ofHours(10)),
                ZonedDateTime.of(2030, 12, 12, 18, 3, 51, 0, ZoneOffset.ofHours(10))));
        // THEN
        assertEquals(value, extractedValue);
    }

    @Test
    void shouldExtractLocalTimeArray() {
        // GIVEN
        Extractors extractors = new Extractors();
        var asChars = "2:41:34;18:3:51".toCharArray();
        var extractor = extractors.localTimeArray();

        // WHEN
        var extractedValue = extractor.extract(asChars, 0, asChars.length, false);

        var value = Values.localTimeArray(array(LocalTime.of(2, 41, 34, 0), LocalTime.of(18, 3, 51, 0)));
        // THEN
        assertEquals(value, extractedValue);
    }

    @Test
    void shouldExtractLocalDateTimeArray() {
        // GIVEN
        Extractors extractors = new Extractors();
        var asChars = "1985-4-20T2:41:34;2030-12-12T18:3:51".toCharArray();
        var extractor = extractors.localDateTimeArray();
        String headerInformation = "{timezone:+10:00}";

        // WHEN
        var extractedValue = extractor.extract(
                asChars, 0, asChars.length, false, TimeValue.parseHeaderInformation(headerInformation));

        var value = Values.localDateTimeArray(
                array(LocalDateTime.of(1985, 4, 20, 2, 41, 34, 0), LocalDateTime.of(2030, 12, 12, 18, 3, 51, 0)));
        // THEN
        assertEquals(value, extractedValue);
    }

    @Test
    void shouldExtractDurationArray() {
        // GIVEN
        Extractors extractors = new Extractors();
        var asChars = "PT60S;PT2H".toCharArray();
        var extractor = extractors.durationArray();

        // WHEN
        var extractedValue = extractor.extract(asChars, 0, asChars.length, false);

        var value = Values.durationArray(array(Duration.of(60, ChronoUnit.SECONDS), Duration.of(2, ChronoUnit.HOURS)));
        // THEN
        assertEquals(value, extractedValue);
    }

    @MethodSource("extractorTypes")
    @ParameterizedTest
    <T> void shouldExtractEmptyField(Function<Extractors, Extractor<T>> extractorSelector) {
        // given
        var extractors = new Extractors();
        var extractor = extractorSelector.apply(extractors);

        // when
        var value = extractor.extract(new char[0], 0, 0, false);

        // then
        assertThat(extractor.isEmpty(value)).isTrue();
    }

    public static Stream<Arguments> extractorTypes() {
        List<Arguments> types = new ArrayList<>();
        types.add(extractorType(Extractors::boolean_));
        types.add(extractorType(Extractors::booleanArray));
        types.add(extractorType(Extractors::byte_));
        types.add(extractorType(Extractors::byteArray));
        types.add(extractorType(Extractors::short_));
        types.add(extractorType(Extractors::shortArray));
        types.add(extractorType(Extractors::int_));
        types.add(extractorType(Extractors::intArray));
        types.add(extractorType(Extractors::long_));
        types.add(extractorType(Extractors::longArray));
        types.add(extractorType(Extractors::float_));
        types.add(extractorType(Extractors::floatArray));
        types.add(extractorType(Extractors::double_));
        types.add(extractorType(Extractors::doubleArray));
        types.add(extractorType(Extractors::char_));
        types.add(extractorType(Extractors::string));
        types.add(extractorType(Extractors::stringArray));
        types.add(extractorType(Extractors::textValue));
        types.add(extractorType(Extractors::date));
        types.add(extractorType(Extractors::dateArray));
        types.add(extractorType(Extractors::time));
        types.add(extractorType(Extractors::timeArray));
        types.add(extractorType(Extractors::dateTime));
        types.add(extractorType(Extractors::dateTimeArray));
        types.add(extractorType(Extractors::localDateTime));
        types.add(extractorType(Extractors::localDateTimeArray));
        types.add(extractorType(Extractors::localTime));
        types.add(extractorType(Extractors::localTimeArray));
        types.add(extractorType(Extractors::point));
        types.add(extractorType(Extractors::pointArray));
        types.add(extractorType(Extractors::duration));
        types.add(extractorType(Extractors::durationArray));
        return types.stream();
    }

    private static Arguments extractorType(Function<Extractors, Extractor<?>> selector) {
        return Arguments.of(selector);
    }

    private static String toString(long[] values, char delimiter) {
        StringBuilder builder = new StringBuilder();
        for (long value : values) {
            builder.append(builder.length() > 0 ? delimiter : "").append(value);
        }
        return builder.toString();
    }

    private static String toString(double[] values, char delimiter) {
        StringBuilder builder = new StringBuilder();
        for (double value : values) {
            builder.append(builder.length() > 0 ? delimiter : "").append(value);
        }
        return builder.toString();
    }

    private static String toString(boolean[] values, char delimiter) {
        StringBuilder builder = new StringBuilder();
        for (boolean value : values) {
            builder.append(builder.length() > 0 ? delimiter : "").append(value);
        }
        return builder.toString();
    }

    private static void assertBooleanArrayEquals(boolean[] expected, boolean[] values) {
        assertEquals(expected.length, values.length, "Array lengths differ");
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], values[i], "Item " + i + " differs");
        }
    }
}
