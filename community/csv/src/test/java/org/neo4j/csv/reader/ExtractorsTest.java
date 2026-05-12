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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Named.named;
import static org.neo4j.internal.helpers.ArrayUtil.array;

import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphdb.Vector;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.values.storable.CSVHeaderInformation;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

class ExtractorsTest {

    @Test
    void shouldFailExtractingLongArrayWhereAnyValueIsEmpty() {
        // GIVEN
        Extractors extractors = new Extractors();
        String data = "112233;4455;;66778899";

        // THEN
        assertThatThrownBy(
                        () -> extractors.longArray().extract(data.toCharArray(), 0, data.length(), false),
                        "fails when a value in the middle of the array is empty")
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void shouldFailExtractingLongArrayWhereLastValueIsEmpty() {
        // GIVEN
        Extractors extractors = new Extractors();
        String data = "112233;4455;66778899;";

        // THEN
        assertThatThrownBy(
                        () -> extractors.longArray().extract(data.toCharArray(), 0, data.length(), false),
                        "fails when the last value of the array is empty")
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void shouldFailExtractingLongArrayWhereAnyValueIsntReallyANumber() {
        // GIVEN
        Extractors extractors = new Extractors();
        String data = "123;456;abc;789";

        // THEN
        assertThatThrownBy(
                        () -> extractors.longArray().extract(data.toCharArray(), 0, data.length(), false),
                        "fails when parsing invalid data type - word when number was expected")
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void shouldFailExtractingInt64VectorWhereAnyValueIsEmpty() {
        // GIVEN
        Extractors extractors = new Extractors();
        String data = "112233;4455;;66778899";

        // THEN
        assertThatThrownBy(
                        () -> extractors.int64Vector().extract(data.toCharArray(), 0, data.length(), false),
                        "fails when a value in the middle of the array is empty")
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void shouldFailExtractingInt64VectorWhereLastValueIsEmpty() {
        // GIVEN
        Extractors extractors = new Extractors();
        String data = "112233;4455;66778899;";

        // THEN
        assertThatThrownBy(
                        () -> extractors.int64Vector().extract(data.toCharArray(), 0, data.length(), false),
                        "fails when the last value of the array is empty")
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void shouldFailExtractingInt64VectorWhereAnyValueIsntReallyANumber() {
        // GIVEN
        Extractors extractors = new Extractors();
        String data = "123;456;abc;789";

        // THEN
        assertThatThrownBy(
                        () -> extractors.int64Vector().extract(data.toCharArray(), 0, data.length(), false),
                        "fails when parsing invalid data type - word when number was expected")
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void shouldExtractNullForEmptyQuotedStringIfConfiguredTo() {
        // GIVEN
        Extractors extractors = new Extractors(';', ';', true);
        Extractor<String> extractor = extractors.string();

        // WHEN
        var extractedValue = extractor.extract(new char[0], 0, 0, true);

        // THEN
        assertThat(extractedValue).isNull();
    }

    @Test
    void shouldTrimStringArrayIfConfiguredTo() {
        // GIVEN
        Extractors extractors = new Extractors(';', ';', true, true);
        String value = "ab;cd ; ef; gh ";

        // WHEN
        char[] asChars = value.toCharArray();
        Extractor<String[]> extractor = extractors.stringArray();
        var extractedValue = extractor.extract(asChars, 0, asChars.length, true);

        // THEN
        assertThat(extractedValue).containsExactly("ab", "cd", "ef", "gh");
    }

    @Test
    void shouldNotTrimStringIfNotConfiguredTo() {
        // GIVEN
        Extractors extractors = new Extractors(';', ';', true, false);
        String value = "ab;cd ; ef; gh ";

        // WHEN
        char[] asChars = value.toCharArray();
        Extractor<String[]> extractor = extractors.stringArray();
        var extractedValue = extractor.extract(asChars, 0, asChars.length, true);

        // THEN
        assertThat(extractedValue).containsExactly("ab", "cd ", " ef", " gh ");
    }

    static Stream<Arguments> vectorCSVHeaderInformationBehavesAsExpected() {
        return Stream.of(
                Arguments.of(Map.of("coordinateType", "byte", "dimensions", "3"), Vector.CoordinateType.INTEGER8, 3),
                Arguments.of(Map.of("coordinatetype", "shorT", "dimensions", "4"), Vector.CoordinateType.INTEGER16, 4),
                Arguments.of(Map.of("coordinatetype", "INT", "dimensions", "3"), Vector.CoordinateType.INTEGER32, 3),
                Arguments.of(Map.of("CoordinateType", "long", "dimensions", "3"), Vector.CoordinateType.INTEGER64, 3),
                Arguments.of(Map.of("cOordinatetYpe", "FloAt", "dimenSions", "3"), Vector.CoordinateType.FLOAT32, 3),
                Arguments.of(Map.of("coordinateType", "double", "dimensions", "3"), Vector.CoordinateType.FLOAT64, 3),
                Arguments.of(
                        Map.of("coordinateType", "\"double\"", "dimensions", "3"), Vector.CoordinateType.FLOAT64, 3),
                Arguments.of(
                        Map.of("coordinateType", "byte", "dimensions", "3", "ignoredKey", "foo"),
                        Vector.CoordinateType.INTEGER8,
                        3));
    }

    @ParameterizedTest
    @MethodSource
    void vectorCSVHeaderInformationBehavesAsExpected(
            Map<String, String> options, Vector.CoordinateType expectedCoordinateType, int expectedDimensions) {
        final var headerInformation = VectorExtractor.parseHeaderInformation(options);
        assertThat(headerInformation.getCoordinateType()).isEqualTo(expectedCoordinateType);
        assertThat(headerInformation.getDimensions()).isEqualTo(expectedDimensions);
    }

    @Test
    void vectorCSVHeaderInformationComplainsOnUnknownCoordinateType() {
        assertThatThrownBy(() ->
                        VectorExtractor.parseHeaderInformation(Map.of("coordinateType", "pyte", "dimensions", "3")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pyte is not a valid coordinate type.");
    }

    @Test
    void vectorCSVHeaderInformationComplainsOnNonIntegerDimensions() {
        assertThatThrownBy(() ->
                        VectorExtractor.parseHeaderInformation(Map.of("coordinateType", "byte", "dimensions", "three")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("three is not a valid value for dimensions.");
    }

    @MethodSource("extractorTypes")
    @ParameterizedTest(name = "{0}")
    void shouldExtractValue(ExtractorTypeTestCase testCase) {
        var extractor = testCase.extractorSelector.apply(new Extractors(';', '§'));
        var input = testCase.input;
        assertExtractedValue(extractor, testCase.optionalCSVHeaders, input, testCase.expectedOutput);
    }

    @MethodSource("strippableArrayExtractorArguments")
    @ParameterizedTest(name = "{0}")
    void shouldExtractArrayValue(ExtractorTypeTestCase testCase) {
        var extractor = testCase.extractorSelector.apply(new Extractors(';', '§'));
        var input = testCase.input;
        assertExtractedValue(extractor, testCase.optionalCSVHeaders, input, testCase.expectedOutput);
        // check that single bracket arrays fail
        var badInput = new char[input.length + 1];
        badInput[0] = '[';
        System.arraycopy(input, 0, badInput, 1, input.length);
        assertThatThrownBy(() ->
                        assertExtractedValue(extractor, testCase.optionalCSVHeaders, badInput, testCase.expectedOutput))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Array content expected between '[' and ']' but no terminal ']' character found");

        // check that bracket pairs are stripped
        var paddedInput = new char[input.length + 2];
        paddedInput[0] = '[';
        paddedInput[paddedInput.length - 1] = ']';
        System.arraycopy(input, 0, paddedInput, 1, input.length);
        assertExtractedValue(extractor, testCase.optionalCSVHeaders, paddedInput, testCase.expectedOutput);
    }

    @MethodSource("extractorTypes")
    @ParameterizedTest(name = "{0}")
    void shouldExtractNormalizedValue(ExtractorTypeTestCase testCase) {
        // given
        var extractors = new Extractors(';', '§');
        var normalizedExtractor = testCase.extractorSelector.apply(extractors).normalize();
        var input = testCase.input;

        // when
        var extractedValue = testCase.optionalCSVHeaders != null
                ? normalizedExtractor.extract(input, 0, input.length, false, testCase.optionalCSVHeaders)
                : normalizedExtractor.extract(input, 0, input.length, false);

        // then
        assertThat(extractedValue)
                .as("extracted normalized value is equal to the expected normalized output")
                .isEqualTo(testCase.expectedNormalizedOutput);
    }

    @MethodSource("extractorTypes")
    @ParameterizedTest(name = "{0}")
    void shouldHaveExpectedNormalizationExtractorType(ExtractorTypeTestCase testCase) {
        // given
        var extractors = new Extractors(';', '§');
        var extractor = testCase.extractorSelector.apply(extractors);
        var expectedNormalizedExtractor = testCase.normalizedSelector.apply(extractors);

        // when
        var normalizedExtractor = extractor.normalize();

        // then
        assertThat(normalizedExtractor)
                .as("normalized extractor is the same as the expected one")
                .isInstanceOf(expectedNormalizedExtractor.getClass());
    }

    @MethodSource("extractorTypes")
    @ParameterizedTest(name = "{0}")
    void extractedClassMatchesActualExtractedType(ExtractorTypeTestCase testCase) {
        // given
        var extractors = new Extractors(';', '§');
        var extractor = testCase.extractorSelector.apply(extractors);
        var input = testCase.input;

        // when
        var extractedValue = testCase.optionalCSVHeaders != null
                ? extractor.extract(input, 0, input.length, false, testCase.optionalCSVHeaders)
                : extractor.extract(input, 0, input.length, false);

        // then
        assertThat(extractedValue).isNotNull();
        assertThat(extractedValue).isInstanceOf(box(extractor.extractedClass()));
    }

    private static Class<?> box(Class<?> cls) {
        if (!cls.isPrimitive()) {
            return cls;
        }
        if (cls == int.class) return Integer.class;
        if (cls == long.class) return Long.class;
        if (cls == short.class) return Short.class;
        if (cls == byte.class) return Byte.class;
        if (cls == float.class) return Float.class;
        if (cls == double.class) return Double.class;
        if (cls == boolean.class) return Boolean.class;
        if (cls == char.class) return Character.class;
        throw new IllegalArgumentException("Unknown primitive class: " + cls);
    }

    @MethodSource("extractorTypes")
    @ParameterizedTest(name = "{0}")
    void shouldExtractEmptyField(ExtractorTypeTestCase testCase) {
        // given
        var extractors = new Extractors(';', '§');
        var extractor = testCase.extractorSelector.apply(extractors);

        // when
        var value = extractor.extract(new char[0], 0, 0, false);

        // then
        assertThat(extractor.isEmpty(value)).as("empty input").isTrue();
    }

    @MethodSource("extractorTypes")
    @ParameterizedTest(name = "{0}")
    void shouldHaveCorrectExtractorName(ExtractorTypeTestCase testCase) {
        // given
        var extractors = new Extractors(';', '§');
        var extractor = testCase.extractorSelector.apply(extractors);

        // then
        assertThat(extractor.name()).as("Extractor name as expected").isEqualTo(testCase.expectedExtractorName);
    }

    @MethodSource("numericOverflows")
    @ParameterizedTest(name = "{0}")
    void shouldHandleNumericOverflows(ExtractorTypeTestCase testCase) {
        // given
        var extractors = new Extractors(';', '§');
        var extractor = testCase.extractorSelector.apply(extractors);
        var input = testCase.input;
        assertThatThrownBy(() -> extractor.extract(input, 0, input.length, false))
                .isInstanceOf(ArithmeticException.class)
                .hasMessageContainingAll("Value", "is too big to be represented as", testCase.expectedExtractorName);
    }

    @ParameterizedTest
    @ValueSource(longs = {Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MIN_VALUE + 10})
    void shouldHandleLargeNegatives(long value) {
        var extractors = new Extractors(';', '§');
        var extractor = extractors.long_();
        var input = ("" + value).toCharArray();
        assertThat(extractor.extract(input, 0, input.length, false)).isEqualTo(value);
    }

    @ParameterizedTest
    @CsvSource({
        "true,true",
        "True,true",
        "TRUE,true",
        "tRuE,true",
        "  True  ,true",
        "false,false",
        "False,false",
        "FALSE,false",
        "  False  ,false"
    })
    void shouldExtractBooleanCaseInsensitively(String input, boolean expected) {
        // given
        var extractors = new Extractors();

        // when
        var value = extractors.boolean_().extract(input.toCharArray(), 0, input.length(), false);

        // then
        assertThat(value).isEqualTo(expected);
    }

    private static void assertExtractedValue(
            Extractor<?> extractor, CSVHeaderInformation headerInfo, char[] input, Object expectedOutput) {
        var extractedValue = headerInfo != null
                ? extractor.extract(input, 0, input.length, false, headerInfo)
                : extractor.extract(input, 0, input.length, false);

        // then
        assertThat(extractedValue)
                .as("extracted value is equal to the expected output")
                .isEqualTo(expectedOutput);
    }

    private static Stream<Arguments> extractorTypes() {
        return Iterables.stream(Iterables.concat(
                extractorArguments(), nonStrippableArrayExtractorArguments(), strippableArrayExtractorArguments()));
    }

    private static List<Arguments> extractorArguments() {
        return List.of(
                new ExtractorTypeTestCaseBuilder("Boolean Extractor", Extractors::boolean_, "true", true, "boolean")
                        .build(),
                new ExtractorTypeTestCaseBuilder("Byte Extractor", Extractors::byte_, "55", (byte) 55, "byte")
                        .withNormalization(Extractors::long_, 55L)
                        .build(),
                new ExtractorTypeTestCaseBuilder("Short Extractor", Extractors::short_, "20000", (short) 20000, "short")
                        .withNormalization(Extractors::long_, 20000L)
                        .build(),
                new ExtractorTypeTestCaseBuilder("Int Extractor", Extractors::int_, "2000000", 2000000, "int")
                        .withNormalization(Extractors::long_, 2000000L)
                        .build(),
                new ExtractorTypeTestCaseBuilder("Long Extractor", Extractors::long_, "0", 0L, "long").build(),
                new ExtractorTypeTestCaseBuilder("Long Extractor", Extractors::long_, "-0", 0L, "long").build(),
                new ExtractorTypeTestCaseBuilder("Long Extractor", Extractors::long_, "000000", 0L, "long").build(),
                new ExtractorTypeTestCaseBuilder(
                                "Long Extractor", Extractors::long_, "00000000000000000000000000000000000", 0L, "long")
                        .build(),
                new ExtractorTypeTestCaseBuilder("Long Extractor", Extractors::long_, "42", 42L, "long").build(),
                new ExtractorTypeTestCaseBuilder(
                                "Long Extractor",
                                Extractors::long_,
                                "0000000000000000000000000000000000042",
                                42L,
                                "long")
                        .build(),
                new ExtractorTypeTestCaseBuilder("Long Extractor", Extractors::long_, "4000000000", 4000000000L, "long")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Long Extractor", Extractors::long_, "-4000000000", -4000000000L, "long")
                        .build(),
                new ExtractorTypeTestCaseBuilder("Long Extractor", Extractors::long_, "-42", -42L, "long").build(),
                new ExtractorTypeTestCaseBuilder("Long Extractor", Extractors::long_, "-000042", -42L, "long").build(),
                new ExtractorTypeTestCaseBuilder(
                                "Long Extractor",
                                Extractors::long_,
                                "-0000000000000000000000000000000000042",
                                -42L,
                                "long")
                        .build(),
                new ExtractorTypeTestCaseBuilder("Float Extractor", Extractors::float_, "1.0", 1.0F, "float")
                        .withNormalization(Extractors::double_, 1.0D)
                        .build(),
                new ExtractorTypeTestCaseBuilder("Double Extractor", Extractors::double_, "1.0", 1.0D, "double")
                        .build(),
                new ExtractorTypeTestCaseBuilder("Char Extractor", Extractors::char_, "a", 'a', "char")
                        .withNormalization(Extractors::string, "a")
                        .build(),
                new ExtractorTypeTestCaseBuilder("String Extractor", Extractors::string, "abcde", "abcde", "String")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "TextValue Extractor",
                                Extractors::textValue,
                                "abcde",
                                Values.utf8Value("abcde"),
                                "TextValue")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Date Extractor", Extractors::date, "1985-4-20", DateValue.date(1985, 4, 20), "Date")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Time Extractor",
                                Extractors::time,
                                "2:41:34",
                                TimeValue.time(2, 41, 34, 0, ZoneOffset.ofHours(10)),
                                "Time")
                        .withCSVHeaders(TimeValue.parseHeaderInformation("{timezone:+10:00}"))
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "DateTime Extractor",
                                Extractors::dateTime,
                                "1985-4-20T2:41:34",
                                DateTimeValue.datetime(1985, 4, 20, 2, 41, 34, 0, ZoneOffset.ofHours(10)),
                                "DateTime")
                        .withCSVHeaders(TimeValue.parseHeaderInformation("{timezone:+10:00}"))
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "LocalDateTime Extractor",
                                Extractors::localDateTime,
                                "1985-4-20T2:41:34",
                                LocalDateTimeValue.localDateTime(1985, 4, 20, 2, 41, 34, 0),
                                "LocalDateTime")
                        .withCSVHeaders(TimeValue.parseHeaderInformation("{timezone:+10:00}"))
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "LocalTime Extractor",
                                Extractors::localTime,
                                "2:41:34",
                                LocalTimeValue.localTime(2, 41, 34, 0),
                                "LocalTime")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Point Extractor",
                                Extractors::point,
                                "Point{latitude: 56.7, longitude: 13.2}",
                                Values.pointValue(CoordinateReferenceSystem.WGS_84, 13.2, 56.7),
                                "Point")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Duration Extractor",
                                Extractors::duration,
                                "PT60S",
                                DurationValue.duration(0, 0, 60, 0),
                                "Duration")
                        .build());
    }

    private static List<Arguments> nonStrippableArrayExtractorArguments() {
        return List.of(
                new ExtractorTypeTestCaseBuilder(
                                "String Array Extractor",
                                Extractors::stringArray,
                                "abcde; fghijkl;mnopq",
                                new String[] {"abcde", " fghijkl", "mnopq"},
                                "String[]")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Date Array Extractor",
                                Extractors::dateArray,
                                "1985-4-20;2030-12-12",
                                Values.dateArray(array(LocalDate.of(1985, 4, 20), LocalDate.of(2030, 12, 12))),
                                "Date[]")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Time Array Extractor",
                                Extractors::timeArray,
                                "2:41:34;18:3:51",
                                Values.timeArray(array(
                                        OffsetTime.of(2, 41, 34, 0, ZoneOffset.ofHours(10)),
                                        OffsetTime.of(18, 3, 51, 0, ZoneOffset.ofHours(10)))),
                                "Time[]")
                        .withCSVHeaders(TimeValue.parseHeaderInformation("{timezone:+10:00}"))
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "DateTime Array Extractor",
                                Extractors::dateTimeArray,
                                "1985-4-20T2:41:34;2030-12-12T18:3:51",
                                Values.dateTimeArray(array(
                                        ZonedDateTime.of(1985, 4, 20, 2, 41, 34, 0, ZoneOffset.ofHours(10)),
                                        ZonedDateTime.of(2030, 12, 12, 18, 3, 51, 0, ZoneOffset.ofHours(10)))),
                                "DateTime[]")
                        .withCSVHeaders(TimeValue.parseHeaderInformation("{timezone:+10:00}"))
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "LocalDateTime Array Extractor",
                                Extractors::localDateTimeArray,
                                "1985-4-20T2:41:34;2030-12-12T18:3:51",
                                Values.localDateTimeArray(array(
                                        LocalDateTime.of(1985, 4, 20, 2, 41, 34, 0),
                                        LocalDateTime.of(2030, 12, 12, 18, 3, 51, 0))),
                                "LocalDateTime[]")
                        .withCSVHeaders(TimeValue.parseHeaderInformation("{timezone:+10:00}"))
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "LocalTime Array Extractor",
                                Extractors::localTimeArray,
                                "2:41:34;18:3:51",
                                Values.localTimeArray(array(LocalTime.of(2, 41, 34, 0), LocalTime.of(18, 3, 51, 0))),
                                "LocalTime[]")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Point Array Extractor",
                                Extractors::pointArray,
                                "{latitude: 56.7, longitude: 13.2};{latitude: 0.7, longitude: 0.25}",
                                Values.pointArray(array(
                                        Values.pointValue(CoordinateReferenceSystem.WGS_84, 13.2, 56.7),
                                        Values.pointValue(CoordinateReferenceSystem.WGS_84, 0.25, 0.7))),
                                "Point[]")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Duration Array Extractor",
                                Extractors::durationArray,
                                "PT60S;PT2H",
                                Values.durationArray(
                                        array(Duration.of(60, ChronoUnit.SECONDS), Duration.of(2, ChronoUnit.HOURS))),
                                "Duration[]")
                        .build());
    }

    private static List<Arguments> strippableArrayExtractorArguments() {
        return List.of(
                new ExtractorTypeTestCaseBuilder(
                                "Boolean Array Extractor",
                                Extractors::booleanArray,
                                "true;false;true",
                                new boolean[] {true, false, true},
                                "boolean[]")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Byte Array Extractor",
                                Extractors::byteArray,
                                "-33; 0; 55",
                                new byte[] {-33, 0, 55},
                                "byte[]")
                        .withNormalization(Extractors::byteArray, new byte[] {-33, 0, 55})
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Short Array Extractor",
                                Extractors::shortArray,
                                "-10000; 0; 20000",
                                new short[] {-10000, 0, 20000},
                                "short[]")
                        .withNormalization(Extractors::shortArray, new short[] {-10000, 0, 20000})
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Int Array Extractor",
                                Extractors::intArray,
                                "-1000000; 0; 2000000",
                                new int[] {-1000000, 0, 2000000},
                                "int[]")
                        .withNormalization(Extractors::intArray, new int[] {-1000000, 0, 2000000})
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Long Array Extractor",
                                Extractors::longArray,
                                "-3000000000; 0; 4000000000",
                                new long[] {-3000000000L, 0L, 4000000000L},
                                "long[]")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Float Array Extractor",
                                Extractors::floatArray,
                                "-1.0; 0.0; 1.0",
                                new float[] {-1.0F, 0F, 1.0F},
                                "float[]")
                        .withNormalization(Extractors::floatArray, new float[] {-1.0F, 0F, 1.0F})
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Double Array Extractor",
                                Extractors::doubleArray,
                                "123.123; 4567.4567; 987654321.0987",
                                new double[] {123.123D, 4567.4567D, 987654321.0987D},
                                "double[]")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Int8Vector Extractor",
                                Extractors::int8Vector,
                                "-33§ 0§ 55",
                                Values.int8Vector(new byte[] {-33, 0, 55}),
                                "Int8Vector")
                        .withNormalization(Extractors::int8Vector, Values.int8Vector(new byte[] {-33, 0, 55}))
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Int16Vector Extractor",
                                Extractors::int16Vector,
                                "-33§ 0§ 55",
                                Values.int16Vector(new short[] {-33, 0, 55}),
                                "Int16Vector")
                        .withNormalization(Extractors::int16Vector, Values.int16Vector(new short[] {-33, 0, 55}))
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Int32Vector Extractor",
                                Extractors::int32Vector,
                                "-33§ 0§ 55",
                                Values.int32Vector(-33, 0, 55),
                                "Int32Vector")
                        .withNormalization(Extractors::int32Vector, Values.int32Vector(-33, 0, 55))
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Int64Vector Extractor",
                                Extractors::int64Vector,
                                "-33§ 0§ 55",
                                Values.int64Vector(-33, 0, 55),
                                "Int64Vector")
                        .withNormalization(Extractors::int64Vector, Values.int64Vector(-33, 0, 55))
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Float32Vector Extractor",
                                Extractors::float32Vector,
                                "-33§ 0§ 55",
                                Values.float32Vector(-33, 0, 55),
                                "Float32Vector")
                        .withNormalization(Extractors::float32Vector, Values.float32Vector(-33, 0, 55))
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Float64Vector Extractor",
                                Extractors::float64Vector,
                                "-33§ 0§ 55",
                                Values.float64Vector(-33, 0, 55),
                                "Float64Vector")
                        .withNormalization(Extractors::float64Vector, Values.float64Vector(-33, 0, 55))
                        .build());
    }

    private static Stream<Arguments> numericOverflows() {
        var longMin = BigInteger.valueOf(Long.MIN_VALUE);
        var longMax = BigInteger.valueOf(Long.MAX_VALUE);
        return Stream.of(
                new ExtractorTypeTestCaseBuilder(
                                "Byte", Extractors::byte_, Byte.MAX_VALUE + "0000", "ignored: over Byte.Max", "byte")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Byte[]",
                                Extractors::byteArray,
                                "1;" + Byte.MAX_VALUE + "0000",
                                "ignored: over Byte.Max",
                                "byte")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Short",
                                Extractors::short_,
                                Short.MAX_VALUE + "0000",
                                "ignored: over Short.Max",
                                "short")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Short[]",
                                Extractors::shortArray,
                                "1;" + Short.MAX_VALUE + "0000",
                                "ignored: over Short.Max",
                                "short")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Int", Extractors::int_, Integer.MAX_VALUE + "0000", "ignored: over Int.Max", "int")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Int[]",
                                Extractors::intArray,
                                "1;" + Integer.MAX_VALUE + "0000",
                                "ignored: over Int.Max",
                                "int")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Long overflow to a valid byte",
                                Extractors::byte_,
                                Long.MAX_VALUE + "0",
                                "ignored: overflows to a valid byte",
                                "long")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Long overflow to a valid short",
                                Extractors::short_,
                                Long.MAX_VALUE + "0",
                                "ignored: overflows to a valid short",
                                "long")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Long overflow to a valid int",
                                Extractors::int_,
                                Long.MAX_VALUE + "0",
                                "ignored: overflows to a valid int",
                                "long")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Long.Max + 1",
                                Extractors::long_,
                                longMax.add(BigInteger.ONE).toString(),
                                "ignored: over Long.Max",
                                "long")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Long.Max + 10",
                                Extractors::long_,
                                longMax.add(BigInteger.TEN).toString(),
                                "ignored: over Long.Max",
                                "long")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Long.Max * 10000",
                                Extractors::long_,
                                Long.MAX_VALUE + "0000",
                                "ignored: over Long.Max",
                                "long")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Long.Max * 10000 []",
                                Extractors::longArray,
                                "1;" + Long.MAX_VALUE + "0000",
                                "ignored: over Long.Max",
                                "long")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Huge Long",
                                Extractors::long_,
                                Long.MAX_VALUE + "0000000000000000000000000000000000000000000000000000",
                                "ignored: overflow goes round back into positive again",
                                "long")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Long.Min - 1",
                                Extractors::long_,
                                longMin.subtract(BigInteger.ONE).toString(),
                                "ignored: too low",
                                "long")
                        .build(),
                new ExtractorTypeTestCaseBuilder(
                                "Long.Min - 10",
                                Extractors::long_,
                                longMin.subtract(BigInteger.TEN).toString(),
                                "ignored: too low",
                                "long")
                        .build());
    }

    private record ExtractorTypeTestCase(
            Function<Extractors, Extractor<?>> extractorSelector,
            char[] input,
            Object expectedOutput,
            String expectedExtractorName,
            Function<Extractors, Extractor<?>> normalizedSelector,
            Object expectedNormalizedOutput,
            CSVHeaderInformation optionalCSVHeaders) {}

    private static class ExtractorTypeTestCaseBuilder {
        private final String name;
        private final Function<Extractors, Extractor<?>> selector;
        private final String input;
        private final Object expectedOutput;
        private final String expectedExtractorName;

        private Function<Extractors, Extractor<?>> normalizedSelector;
        private Object expectedNormalizedOutput;

        private CSVHeaderInformation optionalCSVHeaders;

        private ExtractorTypeTestCaseBuilder(
                String name,
                Function<Extractors, Extractor<?>> selector,
                String input,
                Object expectedOutput,
                String expectedExtractorName) {
            this.name = name;
            this.selector = selector;
            this.input = input;
            this.expectedOutput = expectedOutput;
            this.expectedExtractorName = expectedExtractorName;

            normalizedSelector = selector;
            expectedNormalizedOutput = expectedOutput;
            optionalCSVHeaders = null;
        }

        public ExtractorTypeTestCaseBuilder withNormalization(
                Function<Extractors, Extractor<?>> normalizedSelector, Object expectedNormalizedOutput) {
            this.normalizedSelector = normalizedSelector;
            this.expectedNormalizedOutput = expectedNormalizedOutput;
            return this;
        }

        public ExtractorTypeTestCaseBuilder withCSVHeaders(CSVHeaderInformation optionalCSVHeaders) {
            this.optionalCSVHeaders = optionalCSVHeaders;
            return this;
        }

        public Arguments build() {
            return Arguments.of(named(
                    name,
                    new ExtractorTypeTestCase(
                            selector,
                            input.toCharArray(),
                            expectedOutput,
                            expectedExtractorName,
                            normalizedSelector,
                            expectedNormalizedOutput,
                            optionalCSVHeaders)));
        }
    }
}
