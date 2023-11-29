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
                        () -> extractors.valueOf("long[]").extract(data.toCharArray(), 0, data.length(), false),
                        "fails when parsing invalid data type - word when number was expected")
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void shouldExtractNullForEmptyQuotedStringIfConfiguredTo() {
        // GIVEN
        Extractors extractors = new Extractors(';', true);
        Extractor<String> extractor = extractors.string();

        // WHEN
        var extractedValue = extractor.extract(new char[0], 0, 0, true);

        // THEN
        assertThat(extractedValue).isNull();
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
        assertThat(extractedValue).containsExactly("ab", "cd", "ef", "gh");
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
        assertThat(extractedValue).containsExactly("ab", "cd ", " ef", " gh ");
    }

    @MethodSource("extractorTypes")
    @ParameterizedTest(name = "{0}")
    void shouldExtractValue(ExtractorTypeTestCase testCase) {
        // given
        var extractors = new Extractors();
        var extractor = testCase.extractorSelector.apply(extractors);
        var input = testCase.input;

        // when
        var extractedValue = testCase.optionalCSVHeaders != null
                ? extractor.extract(input, 0, input.length, false, testCase.optionalCSVHeaders)
                : extractor.extract(input, 0, input.length, false);

        // then
        assertThat(extractedValue)
                .as("extracted value is equal to the expected output")
                .isEqualTo(testCase.expectedOutput);
    }

    @MethodSource("extractorTypes")
    @ParameterizedTest(name = "{0}")
    void shouldExtractNormalizedValue(ExtractorTypeTestCase testCase) {
        // given
        var extractors = new Extractors();
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
        var extractors = new Extractors();
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
    void shouldExtractEmptyField(ExtractorTypeTestCase testCase) {
        // given
        var extractors = new Extractors();
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
        var extractors = new Extractors();
        var extractor = testCase.extractorSelector.apply(extractors);

        // then
        assertThat(extractor.name()).as("Extractor name as expected").isEqualTo(testCase.expectedExtractorName);
    }

    public static Stream<Arguments> extractorTypes() {
        List<Arguments> types = new ArrayList<>();

        types.add(new ExtractorTypeTestCaseBuilder("Boolean Extractor", Extractors::boolean_, "true", true, "boolean")
                .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "Boolean Array Extractor",
                        Extractors::booleanArray,
                        "true;false;true",
                        new boolean[] {true, false, true},
                        "boolean[]")
                .build());

        types.add(new ExtractorTypeTestCaseBuilder("Byte Extractor", Extractors::byte_, "55", (byte) 55, "byte")
                .withNormalization(Extractors::long_, 55L)
                .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "Byte Array Extractor", Extractors::byteArray, "-33; 0; 55", new byte[] {-33, 0, 55}, "byte[]")
                .withNormalization(Extractors::byteArray, new byte[] {-33, 0, 55})
                .build());

        types.add(
                new ExtractorTypeTestCaseBuilder("Short Extractor", Extractors::short_, "20000", (short) 20000, "short")
                        .withNormalization(Extractors::long_, 20000L)
                        .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "Short Array Extractor",
                        Extractors::shortArray,
                        "-10000; 0; 20000",
                        new short[] {-10000, 0, 20000},
                        "short[]")
                .withNormalization(Extractors::shortArray, new short[] {-10000, 0, 20000})
                .build());

        types.add(new ExtractorTypeTestCaseBuilder("Int Extractor", Extractors::int_, "2000000", 2000000, "int")
                .withNormalization(Extractors::long_, 2000000L)
                .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "Int Array Extractor",
                        Extractors::intArray,
                        "-1000000; 0; 2000000",
                        new int[] {-1000000, 0, 2000000},
                        "int[]")
                .withNormalization(Extractors::intArray, new int[] {-1000000, 0, 2000000})
                .build());

        types.add(
                new ExtractorTypeTestCaseBuilder("Long Extractor", Extractors::long_, "4000000000", 4000000000L, "long")
                        .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "Long Array Extractor",
                        Extractors::longArray,
                        "-3000000000; 0; 4000000000",
                        new long[] {-3000000000L, 0L, 4000000000L},
                        "long[]")
                .build());

        types.add(new ExtractorTypeTestCaseBuilder("Float Extractor", Extractors::float_, "1.0", 1.0F, "float")
                .withNormalization(Extractors::double_, 1.0D)
                .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "Float Array Extractor",
                        Extractors::floatArray,
                        "-1.0; 0.0; 1.0",
                        new float[] {-1.0F, 0F, 1.0F},
                        "float[]")
                .withNormalization(Extractors::floatArray, new float[] {-1.0F, 0F, 1.0F})
                .build());

        types.add(new ExtractorTypeTestCaseBuilder("Double Extractor", Extractors::double_, "1.0", 1.0D, "double")
                .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "Double Array Extractor",
                        Extractors::doubleArray,
                        "123.123; 4567.4567; 987654321.0987",
                        new double[] {123.123D, 4567.4567D, 987654321.0987D},
                        "double[]")
                .build());

        types.add(new ExtractorTypeTestCaseBuilder("Char Extractor", Extractors::char_, "a", 'a', "char")
                .withNormalization(Extractors::string, "a")
                .build());
        types.add(new ExtractorTypeTestCaseBuilder("String Extractor", Extractors::string, "abcde", "abcde", "String")
                .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "String Array Extractor",
                        Extractors::stringArray,
                        "abcde; fghijkl;mnopq",
                        new String[] {"abcde", " fghijkl", "mnopq"},
                        "String[]")
                .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "TextValue Extractor", Extractors::textValue, "abcde", Values.utf8Value("abcde"), "TextValue")
                .build());

        types.add(new ExtractorTypeTestCaseBuilder(
                        "Date Extractor", Extractors::date, "1985-4-20", DateValue.date(1985, 4, 20), "Date")
                .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "Date Array Extractor",
                        Extractors::dateArray,
                        "1985-4-20;2030-12-12",
                        Values.dateArray(array(LocalDate.of(1985, 4, 20), LocalDate.of(2030, 12, 12))),
                        "Date[]")
                .build());

        types.add(new ExtractorTypeTestCaseBuilder(
                        "Time Extractor",
                        Extractors::time,
                        "2:41:34",
                        TimeValue.time(2, 41, 34, 0, ZoneOffset.ofHours(10)),
                        "Time")
                .withCSVHeaders(TimeValue.parseHeaderInformation("{timezone:+10:00}"))
                .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "Time Array Extractor",
                        Extractors::timeArray,
                        "2:41:34;18:3:51",
                        Values.timeArray(array(
                                OffsetTime.of(2, 41, 34, 0, ZoneOffset.ofHours(10)),
                                OffsetTime.of(18, 3, 51, 0, ZoneOffset.ofHours(10)))),
                        "Time[]")
                .withCSVHeaders(TimeValue.parseHeaderInformation("{timezone:+10:00}"))
                .build());

        types.add(new ExtractorTypeTestCaseBuilder(
                        "DateTime Extractor",
                        Extractors::dateTime,
                        "1985-4-20T2:41:34",
                        DateTimeValue.datetime(1985, 4, 20, 2, 41, 34, 0, ZoneOffset.ofHours(10)),
                        "DateTime")
                .withCSVHeaders(TimeValue.parseHeaderInformation("{timezone:+10:00}"))
                .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "DateTime Array Extractor",
                        Extractors::dateTimeArray,
                        "1985-4-20T2:41:34;2030-12-12T18:3:51",
                        Values.dateTimeArray(array(
                                ZonedDateTime.of(1985, 4, 20, 2, 41, 34, 0, ZoneOffset.ofHours(10)),
                                ZonedDateTime.of(2030, 12, 12, 18, 3, 51, 0, ZoneOffset.ofHours(10)))),
                        "DateTime[]")
                .withCSVHeaders(TimeValue.parseHeaderInformation("{timezone:+10:00}"))
                .build());

        types.add(new ExtractorTypeTestCaseBuilder(
                        "LocalDateTime Extractor",
                        Extractors::localDateTime,
                        "1985-4-20T2:41:34",
                        LocalDateTimeValue.localDateTime(1985, 4, 20, 2, 41, 34, 0),
                        "LocalDateTime")
                .withCSVHeaders(TimeValue.parseHeaderInformation("{timezone:+10:00}"))
                .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "LocalDateTime Array Extractor",
                        Extractors::localDateTimeArray,
                        "1985-4-20T2:41:34;2030-12-12T18:3:51",
                        Values.localDateTimeArray(array(
                                LocalDateTime.of(1985, 4, 20, 2, 41, 34, 0),
                                LocalDateTime.of(2030, 12, 12, 18, 3, 51, 0))),
                        "LocalDateTime[]")
                .withCSVHeaders(TimeValue.parseHeaderInformation("{timezone:+10:00}"))
                .build());

        types.add(new ExtractorTypeTestCaseBuilder(
                        "LocalTime Extractor",
                        Extractors::localTime,
                        "2:41:34",
                        LocalTimeValue.localTime(2, 41, 34, 0),
                        "LocalTime")
                .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "LocalTime Array Extractor",
                        Extractors::localTimeArray,
                        "2:41:34;18:3:51",
                        Values.localTimeArray(array(LocalTime.of(2, 41, 34, 0), LocalTime.of(18, 3, 51, 0))),
                        "LocalTime[]")
                .build());

        types.add(new ExtractorTypeTestCaseBuilder(
                        "Point Extractor",
                        Extractors::point,
                        "Point{latitude: 56.7, longitude: 13.2}",
                        Values.pointValue(CoordinateReferenceSystem.WGS_84, 13.2, 56.7),
                        "Point")
                .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "Point Array Extractor",
                        Extractors::pointArray,
                        "{latitude: 56.7, longitude: 13.2};{latitude: 0.7, longitude: 0.25}",
                        Values.pointArray(array(
                                Values.pointValue(CoordinateReferenceSystem.WGS_84, 13.2, 56.7),
                                Values.pointValue(CoordinateReferenceSystem.WGS_84, 0.25, 0.7))),
                        "Point[]")
                .build());

        types.add(new ExtractorTypeTestCaseBuilder(
                        "Duration Extractor",
                        Extractors::duration,
                        "PT60S",
                        DurationValue.duration(0, 0, 60, 0),
                        "Duration")
                .build());
        types.add(new ExtractorTypeTestCaseBuilder(
                        "Duration Array Extractor",
                        Extractors::durationArray,
                        "PT60S;PT2H",
                        Values.durationArray(
                                array(Duration.of(60, ChronoUnit.SECONDS), Duration.of(2, ChronoUnit.HOURS))),
                        "Duration[]")
                .build());
        return types.stream();
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
