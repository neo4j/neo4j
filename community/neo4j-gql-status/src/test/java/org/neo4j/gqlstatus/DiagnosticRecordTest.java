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
package org.neo4j.gqlstatus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DiagnosticRecordTest {
    @Test
    void shouldHaveExpectedKeys() {
        Map<String, Object> diagnosticRecordMap =
                new DiagnosticRecord("", ErrorClassification.CLIENT_ERROR, 0, 0, 0, Map.of()).asMap();
        Set<String> expectedKeys = Set.of(
                "OPERATION", "OPERATION_CODE", "CURRENT_SCHEMA", "_severity", "_classification", "_position" /*,
                "_status_parameters"*/
                // TODO: enable this line again when re-introducing status parameters
                );

        assertThat(diagnosticRecordMap).containsOnlyKeys(expectedKeys);
    }

    @Test
    void shouldHaveExpectedDefaultValues() {
        Map<String, Object> diagnosticRecordMap =
                new DiagnosticRecord("", ErrorClassification.CLIENT_ERROR, 0, 0, 0, Map.of()).asMap();
        assertThat(diagnosticRecordMap.get("CURRENT_SCHEMA")).isEqualTo("/");
        assertThat(diagnosticRecordMap.get("OPERATION")).isEqualTo("");
        assertThat(diagnosticRecordMap.get("OPERATION_CODE")).isEqualTo("0");
    }

    @Test
    void shouldConstructProperPositionMap() {
        Map<String, Object> diagnosticRecordMap =
                new DiagnosticRecord("", ErrorClassification.CLIENT_ERROR, 1, 2, 3, Map.of()).asMap();
        assertThat(diagnosticRecordMap.get("_position")).isInstanceOf(Map.class);

        @SuppressWarnings("unchecked")
        Map<String, Object> position = (Map<String, Object>) diagnosticRecordMap.get("_position");

        assertThat(position).containsEntry("offset", 1);
        assertThat(position).containsEntry("line", 2);
        assertThat(position).containsEntry("column", 3);
    }

    @Test
    void shouldNotStoreUnknownErrorClassificationFromConstructor() {
        Map<String, Object> diagnosticRecordMap =
                new DiagnosticRecord("", ErrorClassification.UNKNOWN, 0, 0, 0, Map.of()).asMap();
        assertThat(diagnosticRecordMap).doesNotContainKey("_classification");
    }

    @Test
    void shouldNotStoreUnknownNotificationClassificationFromConstructor() {
        Map<String, Object> diagnosticRecordMap =
                new DiagnosticRecord("", NotificationClassification.UNKNOWN, 0, 0, 0, Map.of()).asMap();
        assertThat(diagnosticRecordMap).doesNotContainKey("_classification");
    }

    @Test
    void shouldNotStoreUnknownErrorClassificationFromBuilder() {
        DiagnosticRecord.Builder diagnosticRecordBuilder = DiagnosticRecord.from();
        diagnosticRecordBuilder.withClassification(ErrorClassification.UNKNOWN);
        Map<String, Object> diagnosticRecordMap =
                diagnosticRecordBuilder.build().asMap();
        assertThat(diagnosticRecordMap).doesNotContainKey("_classification");
    }

    @Test
    void shouldNotStoreUnknownNotificationClassificationFromBuilder() {
        DiagnosticRecord.Builder diagnosticRecordBuilder = DiagnosticRecord.from();
        diagnosticRecordBuilder.withClassification(NotificationClassification.UNKNOWN);
        Map<String, Object> diagnosticRecordMap =
                diagnosticRecordBuilder.build().asMap();
        assertThat(diagnosticRecordMap).doesNotContainKey("_classification");
    }

    @ParameterizedTest
    @MethodSource("propertyFixture")
    <T> void shouldBuildDiagnosticRecordWithProperty(
            DiagnosticRecordProperty<T> property, T value, Optional<T> expectedValue) {
        var diagnosticRecord =
                DiagnosticRecord.from().withProperty(property, value).build();

        var map = diagnosticRecord.asMap();

        if (expectedValue.isPresent()) {
            assertThat(map).containsEntry(property.key(), expectedValue.get());
        } else {
            assertThat(map).doesNotContainKey(property.key());
        }
    }

    @ParameterizedTest
    @MethodSource("propertyFixture")
    <T> void shouldSetAPropertyInTheDiagnosticRecord(
            DiagnosticRecordProperty<T> property, T value, Optional<T> expectedValue) {
        var diagnosticRecord = new DiagnosticRecord();
        diagnosticRecord.withProperty(property, value);

        var map = diagnosticRecord.asMap();

        if (expectedValue.isPresent()) {
            assertThat(map).containsEntry(property.key(), expectedValue.get());
        } else {
            assertThat(map).doesNotContainKey(property.key());
        }
    }

    private static Stream<Arguments> propertyFixture() {
        Supplier<NonGqlStandardDiagnosticRecordProperty.Builder<String>> propertyBuilder =
                () -> NonGqlStandardDiagnosticRecordProperty.Builder.fromKey("_custom_key");
        var simpleProperty = propertyBuilder.get().build();
        var omittedValueProperty = propertyBuilder
                .get()
                .withValueOmittedPredicate(o -> o != null && o.equals(""))
                .build();
        var transformedValueProperty = propertyBuilder
                .get()
                .withValueSerializer(o -> {
                    if (o != null) {
                        return "Value: ".concat(o.toString());
                    }
                    return null;
                })
                .build();
        var disabledProperty = propertyBuilder.get().disabled().build();

        return Stream.of(
                Arguments.of(simpleProperty, "value", Optional.of("value")),
                Arguments.of(simpleProperty, "", Optional.of("")),
                Arguments.of(simpleProperty, null, Optional.empty()),
                Arguments.of(omittedValueProperty, "value", Optional.of("value")),
                Arguments.of(omittedValueProperty, "", Optional.empty()),
                Arguments.of(omittedValueProperty, null, Optional.empty()),
                Arguments.of(transformedValueProperty, "value", Optional.of("Value: value")),
                Arguments.of(transformedValueProperty, "", Optional.of("Value: ")),
                Arguments.of(transformedValueProperty, null, Optional.empty()),
                Arguments.of(disabledProperty, "value", Optional.empty()),
                Arguments.of(disabledProperty, "", Optional.empty()),
                Arguments.of(disabledProperty, null, Optional.empty()));
    }
}
