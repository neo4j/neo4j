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
package org.neo4j.server.queryapi.request.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.NotificationClassification;
import org.neo4j.driver.NotificationSeverity;
import org.neo4j.server.queryapi.request.QueryRequestNotificationsFilter;

class QueryRequestNotificationsFilterImplTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @ParameterizedTest
    @MethodSource("validInputs")
    void shouldDeserializeValidInputs(
            String minimumSeverityLevel,
            Optional<NotificationSeverity> expectedMinimumSeverityLevel,
            String[] disabledCategories,
            Optional<Set<NotificationClassification>> expectedDisabledCategories)
            throws IOException {
        var filter = QueryRequestNotificationsFilterImpl.deserialize(minimumSeverityLevel, disabledCategories);

        assertThat(filter.minimumSeverityLevel()).isEqualTo(expectedMinimumSeverityLevel);
        assertThat(filter.disabledCategories()).isEqualTo(expectedDisabledCategories);
    }

    @ParameterizedTest
    @MethodSource("invalidMinimumSeverityLevels")
    void shouldRejectInvalidMinimumSeverityLevels(String minimumSeverityLevel) {
        assertThatThrownBy(() -> QueryRequestNotificationsFilterImpl.deserialize(minimumSeverityLevel, null))
                .isInstanceOf(JsonParseException.class)
                .hasMessage("Invalid minimum severity level: " + minimumSeverityLevel);
    }

    @ParameterizedTest
    @MethodSource("invalidDisabledCategories")
    void shouldRejectInvalidDisabledCategories(
            String[] disabledCategories, Class<? extends Throwable> expectedException) {
        assertThatThrownBy(() -> QueryRequestNotificationsFilterImpl.deserialize(null, disabledCategories))
                .isInstanceOf(expectedException);
    }

    @ParameterizedTest
    @MethodSource("validJsonInputs")
    void shouldDeserializeValidJsonInputs(
            String json,
            Optional<NotificationSeverity> expectedMinimumSeverityLevel,
            Optional<Set<NotificationClassification>> expectedDisabledCategories)
            throws IOException {
        var filter = readFilter(json);

        assertThat(filter.minimumSeverityLevel()).isEqualTo(expectedMinimumSeverityLevel);
        assertThat(filter.disabledCategories()).isEqualTo(expectedDisabledCategories);
    }

    @ParameterizedTest
    @MethodSource("invalidJsonInputs")
    void shouldRejectInvalidJsonInputs(String json, String expectedCause) {
        assertThatThrownBy(() -> readFilter(json))
                .isInstanceOf(JsonMappingException.class)
                .cause()
                .isInstanceOf(JsonParseException.class)
                .hasMessage(expectedCause);
    }

    private static Stream<Arguments> validInputs() {
        return minimumSeverityLevels()
                .flatMap(minimumSeverityLevel -> disabledCategories()
                        .map(disabledCategories -> Arguments.of(
                                minimumSeverityLevel.input(),
                                minimumSeverityLevel.expected(),
                                disabledCategories.input(),
                                disabledCategories.expected())));
    }

    private static Stream<MinimumSeverityLevel> minimumSeverityLevels() {
        return Stream.of(
                new MinimumSeverityLevel(null, Optional.empty()),
                new MinimumSeverityLevel("WARNING", Optional.of(NotificationSeverity.WARNING)),
                new MinimumSeverityLevel("INFORMATION", Optional.of(NotificationSeverity.INFORMATION)),
                new MinimumSeverityLevel("OFF", Optional.of(NotificationSeverity.OFF)));
    }

    private static Stream<DisabledCategories> disabledCategories() {
        return Stream.of(
                new DisabledCategories(null, Optional.empty()),
                new DisabledCategories(new String[] {}, Optional.of(Set.of())),
                new DisabledCategories(new String[] {"HINT"}, Optional.of(Set.of(NotificationClassification.HINT))),
                new DisabledCategories(
                        new String[] {"UNRECOGNIZED"}, Optional.of(Set.of(NotificationClassification.UNRECOGNIZED))),
                new DisabledCategories(
                        new String[] {"PERFORMANCE"}, Optional.of(Set.of(NotificationClassification.PERFORMANCE))),
                new DisabledCategories(
                        Arrays.stream(NotificationClassification.values())
                                .map(NotificationClassification::name)
                                .toArray(String[]::new),
                        Optional.of(Set.of(NotificationClassification.values()))));
    }

    private static Stream<String> invalidMinimumSeverityLevels() {
        return Stream.of("", "warning", "Warning", "DUNO", "SEVERE", "OFF ");
    }

    private static Stream<Arguments> invalidDisabledCategories() {
        return Stream.of(
                Arguments.of(new String[] {""}, JsonParseException.class),
                Arguments.of(new String[] {"hint"}, JsonParseException.class),
                Arguments.of(new String[] {"Hint"}, JsonParseException.class),
                Arguments.of(new String[] {"DUNO"}, JsonParseException.class),
                Arguments.of(new String[] {"HINT "}, JsonParseException.class),
                Arguments.of(new String[] {null}, JsonParseException.class),
                Arguments.of(new String[] {"HINT", "DUNO"}, JsonParseException.class));
    }

    private QueryRequestNotificationsFilter readFilter(String json) throws IOException {
        return objectMapper.readerFor(QueryRequestNotificationsFilterImpl.class).readValue(json);
    }

    private static Stream<Arguments> validJsonInputs() {
        return Stream.of(
                Arguments.of("{}", Optional.empty(), Optional.empty()),
                Arguments.of("""
                        {
                          "minimumSeverityLevel": null,
                          "disabledCategories": null
                        }
                        """, Optional.empty(), Optional.empty()),
                Arguments.of("""
                        {
                          "minimumSeverityLevel": "WARNING"
                        }
                        """, Optional.of(NotificationSeverity.WARNING), Optional.empty()),
                Arguments.of("""
                        {
                          "minimumSeverityLevel": "INFORMATION"
                        }
                        """, Optional.of(NotificationSeverity.INFORMATION), Optional.empty()),
                Arguments.of("""
                        {
                          "minimumSeverityLevel": "OFF"
                        }
                        """, Optional.of(NotificationSeverity.OFF), Optional.empty()),
                Arguments.of("""
                        {
                          "disabledCategories": []
                        }
                        """, Optional.empty(), Optional.of(Set.of())),
                Arguments.of("""
                        {
                          "disabledCategories": ["HINT"]
                        }
                        """, Optional.empty(), Optional.of(Set.of(NotificationClassification.HINT))),
                Arguments.of(
                        """
                        {
                          "disabledCategories": ["UNRECOGNIZED", "PERFORMANCE"]
                        }
                        """,
                        Optional.empty(),
                        Optional.of(Set.of(
                                NotificationClassification.UNRECOGNIZED, NotificationClassification.PERFORMANCE))),
                Arguments.of(
                        """
                        {
                          "minimumSeverityLevel": "WARNING",
                          "disabledCategories": ["HINT", "UNRECOGNIZED", "PERFORMANCE"]
                        }
                        """,
                        Optional.of(NotificationSeverity.WARNING),
                        Optional.of(Set.of(
                                NotificationClassification.HINT,
                                NotificationClassification.UNRECOGNIZED,
                                NotificationClassification.PERFORMANCE))),
                Arguments.of(
                        """
                        {
                          "minimumSeverityLevel": "OFF",
                          "disabledCategories": [%s]
                        }
                        """.formatted(allNotificationClassificationsJsonArrayElements()),
                        Optional.of(NotificationSeverity.OFF),
                        Optional.of(Set.of(NotificationClassification.values()))));
    }

    private static Stream<Arguments> invalidJsonInputs() {
        return Stream.of(
                Arguments.of("""
                        {
                          "minimumSeverityLevel": ""
                        }
                        """, "Invalid minimum severity level: "),
                Arguments.of("""
                        {
                          "minimumSeverityLevel": "warning"
                        }
                        """, "Invalid minimum severity level: warning"),
                Arguments.of("""
                        {
                          "minimumSeverityLevel": "DUNO"
                        }
                        """, "Invalid minimum severity level: DUNO"),
                Arguments.of("""
                        {
                          "minimumSeverityLevel": "OFF "
                        }
                        """, "Invalid minimum severity level: OFF "),
                Arguments.of("""
                        {
                          "disabledCategories": [""]
                        }
                        """, "Invalid disabled category: "),
                Arguments.of("""
                        {
                          "disabledCategories": ["hint"]
                        }
                        """, "Invalid disabled category: hint"),
                Arguments.of("""
                        {
                          "disabledCategories": ["DUNO"]
                        }
                        """, "Invalid disabled category: DUNO"),
                Arguments.of("""
                        {
                          "disabledCategories": ["HINT "]
                        }
                        """, "Invalid disabled category: HINT "),
                Arguments.of("""
                        {
                          "disabledCategories": [null]
                        }
                        """, "Invalid disabled category: null"),
                Arguments.of("""
                        {
                          "minimumSeverityLevel": "WARNING",
                          "disabledCategories": ["HINT", "DUNO"]
                        }
                        """, "Invalid disabled category: DUNO"));
    }

    private static String allNotificationClassificationsJsonArrayElements() {
        return Arrays.stream(NotificationClassification.values())
                .map(NotificationClassification::name)
                .map("\"%s\""::formatted)
                .collect(java.util.stream.Collectors.joining(", "));
    }

    private record MinimumSeverityLevel(String input, Optional<NotificationSeverity> expected) {}

    private record DisabledCategories(String[] input, Optional<Set<NotificationClassification>> expected) {}
}
