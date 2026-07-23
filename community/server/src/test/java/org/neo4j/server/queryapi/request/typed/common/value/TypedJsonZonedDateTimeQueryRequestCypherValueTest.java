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
package org.neo4j.server.queryapi.request.typed.common.value;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.server.queryapi.request.typed.common.TypedJsonRequestModule;
import org.neo4j.server.queryapi.types.CypherTypes;
import org.neo4j.server.queryapi.types.View;

@ParameterizedClass
@MethodSource("views")
class TypedJsonZonedDateTimeQueryRequestCypherValueTest {
    private final ObjectMapper mapper;

    TypedJsonZonedDateTimeQueryRequestCypherValueTest(View view) {
        mapper = new ObjectMapper().registerModule(new TypedJsonRequestModule(view));
    }

    @ParameterizedTest
    @MethodSource("zonedDateTimes")
    void shouldDeserializeZonedDateTimeParameters(String zonedDateTimeString) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "$type": "ZonedDateTime",
                    "_value": "%s"
                }
                """.formatted(zonedDateTimeString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();

        var zonedDateTime = value.asZonedDateTime();
        assertThat(zonedDateTime).isEqualTo(ZonedDateTime.parse(zonedDateTimeString));
        assertThat(zonedDateTime.getZone()).isNotInstanceOf(ZoneOffset.class);
    }

    @ParameterizedTest
    @MethodSource("zonedDateTimes")
    void shouldDeserializeZonedDateTimeParametersWhenValueIsBeforeType(String zonedDateTimeString) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "_value": "%s",
                    "$type": "ZonedDateTime"
                }
                """.formatted(zonedDateTimeString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();

        var zonedDateTime = value.asZonedDateTime();
        assertThat(zonedDateTime).isEqualTo(ZonedDateTime.parse(zonedDateTimeString));
        assertThat(zonedDateTime.getZone()).isNotInstanceOf(ZoneOffset.class);
    }

    @ParameterizedTest
    @MethodSource("invalidZonedDateTimes")
    void shouldRejectInvalidZonedDateTimeParameters(String zonedDateTimeString) throws JsonProcessingException {
        assertThatThrownBy(() ->
                        mapper.readValue("""
                {
                    "$type": "ZonedDateTime",
                    "_value": "%s"
                }
                """.formatted(zonedDateTimeString), TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(Exception.class)
                .hasRootCauseInstanceOf(DateTimeParseException.class);
    }

    @Test
    void shouldRejectWhenValueNotPresent() throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "ZonedDateTime"
                }
                """, TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    private static Stream<String> zonedDateTimes() {
        return Stream.of(
                "2015-11-21T21:40:32.142Z[Antarctica/Troll]",
                "2025-12-01T12:30:00+01:00[Europe/Berlin]",
                "0800-01-01T00:00:00Z[Etc/GMT]");
    }

    private static Stream<Arguments> invalidZonedDateTimes() {
        return Stream.of(
                Arguments.of("2025-12-01T12:30:00[Europe/Berlin]"),
                Arguments.of("2025-12-01T12:30:00+01:00"),
                Arguments.of("2015-11-21T21:40:32.142Z"),
                Arguments.of("not a zoned datetime"));
    }

    private static Stream<View> views() {
        return Stream.of(View.values())
                .filter(view -> view != View.PLAIN_JSON)
                .filter(view -> view.supports(CypherTypes.ZonedDateTime));
    }
}
