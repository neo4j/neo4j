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
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.server.queryapi.request.typed.common.TypedJsonRequestModule;
import org.neo4j.server.queryapi.types.CypherTypes;
import org.neo4j.server.queryapi.types.View;

@ParameterizedClass
@MethodSource("views")
class TypedJsonLocalDateTimeQueryRequestCypherValueTest {
    private final ObjectMapper mapper;

    TypedJsonLocalDateTimeQueryRequestCypherValueTest(View view) {
        mapper = new ObjectMapper().registerModule(new TypedJsonRequestModule(view));
    }

    @ParameterizedTest
    @MethodSource("localDateTimes")
    void shouldDeserializeLocalDateTimeParameters(String localDateTimeString) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "$type": "LocalDateTime",
                    "_value": "%s"
                }
                """.formatted(localDateTimeString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.asLocalDateTime()).isEqualTo(LocalDateTime.parse(localDateTimeString));
    }

    @ParameterizedTest
    @MethodSource("localDateTimes")
    void shouldDeserializeLocalDateTimeParametersWhenValueIsBeforeType(String localDateTimeString) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "_value": "%s",
                    "$type": "LocalDateTime"
                }
                """.formatted(localDateTimeString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.asLocalDateTime()).isEqualTo(LocalDateTime.parse(localDateTimeString));
    }

    @ParameterizedTest
    @MethodSource("invalidLocalDateTimes")
    void shouldRejectInvalidLocalDateTimeParameters(String localDateTimeString) throws JsonProcessingException {
        assertThatThrownBy(() ->
                        mapper.readValue("""
                {
                    "$type": "LocalDateTime",
                    "_value": "%s"
                }
                """.formatted(localDateTimeString), TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(Exception.class)
                .hasRootCauseInstanceOf(DateTimeException.class);
    }

    @Test
    void shouldRejectWhenValueNotPresent() throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "LocalDateTime"
                }
                """, TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    private static Stream<String> localDateTimes() {
        return Stream.of(
                "2015-07-04T19:32:24", "2025-12-01T12:30:00", "0800-01-01T00:00:00", "2025-12-01T12:30:00.999999999");
    }

    private static Stream<String> invalidLocalDateTimes() {
        return Stream.of(
                "not a local datetime",
                "2025-12-01",
                "12:30:00",
                "2025-12-01T12:30:00Z",
                "2025-12-01T12:30:00+01:00",
                "2025-12-01T12:30:00[Europe/Berlin]",
                "2025-13-01T12:30:00",
                "2025-12-01T24:00:00");
    }

    private static Stream<View> views() {
        return Stream.of(View.values())
                .filter(view -> view != View.PLAIN_JSON)
                .filter(view -> view.supports(CypherTypes.LocalDateTime));
    }
}
