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
import java.time.LocalTime;
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
class TypedJsonLocalTimeQueryRequestCypherValueTest {
    private final ObjectMapper mapper;

    TypedJsonLocalTimeQueryRequestCypherValueTest(View view) {
        mapper = new ObjectMapper().registerModule(new TypedJsonRequestModule(view));
    }

    @ParameterizedTest
    @MethodSource("localTimes")
    void shouldDeserializeLocalTimeParameters(String localTimeString) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "$type": "LocalTime",
                    "_value": "%s"
                }
                """.formatted(localTimeString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.asLocalTime()).isEqualTo(LocalTime.parse(localTimeString));
    }

    @ParameterizedTest
    @MethodSource("localTimes")
    void shouldDeserializeLocalTimeParametersWhenValueIsBeforeType(String localTimeString) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "_value": "%s",
                    "$type": "LocalTime"
                }
                """.formatted(localTimeString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.asLocalTime()).isEqualTo(LocalTime.parse(localTimeString));
    }

    @ParameterizedTest
    @MethodSource("invalidLocalTimes")
    void shouldRejectInvalidLocalTimeParameters(String localTimeString) throws JsonProcessingException {
        assertThatThrownBy(
                        () -> mapper.readValue("""
                {
                    "$type": "LocalTime",
                    "_value": "%s"
                }
                """.formatted(localTimeString), TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(Exception.class)
                .hasRootCauseInstanceOf(DateTimeException.class);
    }

    @Test
    void shouldRejectWhenValueNotPresent() throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "LocalTime"
                }
                """, TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    private static Stream<String> localTimes() {
        return Stream.of("12:50:35.556", "00:00:00", "23:59:59.999999999");
    }

    private static Stream<String> invalidLocalTimes() {
        return Stream.of("not a local time", "12:50:35.556+01:00", "24:00:00", "12:60:35", "12:50:60");
    }

    private static Stream<View> views() {
        return Stream.of(View.values())
                .filter(view -> view != View.PLAIN_JSON)
                .filter(view -> view.supports(CypherTypes.LocalTime));
    }
}
