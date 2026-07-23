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
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.exceptions.TemporalParseException;
import org.neo4j.server.queryapi.request.typed.common.TypedJsonRequestModule;
import org.neo4j.server.queryapi.types.CypherTypes;
import org.neo4j.server.queryapi.types.View;
import org.neo4j.values.storable.DurationValue;

@ParameterizedClass
@MethodSource("views")
class TypedJsonDurationQueryRequestCypherValueTest {
    private final ObjectMapper mapper;

    TypedJsonDurationQueryRequestCypherValueTest(View view) {
        mapper = new ObjectMapper().registerModule(new TypedJsonRequestModule(view));
    }

    @ParameterizedTest
    @MethodSource("durations")
    void shouldDeserializeDurationParameters(String durationString) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "$type": "Duration",
                    "_value": "%s"
                }
                """.formatted(durationString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();

        var expected = DurationValue.parse(durationString);
        var duration = value.asIsoDuration();
        assertThat(duration.months()).isEqualTo(expected.get(ChronoUnit.MONTHS));
        assertThat(duration.days()).isEqualTo(expected.get(ChronoUnit.DAYS));
        assertThat(duration.seconds()).isEqualTo(expected.get(ChronoUnit.SECONDS));
        assertThat(duration.nanoseconds()).isEqualTo(expected.get(ChronoUnit.NANOS));
    }

    @ParameterizedTest
    @MethodSource("durations")
    void shouldDeserializeDurationParametersWhenValueIsBeforeType(String durationString) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "_value": "%s",
                    "$type": "Duration"
                }
                """.formatted(durationString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();

        var expected = DurationValue.parse(durationString);
        var duration = value.asIsoDuration();
        assertThat(duration.months()).isEqualTo(expected.get(ChronoUnit.MONTHS));
        assertThat(duration.days()).isEqualTo(expected.get(ChronoUnit.DAYS));
        assertThat(duration.seconds()).isEqualTo(expected.get(ChronoUnit.SECONDS));
        assertThat(duration.nanoseconds()).isEqualTo(expected.get(ChronoUnit.NANOS));
    }

    @ParameterizedTest
    @MethodSource("invalidDurations")
    void shouldRejectInvalidDurationParameters(String durationString) throws JsonProcessingException {
        assertThatThrownBy(
                        () -> mapper.readValue("""
                {
                    "$type": "Duration",
                    "_value": "%s"
                }
                """.formatted(durationString), TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(Exception.class)
                .hasRootCauseInstanceOf(TemporalParseException.class);
    }

    @ParameterizedTest
    @MethodSource("nonStringValues")
    void shouldRejectNonStringValues(String value) throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "Duration",
                    "_value": %s
                }
                """.formatted(value), TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    @Test
    void shouldRejectWhenValueNotPresent() throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "Duration"
                }
                """, TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    private static Stream<String> durations() {
        return Stream.of("P14DT16H12M", "P1Y2M3DT4H5M6S", "PT0S", "P1M", "P1DT1.123456789S");
    }

    private static Stream<String> invalidDurations() {
        return Stream.of("not a duration", "", "P", "PT", "P1X", "P1DT");
    }

    private static Stream<String> nonStringValues() {
        return Stream.of("[]", "{}");
    }

    private static Stream<View> views() {
        return Stream.of(View.values())
                .filter(view -> view != View.PLAIN_JSON)
                .filter(view -> view.supports(CypherTypes.Duration));
    }
}
