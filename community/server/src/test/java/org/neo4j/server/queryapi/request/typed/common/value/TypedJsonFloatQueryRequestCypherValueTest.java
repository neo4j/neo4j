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
class TypedJsonFloatQueryRequestCypherValueTest {
    private final ObjectMapper mapper;

    TypedJsonFloatQueryRequestCypherValueTest(View view) {
        mapper = new ObjectMapper().registerModule(new TypedJsonRequestModule(view));
    }

    @ParameterizedTest
    @MethodSource("floats")
    void shouldDeserializeFloatParameters(String floatString) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "$type": "Float",
                    "_value": "%s"
                }
                """.formatted(floatString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();

        var expectedValue = Double.parseDouble(floatString);
        if (Double.isNaN(expectedValue)) {
            assertThat(value.asDouble()).isNaN();
        } else {
            assertThat(value.asDouble()).isEqualTo(expectedValue);
        }
    }

    @ParameterizedTest
    @MethodSource("floats")
    void shouldDeserializeFloatParametersWhenValueIsBeforeType(String floatString) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "_value": "%s",
                    "$type": "Float"
                }
                """.formatted(floatString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();

        var expectedValue = Double.parseDouble(floatString);
        if (Double.isNaN(expectedValue)) {
            assertThat(value.asDouble()).isNaN();
        } else {
            assertThat(value.asDouble()).isEqualTo(expectedValue);
        }
    }

    @ParameterizedTest
    @MethodSource("invalidFloats")
    void shouldRejectInvalidFloatParameters(String floatString) throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "Float",
                    "_value": "%s"
                }
                """.formatted(floatString), TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(Exception.class)
                .hasRootCauseInstanceOf(NumberFormatException.class);
    }

    @ParameterizedTest
    @MethodSource("nonStringValues")
    void shouldRejectNonStringValues(String value) throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "Float",
                    "_value": %s
                }
                """.formatted(value), TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    @Test
    void shouldRejectWhenValueNotPresent() throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "Float"
                }
                """, TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    private static Stream<String> floats() {
        return Stream.of(
                "0",
                "1",
                "-1",
                "12.3",
                "-12.3",
                "1e3",
                "NaN",
                "Infinity",
                "-Infinity",
                String.valueOf(Double.MAX_VALUE),
                String.valueOf(Double.MIN_VALUE));
    }

    private static Stream<String> invalidFloats() {
        return Stream.of("not a float", "", "12,3");
    }

    private static Stream<String> nonStringValues() {
        return Stream.of("[]", "{}");
    }

    private static Stream<View> views() {
        return Stream.of(View.values())
                .filter(view -> view != View.PLAIN_JSON)
                .filter(view -> view.supports(CypherTypes.Float));
    }
}
