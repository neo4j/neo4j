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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.server.queryapi.request.typed.common.TypedJsonRequestModule;
import org.neo4j.server.queryapi.types.CypherTypes;
import org.neo4j.server.queryapi.types.View;

@ParameterizedClass
@MethodSource("views")
class TypedJsonBooleanQueryRequestCypherValueTest {
    private final ObjectMapper mapper;

    TypedJsonBooleanQueryRequestCypherValueTest(View view) {
        mapper = new ObjectMapper().registerModule(new TypedJsonRequestModule(view));
    }

    @ParameterizedTest
    @MethodSource("booleans")
    void shouldDeserializeBooleanParameters(String booleanString, boolean expected) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "$type": "Boolean",
                    "_value": "%s"
                }
                """.formatted(booleanString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.asBoolean()).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("booleans")
    void shouldDeserializeBooleanParametersWhenValueIsBeforeType(String booleanString, boolean expected)
            throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "_value": "%s",
                    "$type": "Boolean"
                }
                """.formatted(booleanString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.asBoolean()).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("nonBooleanStrings")
    void shouldDeserializeNonTrueBooleanStringsAsFalse(String booleanString) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "$type": "Boolean",
                    "_value": "%s"
                }
                """.formatted(booleanString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.asBoolean()).isFalse();
    }

    @Test
    void shouldRejectWhenValueNotPresent() throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "Boolean"
                }
                """, TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    private static Stream<Arguments> booleans() {
        return Stream.of(
                Arguments.of("true", true),
                Arguments.of("TRUE", true),
                Arguments.of("false", false),
                Arguments.of("FALSE", false));
    }

    private static Stream<String> nonBooleanStrings() {
        return Stream.of("not a boolean", "", "yes", "no", "1", "0");
    }

    private static Stream<View> views() {
        return Stream.of(View.values())
                .filter(view -> view != View.PLAIN_JSON)
                .filter(view -> view.supports(CypherTypes.Boolean));
    }
}
