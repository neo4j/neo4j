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
class TypedJsonStringQueryRequestCypherValueTest {
    private final ObjectMapper mapper;

    TypedJsonStringQueryRequestCypherValueTest(View view) {
        mapper = new ObjectMapper().registerModule(new TypedJsonRequestModule(view));
    }

    @ParameterizedTest
    @MethodSource("strings")
    void shouldDeserializeStringParameters(String stringValue) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "$type": "String",
                    "_value": "%s"
                }
                """.formatted(stringValue), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.asString()).isEqualTo(stringValue);
    }

    @ParameterizedTest
    @MethodSource("strings")
    void shouldDeserializeStringParametersWhenValueIsBeforeType(String stringValue) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "_value": "%s",
                    "$type": "String"
                }
                """.formatted(stringValue), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.asString()).isEqualTo(stringValue);
    }

    @Test
    void shouldDeserializeEmptyStringParameter() throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "$type": "String",
                    "_value": ""
                }
                """, TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.asString()).isEmpty();
    }

    @Test
    void shouldRejectWhenValueNotPresent() throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "String"
                }
                """, TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    private static Stream<String> strings() {
        return Stream.of("Hello", "Hello world", "123", "true", "ca3d9a43-09e3-4b66-9384-87ea25e27d01");
    }

    private static Stream<View> views() {
        return Stream.of(View.values())
                .filter(view -> view != View.PLAIN_JSON)
                .filter(view -> view.supports(CypherTypes.String));
    }
}
