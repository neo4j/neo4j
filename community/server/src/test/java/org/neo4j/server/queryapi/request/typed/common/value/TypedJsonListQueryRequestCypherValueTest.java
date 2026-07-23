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
class TypedJsonListQueryRequestCypherValueTest {
    private final ObjectMapper mapper;

    TypedJsonListQueryRequestCypherValueTest(View view) {
        mapper = new ObjectMapper().registerModule(new TypedJsonRequestModule(view));
    }

    @Test
    void shouldDeserializeListParameter() throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "$type": "List",
                    "_value": [
                        {
                            "$type": "String",
                            "_value": "Hello"
                        },
                        {
                            "$type": "Integer",
                            "_value": "123"
                        },
                        {
                            "$type": "Boolean",
                            "_value": true
                        },
                        {
                            "$type": "Null",
                            "_value": null
                        }
                    ]
                }
                """, TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.asList()).containsExactly("Hello", 123L, true, null);
    }

    @Test
    void shouldDeserializeListParameterWhenValueIsBeforeType() throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "_value": [
                        {
                            "$type": "String",
                            "_value": "Hello"
                        },
                        {
                            "$type": "Integer",
                            "_value": "123"
                        }
                    ],
                    "$type": "List"
                }
                """, TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.asList()).containsExactly("Hello", 123L);
    }

    @Test
    void shouldDeserializeEmptyListParameter() throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "$type": "List",
                    "_value": []
                }
                """, TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.asList()).isEmpty();
    }

    @Test
    void shouldDeserializeNestedListParameter() throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "$type": "List",
                    "_value": [
                        {
                            "$type": "List",
                            "_value": [
                                {
                                    "$type": "String",
                                    "_value": "value"
                                }
                            ]
                        }
                    ]
                }
                """, TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.get(0).asList()).containsExactly("value");
    }

    @Test
    void shouldDeserializeMapNestedInListParameter() throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "$type": "List",
                    "_value": [
                        {
                            "$type": "Map",
                            "_value": {
                                "inner": {
                                    "$type": "String",
                                    "_value": "value"
                                }
                            }
                        }
                    ]
                }
                """, TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();
        assertThat(value.get(0).asMap()).containsEntry("inner", "value");
    }

    @ParameterizedTest
    @MethodSource("nonListValues")
    void shouldRejectNonListValues(String value) throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "List",
                    "_value": %s
                }
                """.formatted(value), TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    @Test
    void shouldRejectWhenValueNotPresent() throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "List"
                }
                """, TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    private static Stream<String> nonListValues() {
        return Stream.of("\"Hello\"", "123", "12.3", "true", "false", "{}");
    }

    private static Stream<View> views() {
        return Stream.of(View.values())
                .filter(view -> view != View.PLAIN_JSON)
                .filter(view -> view.supports(CypherTypes.List));
    }
}
