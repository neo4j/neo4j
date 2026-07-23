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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.server.queryapi.exception.UnsupportedTypeException;
import org.neo4j.server.queryapi.request.typed.common.TypedJsonRequestModule;
import org.neo4j.server.queryapi.types.View;

@ParameterizedClass
@MethodSource("views")
class TypedJsonQueryRequestCypherValueTest {
    private final ObjectMapper mapper;

    TypedJsonQueryRequestCypherValueTest(View view) {
        mapper = new ObjectMapper().registerModule(new TypedJsonRequestModule(view));
    }

    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    void shouldRejectUnsupportedTypes(String type) {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "%s",
                    "_value": "Ops wrong value"
                }
                """.formatted(type), TypedJsonQueryRequestCypherValue.class))
                .hasRootCauseInstanceOf(UnsupportedTypeException.class)
                .hasMessageContaining("Type " + type + " is not supported.");
    }

    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    void shouldRejectUnsupportedTypesAndNoValue(String type) {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "%s"
                }
                """.formatted(type), TypedJsonQueryRequestCypherValue.class))
                .hasRootCauseInstanceOf(UnsupportedTypeException.class)
                .hasMessageContaining("Type " + type + " is not supported.");
    }

    void shouldRejectUnsupportedTypeMissing() {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "_value": "Ops wrong value"
                }
                """, TypedJsonQueryRequestCypherValue.class))
                .hasRootCauseInstanceOf(UnsupportedTypeException.class)
                .hasMessageContaining("Type null is not supported.");
    }

    private static Stream<String> unsupportedTypes() {
        return Stream.of("Unsupported", "Bananas", "NotAType");
    }

    private static Stream<View> views() {
        return Stream.of(View.values()).filter(view -> view != View.PLAIN_JSON);
    }
}
