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
class TypedJsonPointQueryRequestCypherValueTest {
    private final ObjectMapper mapper;

    TypedJsonPointQueryRequestCypherValueTest(View view) {
        mapper = new ObjectMapper().registerModule(new TypedJsonRequestModule(view));
    }

    @ParameterizedTest
    @MethodSource("points")
    void shouldDeserializePointParameters(String pointString, int srid, double x, double y, Double z) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "$type": "Point",
                    "_value": "%s"
                }
                """.formatted(pointString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();

        var point = value.asPoint();
        assertThat(point.srid()).isEqualTo(srid);
        assertThat(point.x()).isEqualTo(x);
        assertThat(point.y()).isEqualTo(y);
        if (z == null) {
            assertThat(point.z()).isNaN();
        } else {
            assertThat(point.z()).isEqualTo(z);
        }
    }

    @ParameterizedTest
    @MethodSource("points")
    void shouldDeserializePointParametersWhenValueIsBeforeType(
            String pointString, int srid, double x, double y, Double z) throws Exception {
        var cypherValue = mapper.readValue("""
                {
                    "_value": "%s",
                    "$type": "Point"
                }
                """.formatted(pointString), TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();

        var point = value.asPoint();
        assertThat(point.srid()).isEqualTo(srid);
        assertThat(point.x()).isEqualTo(x);
        assertThat(point.y()).isEqualTo(y);
        if (z == null) {
            assertThat(point.z()).isNaN();
        } else {
            assertThat(point.z()).isEqualTo(z);
        }
    }

    @ParameterizedTest
    @MethodSource("invalidPoints")
    void shouldRejectInvalidPointParameters(String pointString) throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "Point",
                    "_value": "%s"
                }
                """.formatted(pointString), TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(Exception.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectWhenValueNotPresent() throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "Point"
                }
                """, TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    private static Stream<Arguments> points() {
        return Stream.of(
                Arguments.of("SRID=7203;POINT (2.3 4.5)", 7203, 2.3, 4.5, null),
                Arguments.of("SRID=9157;POINT Z (2.3 4.5 6.7)", 9157, 2.3, 4.5, 6.7),
                Arguments.of("SRID=4326;POINT (2.3 4.5)", 4326, 2.3, 4.5, null),
                Arguments.of("SRID=4979;POINT Z (2.3 4.5 6.7)", 4979, 2.3, 4.5, 6.7));
    }

    private static Stream<String> invalidPoints() {
        return Stream.of(
                "not a point",
                "POINT (2.3 4.5)",
                "SRID=7203;POINT ()",
                "SRID=7203;POINT (2.3)",
                "SRID=7203;POINT (x 4.5)",
                "SRID=7203;POINT (2.3 y)",
                "SRID=7203;POINT Z (2.3 4.5 z)");
    }

    private static Stream<View> views() {
        return Stream.of(View.values())
                .filter(view -> view != View.PLAIN_JSON)
                .filter(view -> view.supports(CypherTypes.Point));
    }
}
