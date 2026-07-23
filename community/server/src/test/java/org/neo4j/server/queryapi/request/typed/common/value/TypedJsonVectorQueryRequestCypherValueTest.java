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
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.types.Float32Vector;
import org.neo4j.driver.types.Float64Vector;
import org.neo4j.driver.types.Int16Vector;
import org.neo4j.driver.types.Int32Vector;
import org.neo4j.driver.types.Int64Vector;
import org.neo4j.driver.types.Int8Vector;
import org.neo4j.driver.types.Vector;
import org.neo4j.server.queryapi.exception.UnsupportedTypeException;
import org.neo4j.server.queryapi.request.typed.common.TypedJsonRequestModule;
import org.neo4j.server.queryapi.types.CypherTypes;
import org.neo4j.server.queryapi.types.View;

@ParameterizedClass
@MethodSource("views")
class TypedJsonVectorQueryRequestCypherValueTest {
    private final ObjectMapper mapper;

    TypedJsonVectorQueryRequestCypherValueTest(View view) {
        mapper = new ObjectMapper().registerModule(new TypedJsonRequestModule(view));
    }

    private final Map<String, String> coordinatesTypeToVectorType = Map.of(
            "INT8", "byte",
            "INT16", "short",
            "INT32", "int",
            "INT64", "long",
            "FLOAT32", "float",
            "FLOAT64", "double");

    @ParameterizedTest
    @MethodSource("vectors")
    void shouldDeserializeVectorParameters(String coordinatesType, List<String> coordinates) throws Exception {
        var cypherValue = mapper.readValue(
                """
                {
                    "$type": "Vector",
                    "_value": {
                      "coordinatesType": "%s",
                      "coordinates": [%s]
                    }
                }
                """.formatted(
                        coordinatesType,
                        String.join(
                                ", ",
                                coordinates.stream().map("\"%s\""::formatted).toList())),
                TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();

        var vector = value.asVector();
        assertThat(vector.elementType()).hasToString(coordinatesTypeToVectorType.get(coordinatesType));
        assertThat(vector.length()).isEqualTo(coordinates.size());
        assertCoordinates(coordinates, vector);
    }

    @ParameterizedTest
    @MethodSource("vectors")
    void shouldDeserializeVectorParametersWhenCoordinatesTypeIsAfterCoordinates(
            String coordinatesType, List<String> coordinates) throws Exception {
        var cypherValue = mapper.readValue(
                """
                {
                    "$type": "Vector",
                    "_value": {
                      "coordinates": [%s],
                      "coordinatesType": "%s"
                    }
                }
                """.formatted(
                        String.join(
                                ", ",
                                coordinates.stream().map("\"%s\""::formatted).toList()),
                        coordinatesType),
                TypedJsonQueryRequestCypherValue.class);

        var value = cypherValue.value();
        assertThat(value).isNotNull();

        var vector = value.asVector();
        assertThat(vector.elementType()).hasToString(coordinatesTypeToVectorType.get(coordinatesType));
        assertThat(vector.length()).isEqualTo(coordinates.size());
        assertCoordinates(coordinates, vector);
    }

    @ParameterizedTest
    @MethodSource("invalidVectors")
    void shouldRejectInvalidVectorCoordinates(String coordinatesType, List<String> coordinates)
            throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue(
                        """
                {
                    "$type": "Vector",
                    "_value": {
                      "coordinatesType": "%s",
                      "coordinates": [%s]
                    }
                }
                """.formatted(
                                        coordinatesType,
                                        String.join(
                                                ",",
                                                coordinates.stream()
                                                        .map("\"%s\""::formatted)
                                                        .toList())),
                        TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(Exception.class)
                .hasRootCauseExactlyInstanceOf(NumberFormatException.class);
    }

    @ParameterizedTest
    @MethodSource("invalidVectors")
    void shouldRejectNotPresentVectorCoordinates(String coordinatesType) throws JsonProcessingException {
        assertThatThrownBy(
                        () -> mapper.readValue("""
                {
                    "$type": "Vector",
                    "_value": {
                      "coordinatesType": "%s"
                    }
                }
                """.formatted(coordinatesType), TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    @Test
    void shouldRejectWhenValueNotPresent() throws JsonProcessingException {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "Vector"
                }
                """, TypedJsonQueryRequestCypherValue.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    @ParameterizedTest
    @MethodSource("unsupportedCoordinateTypes")
    void shouldRejectUnsupportedCoordinateTypes(String coordinatesType) {
        assertThatThrownBy(
                        () -> mapper.readValue("""
                {
                    "$type": "Vector",
                    "_value": {
                      "coordinatesType": "%s",
                      "coordinates": ["1"]
                    }
                }
                """.formatted(coordinatesType), TypedJsonQueryRequestCypherValue.class))
                .hasRootCauseInstanceOf(UnsupportedTypeException.class)
                .hasMessageContaining("Type " + coordinatesType + " is not supported.");
    }

    @ParameterizedTest
    @MethodSource("unsupportedCoordinateTypes")
    void shouldRejectUnsupportedCoordinateTypesAndNoCoordinates(String coordinatesType) {
        assertThatThrownBy(
                        () -> mapper.readValue("""
                {
                    "$type": "Vector",
                    "_value": {
                      "coordinatesType": "%s"
                    }
                }
                """.formatted(coordinatesType), TypedJsonQueryRequestCypherValue.class))
                .hasRootCauseInstanceOf(UnsupportedTypeException.class)
                .hasMessageContaining("Type " + coordinatesType + " is not supported.");
    }

    @Test
    void shouldRejectUnsupportedCoordinateTypesMissing() {
        assertThatThrownBy(() -> mapper.readValue("""
                {
                    "$type": "Vector",
                    "_value": {
                      "coordinates": ["1"]
                    }
                }
                """, TypedJsonQueryRequestCypherValue.class))
                .hasRootCauseInstanceOf(UnsupportedTypeException.class)
                .hasMessageContaining("Type null is not supported.");
    }

    private static void assertCoordinates(List<String> coordinates, Vector vector) {
        switch (vector) {
            case Int8Vector int8Vector ->
                assertThat(int8Vector.toArray())
                        .containsExactly(
                                coordinates.stream().mapToInt(Byte::parseByte).toArray());
            case Int16Vector int16Vector ->
                assertThat(int16Vector.toArray())
                        .containsExactly(
                                coordinates.stream().mapToInt(Short::parseShort).toArray());
            case Int32Vector int32Vector ->
                assertThat(int32Vector.toArray())
                        .containsExactly(
                                coordinates.stream().mapToInt(Integer::parseInt).toArray());
            case Int64Vector int64Vector ->
                assertThat(int64Vector.toArray())
                        .containsExactly(
                                coordinates.stream().mapToLong(Long::parseLong).toArray());
            case Float32Vector float32Vector ->
                assertThat(float32Vector.toArray())
                        .containsExactly(
                                coordinates.stream().map(Float::parseFloat).toArray(Float[]::new));
            case Float64Vector float64Vector ->
                assertThat(float64Vector.toArray())
                        .containsExactly(coordinates.stream()
                                .mapToDouble(Double::parseDouble)
                                .toArray());
            default ->
                throw new IllegalArgumentException(
                        "Unsupported vector type: " + vector.getClass().getSimpleName());
        }
    }

    private static Stream<Arguments> vectors() {
        return Stream.of(
                Arguments.of("INT8", List.of("-128", "0", "127")),
                Arguments.of("INT16", List.of("-32768", "0", "32767")),
                Arguments.of("INT32", List.of("-2147483648", "0", "2147483647")),
                Arguments.of("INT64", List.of("-9223372036854775808", "0", "9223372036854775807")),
                Arguments.of("FLOAT32", List.of("1.5", "2.5")),
                Arguments.of("FLOAT64", List.of("1.5", "2.5")));
    }

    private static Stream<Arguments> invalidVectors() {
        return Stream.of(
                Arguments.of("INT8", List.of("128")),
                Arguments.of("INT16", List.of("32768")),
                Arguments.of("INT32", List.of("2147483648")),
                Arguments.of("INT64", List.of("9223372036854775808")),
                Arguments.of("FLOAT32", List.of("not a float")),
                Arguments.of("FLOAT64", List.of("not a double")));
    }

    private static Stream<String> unsupportedCoordinateTypes() {
        return Stream.of("INT128", "FLOAT128", "BANANAS");
    }

    private static Stream<View> views() {
        return Stream.of(View.values())
                .filter(view -> view != View.PLAIN_JSON)
                .filter(view -> view.supports(CypherTypes.Vector));
    }
}
