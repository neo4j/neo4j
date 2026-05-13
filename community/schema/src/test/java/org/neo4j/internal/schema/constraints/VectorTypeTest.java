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
package org.neo4j.internal.schema.constraints;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@RandomSupportExtension
class VectorTypeTest {
    static Stream<Arguments> values() {
        return Stream.of(
                Arguments.of(Values.int8Vector(new byte[] {2, 3, 5, 7, 11}), VectorType.int8Vector(5)),
                Arguments.of(Values.int16Vector(new short[] {2, 3, 5, 7, 11}), VectorType.int16Vector(5)),
                Arguments.of(Values.int32Vector(2, 3, 5, 7, 11), VectorType.int32Vector(5)),
                Arguments.of(Values.int64Vector(2, 3, 5, 7, 11), VectorType.int64Vector(5)),
                Arguments.of(Values.float32Vector(2, 3, 5, 7, 11), VectorType.float32Vector(5)),
                Arguments.of(Values.float64Vector(2, 3, 5, 7, 11), VectorType.float64Vector(5)));
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldReturnInternedInstanceWhenInferringVectorType(Value val, ConstrainableType expected) {
        TypeRepresentation actual = TypeRepresentation.infer(val);
        // Check object identity
        assertThat(actual).isSameAs(expected);
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldReturnInternedInstanceWhenDeserializingVectorType(Value ignored, ConstrainableType expected) {
        ConstrainableType actual = TypeRepresentation.deserialize(expected.serialize());
        // Check object identity
        assertThat(actual).isSameAs(expected);
    }

    @ParameterizedTest
    @MethodSource("historicalSerializations")
    void shouldDeserializeHistoricalSerializationFormat(String serialized, VectorType expected) {
        assertThat(TypeRepresentation.deserialize(serialized)).isSameAs(expected);
    }

    static Stream<Arguments> historicalSerializations() {
        return Stream.of(
                Arguments.of("VECTOR[coordinate=INTEGER8, dimensions=1234]", VectorType.int8Vector(1234)),
                Arguments.of("VECTOR[coordinate=INTEGER16, dimensions=1234]", VectorType.int16Vector(1234)),
                Arguments.of("VECTOR[coordinate=INTEGER32, dimensions=1234]", VectorType.int32Vector(1234)),
                Arguments.of("VECTOR[coordinate=INTEGER64, dimensions=1234]", VectorType.int64Vector(1234)),
                Arguments.of("VECTOR[coordinate=FLOAT32, dimensions=1234]", VectorType.float32Vector(1234)),
                Arguments.of("VECTOR[coordinate=FLOAT64, dimensions=1234]", VectorType.float64Vector(1234)));
    }

    @ParameterizedTest
    @MethodSource("invalidSerializations")
    void shouldThrowOnMalformedSerializationString(String serialized) {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> TypeRepresentation.deserialize(serialized));
    }

    static Stream<String> invalidSerializations() {
        return Stream.of(
                "VECTOR[INTEGER8, 1234]", // Missing labels
                "vector[coordinate=INTEGER16, dimensions=1234]", // Invalid container
                "VECTOR[coordinate=INTEGER11, dimensions=1234]", // Invalid CoordinateType
                "VECTOR[coordinate=INTEGER64, dimensions=1.232]", // Invalid dimension
                "");
    }
}
