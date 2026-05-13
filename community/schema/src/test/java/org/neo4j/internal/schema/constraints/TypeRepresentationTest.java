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
import static org.neo4j.values.storable.Values.longValue;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@RandomSupportExtension
class TypeRepresentationTest {

    @Inject
    RandomSupport random;

    private static Stream<Arguments> expectedRepresentations() {
        return Stream.of(
                // Booleans
                Arguments.of(Values.TRUE, SchemaValueType.BOOLEAN),
                Arguments.of(Values.FALSE, SchemaValueType.BOOLEAN),
                Arguments.of(Values.booleanValue(false), SchemaValueType.BOOLEAN),
                Arguments.of(Values.booleanValue(true), SchemaValueType.BOOLEAN),

                // Strings
                Arguments.of(Values.charValue('c'), SchemaValueType.STRING),
                Arguments.of(Values.EMPTY_STRING, SchemaValueType.STRING),
                Arguments.of(Values.utf8Value("hello"), SchemaValueType.STRING),
                Arguments.of(Values.stringValue("hello"), SchemaValueType.STRING),

                // Numerical
                Arguments.of(Values.byteValue((byte) 1), SchemaValueType.INTEGER),
                Arguments.of(Values.shortValue((short) 10), SchemaValueType.INTEGER),
                Arguments.of(Values.intValue(10), SchemaValueType.INTEGER),
                Arguments.of(longValue(10), SchemaValueType.INTEGER),
                Arguments.of(Values.floatValue(1.0f), SchemaValueType.FLOAT),
                Arguments.of(Values.doubleValue(1.0f), SchemaValueType.FLOAT),

                // Date and time
                Arguments.of(Values.temporalValue(ZonedDateTime.now()), SchemaValueType.ZONED_DATETIME),
                Arguments.of(Values.temporalValue(OffsetDateTime.now()), SchemaValueType.ZONED_DATETIME),
                Arguments.of(Values.temporalValue(LocalDateTime.now()), SchemaValueType.LOCAL_DATETIME),
                Arguments.of(Values.temporalValue(LocalTime.now()), SchemaValueType.LOCAL_TIME),
                Arguments.of(Values.temporalValue(OffsetTime.now()), SchemaValueType.ZONED_TIME),
                Arguments.of(Values.temporalValue(LocalDate.now()), SchemaValueType.DATE),
                Arguments.of(Values.durationValue(Duration.ofSeconds(10)), SchemaValueType.DURATION),

                // Geometry
                Arguments.of(Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1, 2), SchemaValueType.POINT),

                // List of booleans
                Arguments.of(Values.of(new boolean[] {true, false}), SchemaValueType.LIST_BOOLEAN),

                // List of strings
                Arguments.of(Values.of(new char[] {'a', 'b', 'c'}), SchemaValueType.LIST_STRING),
                Arguments.of(Values.of(new String[] {"hello", "world"}), SchemaValueType.LIST_STRING),

                // List of numericals
                Arguments.of(Values.byteArray(new byte[] {1, 2, 3}), SchemaValueType.LIST_INTEGER),
                Arguments.of(Values.of(new short[] {1, 2, 3}), SchemaValueType.LIST_INTEGER),
                Arguments.of(Values.of(new int[] {1, 2, 3}), SchemaValueType.LIST_INTEGER),
                Arguments.of(Values.of(new long[] {1, 2, 3}), SchemaValueType.LIST_INTEGER),
                Arguments.of(Values.of(new float[] {1.0f, 2.0f}), SchemaValueType.LIST_FLOAT),
                Arguments.of(Values.of(new double[] {1.0f, 2.0f}), SchemaValueType.LIST_FLOAT),

                // List of dates and times
                Arguments.of(Values.of(new ZonedDateTime[] {ZonedDateTime.now()}), SchemaValueType.LIST_ZONED_DATETIME),
                Arguments.of(Values.of(new LocalDateTime[] {LocalDateTime.now()}), SchemaValueType.LIST_LOCAL_DATETIME),
                Arguments.of(Values.of(new LocalTime[] {LocalTime.now()}), SchemaValueType.LIST_LOCAL_TIME),
                Arguments.of(Values.of(new OffsetTime[] {OffsetTime.now()}), SchemaValueType.LIST_ZONED_TIME),
                Arguments.of(Values.of(new LocalDate[] {LocalDate.now()}), SchemaValueType.LIST_DATE),
                Arguments.of(Values.of(new Duration[] {Duration.ofSeconds(10)}), SchemaValueType.LIST_DURATION),
                Arguments.of(Values.pointArray(new Point[] {PointValue.MAX_VALUE}), SchemaValueType.LIST_POINT),

                // Special cases
                Arguments.of(Values.byteArray(new byte[] {}), SpecialTypes.LIST_NOTHING),
                Arguments.of(Values.NO_VALUE, SpecialTypes.NULL),
                Arguments.of(null, SpecialTypes.NULL),
                Arguments.of(Values.int8Vector(new byte[] {1}), VectorType.int8Vector(1)),
                Arguments.of(Values.int8Vector(new byte[] {1, 2, 3}), VectorType.int8Vector(3)),
                Arguments.of(Values.int16Vector(new short[] {1}), VectorType.int16Vector(1)),
                Arguments.of(Values.int16Vector(new short[] {1, 2, 3}), VectorType.int16Vector(3)),
                Arguments.of(Values.int32Vector(1), VectorType.int32Vector(1)),
                Arguments.of(Values.int32Vector(1, 2, 3), VectorType.int32Vector(3)),
                Arguments.of(Values.int64Vector(1), VectorType.int64Vector(1)),
                Arguments.of(Values.int64Vector(1, 2, 3), VectorType.int64Vector(3)),
                Arguments.of(Values.float32Vector(1), VectorType.float32Vector(1)),
                Arguments.of(Values.float32Vector(1, 2, 3), VectorType.float32Vector(3)),
                Arguments.of(Values.float64Vector(1), VectorType.float64Vector(1)),
                Arguments.of(Values.float64Vector(1, 2, 3), VectorType.float64Vector(3)));
    }

    @ParameterizedTest
    @MethodSource("expectedRepresentations")
    void shouldInferCorrectTypeForValue(Value value, TypeRepresentation expectedType) {
        assertThat(TypeRepresentation.infer(value)).isEqualTo(expectedType);
    }

    private static Stream<TypeRepresentation> types() {
        return Stream.of(
                        Arrays.stream(SchemaValueType.values()),
                        Arrays.stream(SpecialTypes.values()),
                        Stream.of(
                                VectorType.int8Vector(1), VectorType.int8Vector(3),
                                VectorType.int16Vector(1), VectorType.int16Vector(3),
                                VectorType.int32Vector(1), VectorType.int32Vector(3),
                                VectorType.int64Vector(1), VectorType.int64Vector(3),
                                VectorType.float32Vector(1), VectorType.float32Vector(3),
                                VectorType.float64Vector(1), VectorType.float64Vector(3)))
                .flatMap(Function.identity());
    }

    @ParameterizedTest
    @MethodSource("types")
    void shouldReturnZeroWhenComparingTypeToItself(TypeRepresentation type) {
        assertThat(TypeRepresentation.compare(type, type)).isZero();
    }

    private static Stream<Arguments> illegalCombinations() {
        return Stream.of(
                Arguments.of(PropertyTypeSet.of(), Values.of(1L)),
                Arguments.of(PropertyTypeSet.of(), Values.of("HELLO")),
                Arguments.of(PropertyTypeSet.of(SchemaValueType.STRING), Values.of(1L)),
                Arguments.of(
                        PropertyTypeSet.of(SchemaValueType.LIST_BOOLEAN, SchemaValueType.LIST_INTEGER),
                        Values.of("hello")),
                Arguments.of(
                        PropertyTypeSet.of(SchemaValueType.LIST_BOOLEAN, SchemaValueType.LIST_INTEGER),
                        Values.of(new float[] {1.0f})),
                Arguments.of(
                        PropertyTypeSet.of(SchemaValueType.LIST_BOOLEAN, SchemaValueType.FLOAT), Values.of("hello")));
    }

    private static Stream<Arguments> legalCombinations() {
        return Stream.of(
                Arguments.of(PropertyTypeSet.of(SchemaValueType.STRING), Values.of("Hello")),
                Arguments.of(PropertyTypeSet.of(SchemaValueType.INTEGER, SchemaValueType.FLOAT), Values.of(1.0f)),
                Arguments.of(PropertyTypeSet.of(SchemaValueType.INTEGER, SchemaValueType.FLOAT), Values.NO_VALUE),
                Arguments.of(PropertyTypeSet.of(SchemaValueType.LIST_INTEGER), Values.of(new short[] {1})),
                Arguments.of(PropertyTypeSet.of(SchemaValueType.LIST_INTEGER), Values.of(new short[] {})),
                Arguments.of(PropertyTypeSet.of(SchemaValueType.INTEGER, SchemaValueType.FLOAT), Values.NO_VALUE));
    }

    @ParameterizedTest
    @MethodSource("illegalCombinations")
    void shouldPreventIllegalCombinations(PropertyTypeSet set, Value value) {
        assertThat(TypeRepresentation.disallows(set, value)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("legalCombinations")
    void shouldNotPreventLegalCombinations(PropertyTypeSet set, Value value) {
        assertThat(TypeRepresentation.disallows(set, value)).isFalse();
    }

    @Test
    void shouldOrderAllTypesAccordingToCip100Spec() {
        // GIVEN
        List<TypeRepresentation> entries = types().collect(Collectors.toCollection(ArrayList<TypeRepresentation>::new));
        Collections.shuffle(entries, random.random());

        // WHEN
        Set<TypeRepresentation> set = new TreeSet<>(TypeRepresentation::compare);
        set.addAll(entries);
        String[] actual = set.stream().map(TypeRepresentation::userDescription).toArray(String[]::new);

        // THEN
        String[] expected = new String[] {
            "NULL",
            "BOOLEAN",
            "STRING",
            "UUID",
            "INTEGER",
            "FLOAT",
            "DATE",
            "LOCAL TIME",
            "ZONED TIME",
            "LOCAL DATETIME",
            "ZONED DATETIME",
            "DURATION",
            "POINT",
            "VECTOR<INTEGER8 NOT NULL>(1)",
            "VECTOR<INTEGER8 NOT NULL>(3)",
            "VECTOR<INTEGER16 NOT NULL>(1)",
            "VECTOR<INTEGER16 NOT NULL>(3)",
            "VECTOR<INTEGER32 NOT NULL>(1)",
            "VECTOR<INTEGER32 NOT NULL>(3)",
            "VECTOR<INTEGER NOT NULL>(1)",
            "VECTOR<INTEGER NOT NULL>(3)",
            "VECTOR<FLOAT32 NOT NULL>(1)",
            "VECTOR<FLOAT32 NOT NULL>(3)",
            "VECTOR<FLOAT NOT NULL>(1)",
            "VECTOR<FLOAT NOT NULL>(3)",
            "LIST<NOTHING>",
            "LIST<BOOLEAN NOT NULL>",
            "LIST<STRING NOT NULL>",
            "LIST<UUID NOT NULL>",
            "LIST<INTEGER NOT NULL>",
            "LIST<FLOAT NOT NULL>",
            "LIST<DATE NOT NULL>",
            "LIST<LOCAL TIME NOT NULL>",
            "LIST<ZONED TIME NOT NULL>",
            "LIST<LOCAL DATETIME NOT NULL>",
            "LIST<ZONED DATETIME NOT NULL>",
            "LIST<DURATION NOT NULL>",
            "LIST<POINT NOT NULL>",
            "LIST<ANY>",
            "ANY",
        };
        assertThat(actual).isEqualTo(expected);
    }

    private static Stream<ConstrainableType> constrainableTypes() {
        return Stream.of(
                        Arrays.stream(SchemaValueType.values()),
                        Stream.of(
                                VectorType.int8Vector(1), VectorType.int8Vector(3),
                                VectorType.int16Vector(1), VectorType.int16Vector(3),
                                VectorType.int32Vector(1), VectorType.int32Vector(3),
                                VectorType.int64Vector(1), VectorType.int64Vector(3),
                                VectorType.float32Vector(1), VectorType.float32Vector(3),
                                VectorType.float64Vector(1), VectorType.float64Vector(3)))
                .flatMap(Function.identity());
    }

    @ParameterizedTest
    @MethodSource("constrainableTypes")
    void shouldDeserializeBackToOriginalType(ConstrainableType type) {
        assertThat(type).isEqualTo(TypeRepresentation.deserialize(type.serialize()));
    }
}
