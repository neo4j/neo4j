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

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PropertyTypeSetTest {

    private static Stream<Arguments> descriptions() {
        return Stream.of(
                Arguments.of(List.of(), "NOTHING"),
                Arguments.of(List.of(SchemaValueType.DURATION), "DURATION"),
                Arguments.of(
                        List.of(
                                SchemaValueType.FLOAT,
                                SchemaValueType.INTEGER,
                                SchemaValueType.BOOLEAN,
                                SchemaValueType.BOOLEAN,
                                SchemaValueType.FLOAT),
                        "BOOLEAN | INTEGER | FLOAT"));
    }

    @ParameterizedTest
    @MethodSource("descriptions")
    void testUserDescription(List<SchemaValueType> types, String expected) {
        assertThat(PropertyTypeSet.of(types).userDescription()).isEqualTo(expected);
    }

    private static Stream<Arguments> setSizes() {
        return Stream.of(
                Arguments.of(List.of(), 0),
                Arguments.of(List.of(SchemaValueType.DURATION), 1),
                Arguments.of(
                        List.of(
                                SchemaValueType.FLOAT,
                                SchemaValueType.INTEGER,
                                SchemaValueType.BOOLEAN,
                                SchemaValueType.BOOLEAN,
                                SchemaValueType.FLOAT),
                        3));
    }

    @ParameterizedTest
    @MethodSource("setSizes")
    void testSize(List<SchemaValueType> types, int expectedSize) {
        assertThat(PropertyTypeSet.of(types).size()).isEqualTo(expectedSize);
    }

    @Test
    void testEquality() {
        var a = PropertyTypeSet.of(SchemaValueType.BOOLEAN, SchemaValueType.INTEGER);
        var b = PropertyTypeSet.of(SchemaValueType.INTEGER, SchemaValueType.BOOLEAN);
        assertThat(a).isEqualTo(b);

        var c = PropertyTypeSet.of(SchemaValueType.INTEGER, SchemaValueType.STRING);
        assertThat(a).isNotEqualTo(c);
    }

    @Test
    void testSetOperations() {
        var empty = PropertyTypeSet.of();
        var set1 = PropertyTypeSet.of(SchemaValueType.BOOLEAN);
        var set2 = PropertyTypeSet.of(SchemaValueType.INTEGER);
        var union = PropertyTypeSet.of(SchemaValueType.INTEGER, SchemaValueType.BOOLEAN);

        // Unions
        assertThat(set1.union(set2)).isEqualTo(union);
        assertThat(set2.union(set1)).isEqualTo(union);

        assertThat(union.union(set1)).isEqualTo(union);
        assertThat(set1.union(union)).isEqualTo(union);

        assertThat(empty.union(union)).isEqualTo(union);
        assertThat(union.union(empty)).isEqualTo(union);

        // Differences
        assertThat(union.difference(set1)).isEqualTo(set2);
        assertThat(union.difference(set2)).isEqualTo(set1);

        assertThat(union.difference(union)).isEqualTo(empty);
        assertThat(empty.difference(set2)).isEqualTo(empty);
        assertThat(union.difference(empty)).isEqualTo(union);

        // Intersections
        assertThat(union.intersection(set1)).isEqualTo(set1);
        assertThat(union.intersection(set2)).isEqualTo(set2);
        assertThat(union.intersection(empty)).isEqualTo(empty);
        assertThat(empty.intersection(union)).isEqualTo(empty);
        assertThat(union.intersection(union)).isEqualTo(union);
    }
}
