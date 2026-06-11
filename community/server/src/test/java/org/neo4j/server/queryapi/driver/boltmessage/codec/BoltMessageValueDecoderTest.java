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
package org.neo4j.server.queryapi.driver.boltmessage.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.server.queryapi.driver.boltmessage.codec.BoltMessageValueEncoderTest.testValues;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.FloatValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.VirtualValues;

class BoltMessageValueDecoderTest {

    public static Stream<Arguments> values() {
        return Stream.concat(
                testValues()
                        .map(inputArgs ->
                                Arguments.of(inputArgs.get()[1], inputArgs.get()[0])),
                Stream.of(
                        Arguments.of(
                                VirtualValues.nodeValue(
                                        1,
                                        "elementId",
                                        Values.stringArray("label1"),
                                        VirtualValues.map(
                                                new String[] {"integer"}, new AnyValue[] {Values.intValue(123)})),
                                new NodeValue(new InternalNode(
                                        1, "elementId", List.of("label1"), Map.of("integer", new IntegerValue(123))))),
                        Arguments.of(
                                VirtualValues.relationshipValue(
                                        1,
                                        "elementId",
                                        VirtualValues.node(2, "startElementId"),
                                        VirtualValues.node(3, "endElementId"),
                                        Values.stringValue("KNOWS"),
                                        VirtualValues.map(
                                                new String[] {"float"}, new AnyValue[] {Values.floatValue(123.4f)})),
                                new RelationshipValue(new InternalRelationship(
                                        1,
                                        "elementId",
                                        2,
                                        "startElementId",
                                        3,
                                        "endElementId",
                                        "KNOWS",
                                        Map.of("float", new FloatValue(123.4f))))),
                        Arguments.of(
                                VirtualValues.path(
                                        new org.neo4j.values.virtual.NodeValue[] {
                                            VirtualValues.nodeValue(
                                                    2,
                                                    "startElementId",
                                                    Values.stringArray("Person"),
                                                    VirtualValues.map(
                                                            new String[] {"integer"},
                                                            new AnyValue[] {Values.intValue(123)})),
                                            VirtualValues.nodeValue(
                                                    3,
                                                    "endElementId",
                                                    Values.stringArray("Person"),
                                                    VirtualValues.map(
                                                            new String[] {"integer "},
                                                            new AnyValue[] {Values.intValue(321)}))
                                        },
                                        new org.neo4j.values.virtual.RelationshipValue[] {
                                            VirtualValues.relationshipValue(
                                                    1,
                                                    "elementId",
                                                    VirtualValues.node(2, "startElementId"),
                                                    VirtualValues.node(3, "endElementId"),
                                                    Values.stringValue("KNOWS"),
                                                    VirtualValues.map(
                                                            new String[] {"from"},
                                                            new AnyValue[] {Values.stringValue("work")}))
                                        }),
                                new PathValue(new InternalPath(
                                        new InternalNode(
                                                2,
                                                "startElementId",
                                                List.of("Person"),
                                                Map.of("integer", new IntegerValue(123))),
                                        new InternalRelationship(
                                                1,
                                                "elementId",
                                                2,
                                                "startElementId",
                                                3,
                                                "endElementId",
                                                "KNOWS",
                                                Map.of("from", new StringValue("work"))),
                                        new InternalNode(
                                                3,
                                                "endElementId",
                                                List.of("Person"),
                                                Map.of("integer", new IntegerValue(123))))))));
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldDecodeValues(AnyValue inputValue, Value expectedValue) {
        // Paths can't be recursive compared correctly
        if (expectedValue instanceof PathValue) {
            assertThat(BoltMessageValueDecoder.decode(inputValue)).isEqualTo(expectedValue);
        } else {
            assertThat(BoltMessageValueDecoder.decode(inputValue))
                    .usingRecursiveComparison()
                    .isEqualTo(expectedValue);
        }
    }
}
