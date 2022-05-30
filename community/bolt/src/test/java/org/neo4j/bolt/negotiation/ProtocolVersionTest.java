/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.negotiation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class ProtocolVersionTest {

    private static Stream<ProtocolVersion> versions() {
        return IntStream.range(3, 5)
                .boxed()
                .flatMap(major -> IntStream.range(0, 9).boxed().flatMap(minor -> IntStream.range(0, minor)
                        .mapToObj(range -> new ProtocolVersion(major, minor, range))));
    }

    private static int encode(ProtocolVersion version) {
        return (((int) version.range()) << 16) ^ (((int) version.minor()) << 8) ^ ((int) version.major());
    }

    @TestFactory
    Stream<DynamicTest> shouldEncodeVersions() {
        return versions()
                .map(input -> dynamicTest(input.toString(), () -> {
                    var expected = encode(input);
                    assertEquals(expected, input.encode());
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldDecodeVersions() {
        return versions()
                .map(expected -> dynamicTest(expected.toString(), () -> {
                    var encoded = encode(expected);
                    var actual = new ProtocolVersion(encoded);

                    assertEquals(expected, actual);
                }));
    }
}
