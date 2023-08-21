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
package org.neo4j.bolt.negotiation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
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

    @TestFactory
    Stream<DynamicTest> shouldCheckVersionEquality() {
        return versions()
                .map(expected -> dynamicTest(expected.toString(), () -> {
                    var copy = new ProtocolVersion(expected.major(), expected.minor(), expected.range());

                    Assertions.assertThat(expected).isEqualTo(copy);

                    versions().forEach(it -> {
                        if (it.major() == expected.major()
                                && it.minor() == expected.minor()
                                && it.range() == expected.range()) {
                            return;
                        }

                        Assertions.assertThat(expected)
                                .describedAs("it does not equal a mismatching version")
                                .isNotEqualTo(it);
                    });
                }));
    }

    @Test
    void shouldCompareVersions() {
        var versions = List.of(
                new ProtocolVersion(4, 0),
                new ProtocolVersion(4, 1),
                new ProtocolVersion(4, 2, 2),
                new ProtocolVersion(5, 0),
                new ProtocolVersion(5, 1),
                new ProtocolVersion(5, 2, 1),
                new ProtocolVersion(5, 3));

        for (var current : versions) {
            var encounteredSelf = false;

            for (var other : versions) {
                if (other.equals(current)) {
                    Assertions.assertThat(current.compareTo(other)).isZero();
                    Assertions.assertThat(other.compareTo(current)).isZero();

                    var copy = new ProtocolVersion(other.major(), other.minor(), other.range());

                    Assertions.assertThat(current.compareTo(copy)).isZero();
                    Assertions.assertThat(copy.compareTo(current)).isZero();

                    encounteredSelf = true;
                    continue;
                }

                if (!encounteredSelf) {
                    Assertions.assertThat(current.compareTo(other)).isPositive();
                    Assertions.assertThat(other.compareTo(current)).isNegative();
                } else {
                    Assertions.assertThat(current.compareTo(other)).isNegative();
                    Assertions.assertThat(other.compareTo(current)).isPositive();
                }
            }
        }
    }
}
