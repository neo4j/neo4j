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
package org.neo4j.genai.util;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class HashingTest {
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    @ParameterizedTest
    @MethodSource
    void knownHashes(byte[] input, String hashHex) {
        final var hash = Hashing.sha256(input);
        assertThat(HEX_FORMAT.formatHex(hash)).isEqualTo(hashHex);
    }

    private static Stream<Arguments> knownHashes() {
        // confirmed externally with known source and/or `sha256sum`
        return Stream.of(
                Arguments.of(
                        Named.of("empty", EMPTY_BYTE_ARRAY),
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Arguments.of(
                        Named.of("foo-bar-baz", "foo-bar-baz".getBytes(StandardCharsets.UTF_8)),
                        "269dce1a5bb90188b2d9cf542a7c30e410c7d8251e34a97bfea56062df51ae23"),
                Arguments.of(
                        Named.of(
                                "Lorem ipsum...",
                                "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                                        .getBytes(StandardCharsets.UTF_8)),
                        "973153f86ec2da1748e63f0cf85b89835b42f8ee8018c549868a1308a19f6ca3"));
    }
}
