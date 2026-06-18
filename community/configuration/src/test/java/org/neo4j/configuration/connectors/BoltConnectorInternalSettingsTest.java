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
package org.neo4j.configuration.connectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BoltConnectorInternalSettingsTest {

    @Nested
    class ConfiguredProtocolVersionSettingsParserTest {

        @ParameterizedTest
        @MethodSource("provideValidProtocolVersions")
        void shouldSuccessfullyParseValidProtocolVersions(
                String version, BoltConnectorInternalSettings.ConfiguredProtocolVersion expectedVersion) {

            var configured = BoltConnectorInternalSettings.PROTOCOL_VERSION.parse(version);

            assertThat(configured).isEqualTo(expectedVersion);
        }

        @ParameterizedTest
        @MethodSource("provideInvalidProtocolVersions")
        void shouldFailOnInvalidProtocolVersions(String invalidVersion) {
            assertThatThrownBy(() -> BoltConnectorInternalSettings.PROTOCOL_VERSION.parse(invalidVersion))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        private static Stream<Arguments> provideValidProtocolVersions() {
            return Stream.of(
                    Arguments.of("5.8", v(5, 8)),
                    Arguments.of(" 5.7 ", v(5, 7)),
                    Arguments.of("3.6    ", v(3, 6)),
                    Arguments.of("      6.0 ", v(6, 0)));
        }

        private static BoltConnectorInternalSettings.ConfiguredProtocolVersion v(Integer major, Integer minor) {
            return new BoltConnectorInternalSettings.ConfiguredProtocolVersion(major, minor);
        }

        private static Stream<Arguments> provideInvalidProtocolVersions() {
            return Stream.of(
                    Arguments.of("5.8abc"),
                    Arguments.of("5,8"),
                    Arguments.of("5"),
                    Arguments.of("58"),
                    Arguments.of(""),
                    Arguments.of("    "),
                    Arguments.of("0x113.0x1244"),
                    Arguments.of("5.A"),
                    Arguments.of("A.5"),
                    Arguments.of("A.B"));
        }
    }
}
