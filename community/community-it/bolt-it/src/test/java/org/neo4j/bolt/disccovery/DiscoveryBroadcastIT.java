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
package org.neo4j.bolt.disccovery;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.regex.Pattern;
import org.neo4j.bolt.protocol.common.connector.transport.NioConnectorTransport;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.transport.IncludeTransport;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.test.connection.setup.SettingBuilder;
import org.neo4j.bolt.testing.assertions.discovery.BeaconSignalAssertions;
import org.neo4j.bolt.testing.assertions.discovery.DiscoveryAssertions;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.client.discovery.DiscoveryTestClient;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class DiscoveryBroadcastIT {

    private static final Pattern DBMS_ID_PATTERN = Pattern.compile("^[A-Fa-f0-9]+$");
    private static final Pattern NODE_ID_PATTERN =
            Pattern.compile("^([A-Fa-f0-9]{8})-([A-Fa-f0-9]{4})-([A-Fa-f0-9]{4})-([A-Fa-f0-9]{4})-([A-Fa-f0-9]{12})$");

    @SettingsFunction
    static void customizeSettings(SettingBuilder settings) {
        settings.set(BoltConnector.enable_discovery, true);
        settings.set(BoltConnector.discovery_broadcast_interval, Duration.ofSeconds(5));
        settings.set(BoltConnector.discovery_broadcast_jitter, 0);
        settings.set(BoltConnector.advertised_address, new SocketAddress("neo.example.org", 7687));
    }

    // FIXME: We're abusing the test extension a little here - Accommodate this
    @TransportTest
    @IncludeTransport(value = TransportType.TCP)
    void shouldBroadcast() throws InterruptedException {
        var client = new DiscoveryTestClient(new NioConnectorTransport());

        DiscoveryAssertions.assertThat(client)
                .receivesBeaconSignal(
                        Duration.ofSeconds(30),
                        BeaconSignalAssertions.create()
                                // expect an average signal period of 5 seconds with a maximum deviation of +- 2 seconds
                                .hasAverageSignalPeriod(Duration.ofSeconds(5), Duration.ofSeconds(2))
                                .hasMagicNumber(0xDEADB017)
                                .hasVarInt(0x01)
                                .hasOpcode(0x01)
                                .hasString((dbmsId) ->
                                        assertThat(dbmsId).isNotEmpty().matches(DBMS_ID_PATTERN))
                                .hasString((nodeId) ->
                                        assertThat(nodeId).isNotEmpty().matches(NODE_ID_PATTERN))
                                .hasString((productVersion) -> {
                                    assertThat(productVersion)
                                            .satisfiesAnyOf(
                                                    (version) -> assertThat(version)
                                                            // make sure we get a CalVer in the format YYYY.MM.PP while
                                                            // disregarding any potential suffixes such as build numbers
                                                            // or release stage
                                                            .matches("^\\d{4,}\\.\\d{2}\\.\\d+.*"),
                                                    (version) -> assertThat(version)
                                                            .isNotEmpty()
                                                            // if running from within an IDE the version information is
                                                            // typically unset
                                                            .isEqualTo("dev"));
                                })
                                .hasString((edition) ->
                                        assertThat(edition).isNotEmpty().isEqualTo("community"))
                                .hasString((advertisementAddress) -> assertThat(advertisementAddress)
                                        .isNotEmpty()
                                        .isEqualTo("neo.example.org:7687")));
    }
}
