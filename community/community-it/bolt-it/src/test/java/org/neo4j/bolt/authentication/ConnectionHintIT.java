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
package org.neo4j.bolt.authentication;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.setup.preset.EnableRouting;
import org.neo4j.bolt.test.annotation.setup.preset.EnableTelemetry;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@EnableTelemetry
@EnableRouting
class ConnectionHintIT {

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 4))
    void shouldIncludeTelemetryHintOnCompatibleVersions(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta)
                        .extractingByKey("hints")
                        .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                        .containsEntry("telemetry.enabled", true));
    }

    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 3))
    void shouldExcludeTelemetryHintOnLegacyVersions(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta)
                        .extractingByKey("hints")
                        .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                        .doesNotContainKey("telemetry.enabled"));
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 8))
    void shouldIncludeSSRHintOnCompatibleVersions(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta)
                        .extractingByKey("hints")
                        .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                        .containsEntry("ssr.enabled", true));
    }

    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 7))
    void shouldExcludeSSRHintOnLegacyVersions(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta)
                        .extractingByKey("hints")
                        .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                        .doesNotContainKey("ssr.enabled"));
    }
}
