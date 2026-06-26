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

import java.io.IOException;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Negotiated;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.connection.transport.IncludeTransport;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.ExcludeWire;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.test.connection.setup.SettingBuilder;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@IncludeTransport(TransportType.UNIX)
@DisabledOnOs(OS.WINDOWS)
public class UnixDomainSocketAuthenticationIT {

    @SettingsFunction
    void customizeSettings(SettingBuilder settings) {
        settings.set(GraphDatabaseSettings.auth_enabled, true).set(BoltConnector.enable_unix_socket_auth, false);
    }

    /**
     * Evaluates whether the UNIX domain socket ignores the global authentication configuration when
     * configured to do so.
     */
    @ProtocolTest
    @ExcludeWire({@Version(major = 4), @Version(major = 5, minor = 0)})
    void shouldPermitUnauthenticatedAccess(BoltWire wire, @Negotiated BoltTestConnection connection)
            throws IOException {
        connection.send(wire.logon());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }

    /**
     * Evaluates whether the UNIX domain socket ignores the global authentication configuration on
     * legacy protocol versions when configured to do so.
     */
    @ProtocolTest
    @IncludeWire({@Version(major = 4), @Version(major = 5, minor = 0)})
    void shouldPermitUnauthenticatedAccessOnLegacyVersions(
            BoltWire wire, @VersionSelected BoltTestConnection connection) throws IOException {
        connection.send(wire.hello());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }
}
