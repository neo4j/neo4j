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
package org.neo4j.bolt;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.ExcludeWire;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@ExtendWith(OtherThreadExtension.class)
public class ResetMessageIT {
    @SettingsFunction
    static void customizeSettings(Map<Setting<?>, Object> settings) {
        settings.put(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout, Duration.ofSeconds(5));
        settings.put(
                BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes,
                ByteUnit.kibiBytes(1));
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 4), @Version(major = 5, minor = 0)})
    void shouldFailAResetWhenInUnauthenticatedState(BoltWire wire, @VersionSelected TransportConnection connection)
            throws IOException {
        connection.send(wire.reset());
        BoltConnectionAssertions.assertThat(connection).receivesFailure();
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 4), @Version(major = 5, minor = 0)})
    void shouldFailAResetWhenInAuthenticationState(BoltWire wire, @VersionSelected TransportConnection connection)
            throws IOException {
        connection.send(wire.hello()); // This will take us to authentication state.
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(wire.reset());
        BoltConnectionAssertions.assertThat(connection).receivesFailure();
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 4), @Version(major = 5, minor = 0)})
    void shouldResetToReadyStateWhenAuthenticated(BoltWire wire, @VersionSelected TransportConnection connection)
            throws IOException {
        connection.send(wire.hello()); // This will take us to authentication state.
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
        connection.send(wire.logon());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(wire.begin());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
        connection.send(wire.reset());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(
                wire.begin()); // This should pass as after reset past Authentication state we should end back in
        // ReadyState
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 4), @Version(major = 5, minor = 0)})
    void shouldResetToReadyStateWhenAuthenticatedLegacy(BoltWire wire, @VersionSelected TransportConnection connection)
            throws IOException {
        connection.send(wire.hello()); // This will take us to Ready state.
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(wire.begin());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
        connection.send(wire.reset());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(wire.begin()); // This should pass as after reset we should end back in ReadyState
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 4), @Version(major = 5, minor = 0)})
    void shouldFailAResetWhenInUnauthenticatedStateLegacy(
            BoltWire wire, @VersionSelected TransportConnection connection) throws IOException {
        connection.send(wire.reset());
        BoltConnectionAssertions.assertThat(connection).receivesFailure();
    }
}
