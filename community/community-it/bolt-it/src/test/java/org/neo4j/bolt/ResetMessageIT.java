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

import java.time.Duration;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.test.connection.setup.SettingBuilder;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.io.ByteUnit;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@ExtendWith(OtherThreadExtension.class)
public class ResetMessageIT {
    @SettingsFunction
    static void customizeSettings(SettingBuilder settings) {
        settings.set(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout, Duration.ofSeconds(5))
                .set(
                        BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes,
                        ByteUnit.kibiBytes(1));
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 1))
    void shouldFailAResetWhenInUnauthenticatedState(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.reset());
        BoltConnectionAssertions.assertThat(connection).receivesFailure();
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 1))
    void shouldFailAResetWhenInAuthenticationState(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello()); // This will take us to authentication state.
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(wire.reset());
        BoltConnectionAssertions.assertThat(connection).receivesFailure();
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 1))
    void shouldResetToReadyStateWhenAuthenticated(BoltWire wire, @VersionSelected BoltTestConnection connection) {
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
    @IncludeWire(until = @Version(major = 5, minor = 0))
    void shouldResetToReadyStateWhenAuthenticatedLegacy(BoltWire wire, @VersionSelected BoltTestConnection connection) {
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
    @IncludeWire(until = @Version(major = 5, minor = 0))
    void shouldFailAResetWhenInUnauthenticatedStateLegacy(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.reset());
        BoltConnectionAssertions.assertThat(connection).receivesFailure();
    }
}
