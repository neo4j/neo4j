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
package org.neo4j.bolt.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.REQUIRED;

import java.io.IOException;
import org.assertj.core.api.Condition;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Connected;
import org.neo4j.bolt.test.annotation.connection.transport.ExcludeTransport;
import org.neo4j.bolt.test.annotation.connection.transport.IncludeTransport;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.test.connection.setup.SettingBuilder;
import org.neo4j.bolt.test.provider.ConnectionProvider;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.client.error.BoltTestClientClosedException;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

/**
 * Ensures that Bolt terminates plaintext connections when the TLS policy requires all connections to establish
 * encrypted connections.
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class RequiredTransportSecurityIT {

    @SettingsFunction
    static void customizeSettings(SettingBuilder settings) {
        settings.set(BoltConnector.encryption_level, REQUIRED);
    }

    @TransportTest
    @ExcludeTransport({TransportType.LOCAL, TransportType.UNIX, TransportType.WEBSOCKET_TLS, TransportType.TCP_TLS})
    void shouldCloseUnencryptedConnectionOnHandshakeWhenEncryptionIsRequired(
            BoltWire wire, @Connected ConnectionProvider connectionProvider) throws IOException {
        BoltTestConnection connection = connectionProvider.create();

        try {
            connection.connect();
            connection.send(wire.getProtocolVersion());
        } catch (RuntimeException e) {
            assertThat(e).isInstanceOf(BoltTestClientClosedException.class);
        }

        BoltConnectionAssertions.assertThat(connection)
                .isNot(protocolNegotiated())
                .isEventuallyTerminated();
    }

    @TransportTest
    @IncludeTransport({TransportType.LOCAL, TransportType.UNIX, TransportType.WEBSOCKET_TLS, TransportType.TCP_TLS})
    void shouldHandshakeOnSafeConnectionsWhenEncryptionIsRequired(
            BoltWire wire, @Connected BoltTestConnection connection) throws IOException {
        connection.send(wire.getProtocolVersion());

        BoltConnectionAssertions.assertThat(connection).is(protocolNegotiated());

        connection.disconnect();
    }

    private static Condition<BoltTestConnection> protocolNegotiated() {
        return new Condition<>(
                c -> {
                    try {
                        c.receiveNegotiatedVersion();
                        return true;
                    } catch (RuntimeException e) {
                        return false;
                    }
                },
                "shouldn't receive negotiated version");
    }
}
