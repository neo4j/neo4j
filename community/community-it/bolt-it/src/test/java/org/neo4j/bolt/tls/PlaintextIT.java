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

import java.io.IOException;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.transport.preset.SecureTransportOnly;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.test.provider.ConnectionProvider;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

/**
 * Evaluates whether Bolt treats TLS clients correctly when operating in plaintext mode (e.g. TLS is disabled or left
 * un-configured).
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class PlaintextIT {

    @SettingsFunction
    static void customizeSettings(Map<Setting<?>, Object> settings) {
        settings.put(BoltConnector.encryption_level, BoltConnector.EncryptionLevel.DISABLED);
    }

    @TransportTest
    @SecureTransportOnly
    void shouldTerminateConnectionDuringHandshake(
            TransportType transport, BoltWire wire, ConnectionProvider connectionProvider) throws Exception {
        // depending on the transport we get different error messages as the WebSocket client seems to hide the
        // actual TLS error
        var message = "Remote host terminated the handshake";
        if (transport == TransportType.WEBSOCKET_TLS) {
            message = "Failed to connect to the server within 5 minutes";
        }

        Assertions.assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> connectionProvider.create().send(wire.getProtocolVersion()))
                .withMessage(message);
    }
}
