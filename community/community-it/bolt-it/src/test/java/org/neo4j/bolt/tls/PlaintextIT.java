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

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Connected;
import org.neo4j.bolt.test.annotation.connection.transport.preset.SecureTransportOnly;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.test.connection.setup.SettingBuilder;
import org.neo4j.bolt.test.provider.ConnectionProvider;
import org.neo4j.bolt.testing.client.error.BoltTestClientIOException;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnector;
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
    static void customizeSettings(SettingBuilder settings) {
        settings.set(BoltConnector.encryption_level, BoltConnector.EncryptionLevel.DISABLED);
    }

    @TransportTest
    @SecureTransportOnly
    void shouldTerminateConnectionDuringHandshake(BoltWire wire, @Connected ConnectionProvider connectionProvider)
            throws Exception {
        assertThatExceptionOfType(BoltTestClientIOException.class)
                .isThrownBy(() -> connectionProvider.create().send(wire.getProtocolVersion()))
                .withStackTraceContaining("Connection closed");
    }
}
