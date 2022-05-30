/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.transport;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;

import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.client.tls.SecureSocketConnection;
import org.neo4j.bolt.testing.client.websocket.SecureWebSocketConnection;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class RejectTransportEncryptionIT {
    @Inject
    private Neo4jWithSocket server;

    private TransportConnection connection;

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        server.setConfigure(settings -> {
            settings.put(BoltConnector.encryption_level, DISABLED);
        });
        server.init(testInfo);
    }

    public static Stream<Arguments> transportFactory() {
        return Stream.of(
                Arguments.of(
                        SecureWebSocketConnection.factory(),
                        new IOException("Failed to connect to the server within 5 minutes")),
                Arguments.of(
                        SecureSocketConnection.factory(), new IOException("Remote host terminated the handshake")));
    }

    @AfterEach
    public void teardown() throws Exception {
        if (connection != null) {
            connection.disconnect();
        }
    }

    @ParameterizedTest(name = "{displayName} {index}")
    @MethodSource("transportFactory")
    public void shouldRejectConnectionAfterHandshake(TransportConnection.Factory c, Exception expected)
            throws Exception {
        this.connection = c.create(server.lookupDefaultConnector());

        assertThatExceptionOfType(expected.getClass())
                .isThrownBy(() -> connection.connect().sendDefaultProtocolVersion())
                .withMessage(expected.getMessage());
    }
}
