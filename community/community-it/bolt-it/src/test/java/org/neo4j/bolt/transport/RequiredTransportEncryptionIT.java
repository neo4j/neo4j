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

import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.REQUIRED;

import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.client.websocket.WebSocketConnection;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class RequiredTransportEncryptionIT {
    private HostnamePort address;
    private TransportConnection connection;

    public static Stream<TransportConnection.Factory> factoryProvider() {
        return Stream.of(SocketConnection.factory(), WebSocketConnection.factory());
    }

    @Inject
    private Neo4jWithSocket server;

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        server.setConfigure(settings -> settings.put(BoltConnector.encryption_level, REQUIRED));
        server.init(testInfo);

        address = server.lookupDefaultConnector();
    }

    @AfterEach
    public void cleanup() throws IOException {
        if (connection != null) {
            connection.disconnect();
        }
    }

    @ParameterizedTest(name = "{displayName} {index}")
    @MethodSource("factoryProvider")
    public void shouldCloseUnencryptedConnectionOnHandshakeWhenEncryptionIsRequired(TransportConnection.Factory c)
            throws Exception {
        this.connection = c.create(address);

        // When
        connection.connect().sendDefaultProtocolVersion();

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();
    }
}
