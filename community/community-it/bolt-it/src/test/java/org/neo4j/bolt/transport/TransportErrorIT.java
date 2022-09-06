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

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.protocol.v40.messaging.request.RunMessage;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class TransportErrorIT extends AbstractBoltTransportsTest {
    @Inject
    private Neo4jWithSocket server;

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        server.setConfigure(getSettingsFunction());
        server.init(testInfo);

        address = server.lookupDefaultConnector();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldHandleIncorrectFraming(TransportConnection.Factory connectionFactory) throws Exception {
        this.initParameters(connectionFactory);

        // Given I have a message that gets truncated in the chunking, so part of it is missing
        var msg = wire.run("UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared");
        var truncated = msg.readSlice(msg.readableBytes() - 12);

        // When
        connection.connect().sendDefaultProtocolVersion().send(truncated);

        // Then
        assertThat(connection).negotiatesDefaultVersion().isEventuallyTerminated();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldHandleMessagesWithIncorrectFields(TransportConnection.Factory connectionFactory)
            throws Exception {
        this.initConnection(connectionFactory);

        // Given I send a message with the wrong types in its fields
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, RunMessage.SIGNATURE))
                .writeString("RETURN 1")
                .writeMapHeader(0)
                .writeInt(42);

        // When
        connection.send(msg);

        // Then
        assertThat(connection)
                .receivesFailure(
                        Status.Request.Invalid,
                        "Illegal value for field \"metadata\": Unexpected type: Expected MAP but got INT");
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldHandleUnknownMarkerBytes(TransportConnection.Factory connectionFactory) throws Exception {
        this.initConnection(connectionFactory);

        // Given I send a message with an invalid type
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, RunMessage.SIGNATURE))
                .writeMarkerByte(0xC7)
                .writeMapHeader(0)
                .writeMapHeader(0);

        // When
        connection.send(msg);

        // Then
        assertThat(connection)
                .receivesFailure(
                        Status.Request.Invalid,
                        "Illegal value for field \"statement\": Unexpected type: Expected STRING but got RESERVED");
    }

    @ParameterizedTest(name = "{displayName} {index}")
    @MethodSource("argumentsProvider")
    public void shouldCloseConnectionOnInvalidHandshake(TransportConnection.Factory connectionFactory)
            throws Exception {
        this.initParameters(connectionFactory);

        // GIVEN
        this.connection.connect().sendRaw(new byte[] {
            (byte) 0xDE, (byte) 0xAD, (byte) 0xB0, (byte) 0x17, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        });

        // THEN
        assertThat(connection).isEventuallyTerminated();
    }
}
