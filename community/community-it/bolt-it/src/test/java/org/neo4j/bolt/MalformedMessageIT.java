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
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

/**
 * Evaluates whether Bolt correctly handles invalid messages.
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class MalformedMessageIT {

    @TransportTest
    void shouldHandleIncorrectFraming(BoltWire wire, TransportConnection connection) throws Exception {
        // Given I have a message that gets truncated in the chunking, so part of it is missing
        var msg = wire.run("UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared");
        var truncated = msg.readSlice(msg.readableBytes() - 12);

        // When
        connection.connect().sendDefaultProtocolVersion().send(truncated);

        // Then
        BoltConnectionAssertions.assertThat(connection)
                .negotiatesDefaultVersion()
                .isEventuallyTerminated();
    }

    @ProtocolTest
    void shouldHandleMessagesWithIncorrectFields(@VersionSelected TransportConnection connection) throws IOException {
        // Given I send a message with the wrong types in its fields
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, RunMessage.SIGNATURE))
                .writeString("RETURN 1")
                .writeMapHeader(0)
                .writeInt(42);

        // When
        connection.send(msg);

        // Then
        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Request.Invalid,
                        "Illegal value for field \"metadata\": Unexpected type: Expected MAP but got INT");
    }

    @ProtocolTest
    void shouldHandleUnknownMarkerBytes(@VersionSelected TransportConnection connection) throws IOException {
        // Given I send a message with an invalid type
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, RunMessage.SIGNATURE))
                .writeMarkerByte(0xC7)
                .writeMapHeader(0)
                .writeMapHeader(0);

        // When
        connection.send(msg);

        // Then
        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Request.Invalid,
                        "Illegal value for field \"statement\": Unexpected type: Expected STRING but got RESERVED");
    }

    @TransportTest
    void shouldCloseConnectionOnInvalidHandshake(TransportConnection connection) throws IOException {

        // GIVEN
        connection.sendRaw(new byte[] {
            (byte) 0xDE, (byte) 0xAD, (byte) 0xB0, (byte) 0x17, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        });

        // THEN
        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();
    }
}
