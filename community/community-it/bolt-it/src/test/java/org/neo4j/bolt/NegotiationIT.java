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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.bolt.testing.client.TransportConnection.DEFAULT_PROTOCOL_VERSION;

import io.netty.buffer.Unpooled;
import java.io.IOException;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@ExtendWith(OtherThreadExtension.class)
public class NegotiationIT {

    @Inject
    private OtherThread otherThread;

    @ProtocolTest
    void shouldNegotiateProtocolVersion(BoltWire wire, TransportConnection connection) throws Exception {
        // When
        connection.send(wire.getProtocolVersion());

        // Then
        BoltConnectionAssertions.assertThat(connection).negotiates(wire.getProtocolVersion());
    }

    @TransportTest
    void shouldReturnNilOnNoApplicableVersion(TransportConnection connection) throws Exception {
        // When
        connection.connect().send(new ProtocolVersion(254, 0, 0));

        // Then
        BoltConnectionAssertions.assertThat(connection).failsToNegotiateVersion();
    }

    @TransportTest
    void shouldNegotiateOnRange(TransportConnection connection) throws Exception {
        var range = new ProtocolVersion(DEFAULT_PROTOCOL_VERSION.major(), DEFAULT_PROTOCOL_VERSION.minor() + 2, 2);

        connection.connect().send(range);

        BoltConnectionAssertions.assertThat(connection).negotiates(DEFAULT_PROTOCOL_VERSION);
    }

    @TransportTest
    void shouldNegotiateWhenPreferredIsUnavailable(BoltWire wire, TransportConnection connection) throws Exception {
        connection.send(new ProtocolVersion(ProtocolVersion.MAX_MAJOR_BIT, 0, 0), wire.getProtocolVersion());

        BoltConnectionAssertions.assertThat(connection).negotiates(wire.getProtocolVersion());
    }

    @TransportTest
    void shouldTimeoutWhenHandshakeIsTransmittedTooSlowly(TransportConnection connection) throws Exception {
        var handshakeBytes = Unpooled.buffer()
                .writeInt(0x6060B017)
                .writeInt(TransportConnection.DEFAULT_PROTOCOL_VERSION.encode())
                .writeInt(ProtocolVersion.INVALID.encode())
                .writeInt(ProtocolVersion.INVALID.encode())
                .writeInt(ProtocolVersion.INVALID.encode());

        otherThread.execute(() -> {
            while (handshakeBytes.isReadable()) {
                connection.sendRaw(handshakeBytes.readSlice(1));
                Thread.sleep(500);
            }

            return null;
        });

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();
    }

    @TransportTest
    void shouldTimeoutWhenTruncatedHandshakeIsTransmitted(TransportConnection connection) throws IOException {
        // Only transmit the magic number (leaving out any protocol versions)
        var buf = Unpooled.buffer().writeInt(0x6060B017);

        connection.connect().sendRaw(buf);

        assertThat(connection).isEventuallyTerminated();
    }
}
