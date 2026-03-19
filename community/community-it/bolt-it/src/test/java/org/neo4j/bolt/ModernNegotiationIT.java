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

import io.netty.buffer.Unpooled;
import java.util.EnumSet;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.negotiation.message.ProtocolCapability;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Connected;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
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
public class ModernNegotiationIT {

    @Inject
    private OtherThread otherThread;

    @ProtocolTest
    void shouldNegotiateProtocolVersion(BoltWire wire, @Connected BoltTestConnection connection) throws Exception {
        // this is a best case scenario in which all protocol versions are unsupported meaning that
        // the server has no other option than select the v2 handshake
        connection.send(
                new ProtocolVersion(2, 0),
                new ProtocolVersion(2, 1),
                ProtocolVersion.NEGOTIATION_V2,
                new ProtocolVersion(2, 2));

        BoltConnectionAssertions.assertThat(connection).receivesProtocolProposal(proposal -> {
            Assertions.assertThat(proposal).isNotNull();

            Assertions.assertThat(proposal.negotiationVersion()).isEqualTo(ProtocolVersion.NEGOTIATION_V2);

            Assertions.assertThat(BoltProtocol.available())
                    .allSatisfy(protocol -> Assertions.assertThat(proposal.versions())
                            .anyMatch(version -> version.matches(protocol.version())));

            // TODO: capabilities is currently empty as we are not explicitly testing against the fabric
            //       connector which is the only connector to feature a capability
            Assertions.assertThat(proposal.capabilities()).containsAll(EnumSet.noneOf(ProtocolCapability.class));
        });

        connection.send(wire.getProtocolVersion(), EnumSet.noneOf(ProtocolCapability.class));

        // the server never confirms the selected protocol version as the client can easily make
        // decisions over compatibility on its own here - we'll need to check whether the protocol stage
        // has moved on instead
        connection.send(wire.hello());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(meta -> Assertions.assertThat(meta)
                .containsEntry("protocol_version", wire.getProtocolVersion().toString()));
    }

    @ProtocolTest
    void shouldNegotiateProtocolVersionWhenHandshakeRangeIsGiven(
            BoltWire wire, @Connected BoltTestConnection connection) throws Exception {
        connection.send(
                new ProtocolVersion(2, 0),
                new ProtocolVersion(2, 1),
                new ProtocolVersion(ProtocolVersion.NEGOTIATION_V2.major(), 4, 3));

        BoltConnectionAssertions.assertThat(connection).receivesProtocolProposal();

        connection.send(wire.getProtocolVersion(), EnumSet.noneOf(ProtocolCapability.class));

        connection.send(wire.hello());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(meta -> Assertions.assertThat(meta)
                .containsEntry("protocol_version", wire.getProtocolVersion().toString()));
    }

    @ProtocolTest
    void shouldUseModernNegotiationBasedOnPriority(BoltWire wire, @Connected BoltTestConnection connection)
            throws Exception {
        connection.send(ProtocolVersion.NEGOTIATION_V2, wire.getProtocolVersion());

        BoltConnectionAssertions.assertThat(connection).receivesProtocolProposal();

        connection.send(wire.getProtocolVersion(), EnumSet.noneOf(ProtocolCapability.class));

        connection.send(wire.hello());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(meta -> Assertions.assertThat(meta)
                .containsEntry("protocol_version", wire.getProtocolVersion().toString()));
    }

    @TransportTest
    void shouldFailIfRangeIsGiven(@Connected BoltTestConnection connection) throws Exception {
        connection.send(ProtocolVersion.NEGOTIATION_V2);

        BoltConnectionAssertions.assertThat(connection).receivesProtocolProposal();

        connection.send(new ProtocolVersion(5, 4, 4), EnumSet.noneOf(ProtocolCapability.class));

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();
    }

    @TransportTest
    void shouldFailIfUnknownVersionIsGiven(@Connected BoltTestConnection connection) throws Exception {
        connection.send(ProtocolVersion.NEGOTIATION_V2);

        BoltConnectionAssertions.assertThat(connection).receivesProtocolProposal();

        connection.send(new ProtocolVersion(2, 1), EnumSet.noneOf(ProtocolCapability.class));

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();
    }

    @TransportTest
    void shouldFailIfPartialVersionIsGiven(@Connected BoltTestConnection connection) {
        connection.send(ProtocolVersion.NEGOTIATION_V2);

        BoltConnectionAssertions.assertThat(connection).receivesProtocolProposal();

        connection.sendRaw(new byte[] {0x00, 0x42});

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();
    }

    @TransportTest
    void shouldTimeoutWhenHandshakeIsTransmittedTooSlowly(@Connected BoltTestConnection connection) {
        connection.send(ProtocolVersion.NEGOTIATION_V2);

        BoltConnectionAssertions.assertThat(connection).receivesProtocolProposal();

        var handshakeBytes = Unpooled.buffer().writeInt(BoltTestConnection.DEFAULT_PROTOCOL_VERSION.encode());

        otherThread.execute(() -> {
            while (handshakeBytes.isReadable()) {
                connection.sendRaw(handshakeBytes.readSlice(1));
                Thread.sleep(2000);
            }

            return null;
        });

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();
    }
}
