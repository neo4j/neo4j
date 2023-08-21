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
package org.neo4j.bolt.negotiation.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.assertions.ByteBufAssertions.assertThat;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationRequest;

class ProtocolNegotiationRequestDecoderTest {

    @Test
    void shouldDecodeRequest() {
        var buf = Unpooled.buffer()
                .writeInt(0x6060B017) // Magic Number
                .writeInt(0x00000003) // Protocol 3.0
                .writeInt(0x00000104) // Protocol 4.1
                .writeInt(0x00030404) // Protocol 4.4-4.2
                .writeInt(0x00000000); // Padding

        var channel = new EmbeddedChannel(new ProtocolNegotiationRequestDecoder());

        channel.writeInbound(buf);

        var request = channel.<ProtocolNegotiationRequest>readInbound();

        assertThat(request).isNotNull();
        assertThat(request.getMagicNumber()).isEqualTo(0x6060B017);
        assertThat(request.getProposedVersions())
                .hasSize(3)
                .containsExactly(new ProtocolVersion(3, 0), new ProtocolVersion(4, 1), new ProtocolVersion(4, 4, 3));

        assertThat(buf).hasNoRemainingReadableBytes().hasBeenReleased();
    }

    @Test
    void shouldDecodeFragmentedRequest() {
        var channel = new EmbeddedChannel(new ProtocolNegotiationRequestDecoder());

        channel.writeInbound(Unpooled.buffer().writeByte(0x60));
        assertThat(channel.<ProtocolNegotiationRequest>readInbound()).isNull();

        channel.writeInbound(Unpooled.buffer().writeShort(0x60B0));
        assertThat(channel.<ProtocolNegotiationRequest>readInbound()).isNull();

        channel.writeInbound(Unpooled.buffer().writeInt(0x17000000));
        assertThat(channel.<ProtocolNegotiationRequest>readInbound()).isNull();

        channel.writeInbound(Unpooled.buffer().writeShort(0x0300));
        assertThat(channel.<ProtocolNegotiationRequest>readInbound()).isNull();

        channel.writeInbound(Unpooled.buffer().writeLong(0x0001040003040400L));
        assertThat(channel.<ProtocolNegotiationRequest>readInbound()).isNull();

        channel.writeInbound(Unpooled.buffer().writeByte(0x00).writeShort(0x0000));
        var request = channel.<ProtocolNegotiationRequest>readInbound();

        assertThat(request).isNotNull();
        assertThat(request.getMagicNumber()).isEqualTo(0x6060B017);
        assertThat(request.getProposedVersions())
                .hasSize(3)
                .containsExactly(new ProtocolVersion(3, 0), new ProtocolVersion(4, 1), new ProtocolVersion(4, 4, 3));
    }
}
