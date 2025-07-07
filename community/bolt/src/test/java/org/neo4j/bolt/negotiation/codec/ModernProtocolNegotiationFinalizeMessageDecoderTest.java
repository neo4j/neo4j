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

import io.netty.handler.codec.DecoderException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.negotiation.message.ModernProtocolNegotiationFinalizeMessage;
import org.neo4j.bolt.negotiation.message.ProtocolCapability;
import org.neo4j.bolt.testing.annotation.StrictBufferExtension;
import org.neo4j.bolt.testing.channel.StrictBufferContext;

@StrictBufferExtension
class ModernProtocolNegotiationFinalizeMessageDecoderTest {

    @Test
    void shouldDecodeValidPayloads(StrictBufferContext ctx) {
        var channel = ctx.channel(new ModernProtocolNegotiationFinalizeMessageDecoder());

        var buf = ctx.buffer()
                .writeInt(new ProtocolVersion(5, 0).encode())
                .writeByte(0b10000001)
                .writeByte(0b00000000);

        channel.writeInbound(buf);

        var message = channel.<ModernProtocolNegotiationFinalizeMessage>readInbound();

        Assertions.assertThat(message).isNotNull().satisfies(m -> {
            Assertions.assertThat(m.selectedVersion()).isEqualTo(new ProtocolVersion(5, 0));

            Assertions.assertThat(m.capabilities()).contains(ProtocolCapability.FABRIC);
        });
    }

    @Test
    void shouldDecodeSplitPayloads(StrictBufferContext ctx) {
        var channel = ctx.channel(new ModernProtocolNegotiationFinalizeMessageDecoder());

        var buf = ctx.buffer()
                .writeInt(new ProtocolVersion(5, 0).encode())
                .writeByte(0b10000001)
                .writeByte(0b00000000);

        channel.writeInbound(buf.readRetainedSlice(2));

        Assertions.assertThat(channel.<ModernProtocolNegotiationFinalizeMessage>readInbound())
                .isNull();

        channel.writeInbound(buf.readRetainedSlice(2));

        Assertions.assertThat(channel.<ModernProtocolNegotiationFinalizeMessage>readInbound())
                .isNull();

        channel.writeInbound(buf);

        var message = channel.<ModernProtocolNegotiationFinalizeMessage>readInbound();

        Assertions.assertThat(message).isNotNull().satisfies(m -> {
            Assertions.assertThat(m.selectedVersion()).isEqualTo(new ProtocolVersion(5, 0));

            Assertions.assertThat(m.capabilities()).contains(ProtocolCapability.FABRIC);
        });
    }

    @Test
    void shouldIgnoreTruncatedPayloads(StrictBufferContext ctx) {
        var channel = ctx.channel(new ModernProtocolNegotiationFinalizeMessageDecoder());

        channel.writeInbound(ctx.buffer().writeByte(0));

        Assertions.assertThat(channel.<ModernProtocolNegotiationFinalizeMessage>readInbound())
                .isNull();

        channel.writeInbound(ctx.buffer().writeByte(0).writeByte(0).writeByte(0));

        Assertions.assertThat(channel.<ModernProtocolNegotiationFinalizeMessage>readInbound())
                .isNull();
    }

    @Test
    void shouldFailWhenRangeIsGiven(StrictBufferContext ctx) {
        var channel = ctx.channel(new ModernProtocolNegotiationFinalizeMessageDecoder());

        var buf = ctx.buffer().writeInt(new ProtocolVersion(5, 4, 3).encode()).writeByte(0b00000000);

        Assertions.assertThatExceptionOfType(DecoderException.class)
                .isThrownBy(() -> channel.writeInbound(buf))
                .withCauseInstanceOf(IllegalArgumentException.class)
                .withMessageContaining("Illegal version selection: Selection cannot include range");
    }
}
