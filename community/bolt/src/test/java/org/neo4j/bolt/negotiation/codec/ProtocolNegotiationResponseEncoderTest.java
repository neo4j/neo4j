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

import static org.neo4j.bolt.testing.assertions.ByteBufAssertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationResponse;

class ProtocolNegotiationResponseEncoderTest {

    @Test
    void shouldEncodeResponse() {
        var msg = new ProtocolNegotiationResponse(new ProtocolVersion(4, 3));

        var channel = new EmbeddedChannel(new ProtocolNegotiationResponseEncoder());

        channel.writeOutbound(msg);

        var buf = channel.<ByteBuf>readOutbound();

        assertThat(buf).isNotNull().hasReadableBytes(4).containsInt(0x00000304).hasNoRemainingReadableBytes();
    }

    @Test
    void shouldEncodeRejectionResponse() {
        var channel = new EmbeddedChannel(new ProtocolNegotiationResponseEncoder());

        channel.writeOutbound(new ProtocolNegotiationResponse(ProtocolVersion.INVALID));

        var buf = channel.<ByteBuf>readOutbound();

        assertThat(buf).isNotNull().hasReadableBytes(4).containsInt(0x00000000).hasNoRemainingReadableBytes();
    }
}
