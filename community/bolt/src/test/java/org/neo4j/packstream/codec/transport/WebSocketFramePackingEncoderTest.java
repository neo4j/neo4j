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
package org.neo4j.packstream.codec.transport;

import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.testing.annotation.StrictBufferExtension;
import org.neo4j.bolt.testing.assertions.ByteBufAssertions;
import org.neo4j.bolt.testing.channel.StrictBufferContext;

@StrictBufferExtension
class WebSocketFramePackingEncoderTest {

    @Test
    void shouldPackRawPayloads(StrictBufferContext ctx) {
        var channel = ctx.channel(new WebSocketFramePackingEncoder());
        var buffer = ctx.outputBuffer() // handler internally retains buffer - mark as output
                .writeByte(0x01)
                .writeByte(0x02)
                .writeByte(0x03);

        channel.writeOutbound(buffer);
        channel.checkException();

        BinaryWebSocketFrame frame = ctx.output(channel.readOutbound());

        ByteBufAssertions.assertThat(frame.content()).isSameAs(buffer);
    }
}
