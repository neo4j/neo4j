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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WebSocketFramePackingEncoderTest {

    private EmbeddedChannel channel;

    @BeforeEach
    void prepareChannel() {
        this.channel = new EmbeddedChannel(new WebSocketFramePackingEncoder());
    }

    @Test
    void shouldPackRawPayloads() {
        var encoded = Unpooled.buffer().writeByte(0x01).writeByte(0x02).writeByte(0x03);

        this.channel.writeOutbound(encoded);
        this.channel.checkException();

        BinaryWebSocketFrame frame = this.channel.readOutbound();

        assertNotNull(frame);
        assertSame(encoded, frame.content());
        assertEquals(1, encoded.refCnt());

        encoded.release();
    }
}
