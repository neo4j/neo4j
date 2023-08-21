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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationResponse;
import org.neo4j.memory.HeapEstimator;

/**
 * Encodes a Bolt server handshake.
 * <p>
 * Within the Bolt protocol, handshakes will result in a 4-byte message consisting of the chosen protocol revision or, if none of the proposed versions is
 * supported by the server, a null message consisting of four zero bytes.
 * <p>
 * Typically, the connection will advance into the initial (protocol version specific) state following the encoding of a server handshake message or be
 * terminated due to negotiation failure.
 */
public class ProtocolNegotiationResponseEncoder extends MessageToByteEncoder<ProtocolNegotiationResponse> {

    public static final long SHALLOW_SIZE =
            HeapEstimator.shallowSizeOfInstance(ProtocolNegotiationResponseEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, ProtocolNegotiationResponse msg, ByteBuf out) {
        out.writeInt(msg.getNegotiatedVersion().encode());
    }
}
