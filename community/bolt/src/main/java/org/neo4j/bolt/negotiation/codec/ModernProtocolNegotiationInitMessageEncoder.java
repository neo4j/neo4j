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
import io.netty.util.ReferenceCountUtil;
import org.neo4j.bolt.negotiation.message.ModernProtocolNegotiationInitMessage;
import org.neo4j.bolt.negotiation.message.ProtocolCapability;
import org.neo4j.bolt.negotiation.util.NegotiationEncodingUtil;
import org.neo4j.bolt.negotiation.version.ProtocolVersionReducer;
import org.neo4j.memory.HeapEstimator;

public final class ModernProtocolNegotiationInitMessageEncoder
        extends MessageToByteEncoder<ModernProtocolNegotiationInitMessage> {

    public static final long SHALLOW_SIZE =
            HeapEstimator.shallowSizeOfInstance(ModernProtocolNegotiationInitMessageEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, ModernProtocolNegotiationInitMessage msg, ByteBuf out)
            throws Exception {
        // try to group versions into consecutive blocks of the same major version so that they can be
        // transferred with as little data as possible
        var canonicalVersions = ProtocolVersionReducer.canonicalize(msg.supportedVersions());

        out.writeInt(msg.negotiationVersion().encode());

        NegotiationEncodingUtil.writeVarInt(out, canonicalVersions.size());
        canonicalVersions.forEach(version -> out.writeInt(version.encode()));

        var capabilityMask = ProtocolCapability.toBitMask(ctx.alloc(), msg.capabilities());
        try {
            NegotiationEncodingUtil.writeBitMask(out, capabilityMask);
        } finally {
            ReferenceCountUtil.release(capabilityMask);
        }
    }
}
