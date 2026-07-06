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
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ReferenceCountUtil;
import java.util.List;
import java.util.Set;
import org.neo4j.bolt.negotiation.message.ModernProtocolNegotiationFinalizeMessage;
import org.neo4j.bolt.negotiation.message.ProtocolCapability;
import org.neo4j.bolt.negotiation.util.NegotiationEncodingUtil;
import org.neo4j.bolt.negotiation.version.ProtocolVersion;
import org.neo4j.memory.HeapEstimator;

public final class ModernProtocolNegotiationFinalizeMessageDecoder extends ByteToMessageDecoder {

    public static final long SHALLOW_SIZE =
            HeapEstimator.shallowSizeOfInstance(ModernProtocolNegotiationFinalizeMessageDecoder.class);
    public static final int CAPABILITY_MASK_LIMIT = 32;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (!in.isReadable(ProtocolVersion.ENCODED_SIZE + 1)) {
            return;
        }
        in.markReaderIndex();

        var selectedVersion = new ProtocolVersion(in.readInt());
        if (selectedVersion.hasRange()) {
            throw new IllegalArgumentException("Illegal version selection: Selection cannot include range");
        }

        if (!NegotiationEncodingUtil.isBitMaskReadable(in, CAPABILITY_MASK_LIMIT)) {
            in.resetReaderIndex();
            return;
        }

        try {
            var capabilityMask = NegotiationEncodingUtil.readBitMask(in, CAPABILITY_MASK_LIMIT);
            Set<ProtocolCapability> capabilities;
            try {
                capabilities = ProtocolCapability.fromBitMask(capabilityMask);
            } finally {
                ReferenceCountUtil.release(capabilityMask);
            }

            out.add(new ModernProtocolNegotiationFinalizeMessage(selectedVersion, capabilities));
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Illegal capability mask", ex);
        }
    }
}
