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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationRequest;
import org.neo4j.memory.HeapEstimator;

/**
 * Decodes Bolt protocol handshakes into their respective Java representations.
 * <p>
 * Within the Bolt protocol, handshakes are encoded as a single 4-byte magic number followed by four version proposals in order of their respective priority as
 * decided by the client.
 * <p>
 * If less than four versions are proposed, the remaining version fields are filled with zero bytes respectively.
 */
public class ProtocolNegotiationRequestDecoder extends ByteToMessageDecoder {

    public static final long SHALLOW_SIZE =
            HeapEstimator.shallowSizeOfInstance(ProtocolNegotiationRequestDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // if there is insufficient data for processing the handshake, return immediately and wait for
        // netty to invoke this decoder function once again when there is more data available
        if (in.readableBytes() < ProtocolNegotiationRequest.ENCODED_SIZE) {
            return;
        }

        var magicNumber = in.readInt();
        var proposedVersions = IntStream.range(0, 4)
                .map(n -> in.readInt())
                .mapToObj(ProtocolVersion::new)
                .filter(version -> !ProtocolVersion.INVALID.equals(version))
                .collect(Collectors.toList());

        out.add(new ProtocolNegotiationRequest(magicNumber, proposedVersions));
    }
}
