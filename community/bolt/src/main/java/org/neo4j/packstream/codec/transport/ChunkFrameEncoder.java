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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Encodes messages of arbitrary length into chunks.
 * <p>
 * Note: Contrary to {@link ChunkFrameDecoder}, this implementation does <b>not</b> identify the end of a fully encoded message. Upstream handlers are expected
 * to flush a single empty buffer in order to mark the end of their transmission.
 */
public class ChunkFrameEncoder extends MessageToByteEncoder<ByteBuf> {

    /**
     * Identifies the maximum number of bytes to be encoded within a given chunk.
     * <p>
     * When a message surpasses this length limitation, it will be separated into multiple chunks.
     */
    private static final int MAX_CHUNK_LENGTH = (1 << 16) - 1;

    private final int limit;

    public ChunkFrameEncoder(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }

        this.limit = limit;
    }

    public ChunkFrameEncoder() {
        this(MAX_CHUNK_LENGTH);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) {
        // explicitly skip empty messages as we expect keep-alives to be transmitted explicitly through another encoder
        if (!msg.isReadable()) {
            return;
        }

        while (msg.isReadable()) {
            var chunkLength = Math.min(this.limit, msg.readableBytes());
            out.writeShort(chunkLength);

            if (chunkLength != 0) {
                out.writeBytes(msg, chunkLength);
            }
        }
    }
}
