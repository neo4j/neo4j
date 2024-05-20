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
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.packstream.error.reader.LimitExceededException;
import org.neo4j.packstream.io.PackstreamBuf;

/**
 * Decodes message frames based on a 16-bit unsigned length prefix.
 * <p>
 * Messages may consist of multiple chunks (each prefixed with its respective length) followed by a single null chunk (consisting of an empty payload).
 * <p>
 * This implementation does not impose any lower bound on chunk sizes. As such, peers may choose to flush their buffers at any point they deem sufficient.
 */
public class ChunkFrameDecoder extends ByteToMessageDecoder {
    public static final String NAME = "chunkFrameDecoder";
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(ChunkFrameDecoder.class);

    private final long limit;
    private final InternalLog log;

    private ChunkFrameDecoder(long limit, InternalLog log) {
        this.limit = limit;
        this.log = log;
    }

    public ChunkFrameDecoder(long limit, InternalLogProvider logging) {
        this(limit, logging.getLog(ChunkFrameDecoder.class));
    }

    public ChunkFrameDecoder(InternalLogProvider logging) {
        this(-1, logging);
    }

    public ChunkFrameDecoder unlimited() {
        return new ChunkFrameDecoder(-1, this.log);
    }

    public ChunkFrameDecoder limit(long limit) {
        return new ChunkFrameDecoder(limit, this.log);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws LimitExceededException {
        // mark the initial position within the buffer to be able to return to this position if there
        // is insufficient data remaining within the buffer
        in.markReaderIndex();

        var totalLength = 0;
        var slices = new ArrayList<ByteBuf>();
        try {
            while (in.isReadable(2)) {
                var chunkLength = in.readUnsignedShort();

                if (chunkLength == 0) {
                    // when an empty chunk is passed without prior context, it is interpreted as a keep-alive
                    // message and is thus discarded as it has previously been handled within the pipeline
                    if (slices.isEmpty()) {
                        continue;
                    }

                    // otherwise, an empty chunk will mark the end of the message thus permitting further
                    // processing of the message downstream
                    var msg = ctx.alloc().compositeBuffer(slices.size()).addComponents(true, slices);

                    out.add(PackstreamBuf.wrap(msg));

                    totalLength = 0;
                    slices.clear();

                    // prepare the method state for following iterations (if any) by marking the next offset as
                    // the base message offset
                    in.markReaderIndex();
                    continue;
                }

                // ensure that messages do not exceed any configured limitations within the pipeline and exit early if
                // we detect a violation
                totalLength += chunkLength;
                if (this.limit != -1 && totalLength > this.limit) {
                    log.debug(
                            "Client %s has exceeded message size limit of %d bytes",
                            ctx.channel().remoteAddress(), this.limit);
                    throw new LimitExceededException(this.limit, totalLength);
                }

                // revert to the initial buffer position and await further data if the current chunk has yet
                // to be fully transmitted
                if (!in.isReadable(chunkLength)) {
                    break;
                }

                slices.add(in.readRetainedSlice(chunkLength));
            }
        } finally {
            slices.forEach(ByteBuf::release);
        }

        in.resetReaderIndex();
    }
}
