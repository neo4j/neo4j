/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.neo4j.bolt.protocol.common.error.AccumulatorResetException;
import org.neo4j.bolt.protocol.common.signal.MessageSignal;
import org.neo4j.packstream.signal.FrameSignal;

/**
 * Accumulates all data within a given record until an explicit signal discards or finalizes the entire data set.
 */
public class RecordResponseAccumulator extends ChannelOutboundHandlerAdapter {

    private CompositeByteBuf pendingChunk;
    private ChannelPromise pendingPromise;

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // if a reset signal is encountered, all previously written chunks are discarded and their backing buffers
        // released back to the pool
        if (msg == MessageSignal.RESET) {
            if (this.pendingPromise != null) {
                this.pendingPromise.setFailure(new AccumulatorResetException());
                this.pendingPromise = null;
            }

            if (this.pendingChunk != null) {
                this.pendingChunk.release();
                this.pendingChunk = null;
            }

            return;
        }
        if (msg == MessageSignal.END) {
            if (this.pendingChunk == null) {
                return;
            }

            ctx.write(this.pendingChunk, this.pendingPromise);
            // announce message end to handlers further down the pipeline
            ctx.writeAndFlush(FrameSignal.MESSAGE_END);

            this.pendingChunk = null;
            this.pendingPromise = null;
            return;
        }

        // we only care about ByteBuf (e.g. encoded) messages for this accumulator implementation, so we'll pass
        // any other messages on to the next handler within the chain
        if (!(msg instanceof ByteBuf buf)) {
            ctx.write(msg, promise);
            return;
        }

        if (this.pendingChunk == null) {
            this.pendingChunk = ctx.alloc().compositeBuffer().addComponent(true, buf);
            this.pendingPromise = promise;
        } else {
            this.pendingChunk.addComponent(true, buf);
            this.pendingPromise = attachPromise(this.pendingPromise, promise);
        }
    }

    private static ChannelPromise attachPromise(ChannelPromise original, ChannelPromise attachment) {
        if (original == null) {
            return attachment;
        }

        return original.addListener(f -> {
            if (f.isSuccess()) {
                attachment.setSuccess();
            } else {
                attachment.setFailure(f.cause());
            }
        });
    }
}
