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
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.MessageToByteEncoder;
import java.util.function.Predicate;
import org.neo4j.packstream.signal.FrameSignal;

/**
 * Encodes various signals which are transmitted as part of the chunk header.
 */
public class FrameSignalEncoder extends MessageToByteEncoder<FrameSignal> {
    private final Predicate<FrameSignal> filterPredicate;

    private boolean dirty;

    public FrameSignalEncoder(Predicate<FrameSignal> filterPredicate) {
        this.filterPredicate = filterPredicate;
    }

    public FrameSignalEncoder() {
        this(null);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, FrameSignal msg, ByteBuf out) throws Exception {
        if (this.filterPredicate != null && this.filterPredicate.test(msg)) {
            return;
        }

        // NOOP chunks are not permitted while messages are in the process of being written
        if (this.dirty && msg.requiresCleanState()) {
            return;
        }

        if (msg == FrameSignal.MESSAGE_END) {
            this.dirty = false;
        }

        out.writeShort(msg.getTag());
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // connection is assumed dirty when one of the prior handlers writes any sort of data to the wire as this
        // handler is typically followed directly by a
        // chunk encoder
        if (msg instanceof ByteBuf) {
            this.dirty = true;
        }

        super.write(ctx, msg, promise);
    }
}
