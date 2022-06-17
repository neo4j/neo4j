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

import static io.netty.buffer.ByteBufUtil.prettyHexDump;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;

@Sharable // currently not shared between channels - required to permit changing position
public class ProtocolLoggingHandler extends ChannelDuplexHandler {

    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(ProtocolLoggingHandler.class);

    private final InternalLog log;

    public ProtocolLoggingHandler(InternalLogProvider logging) {
        this(logging.getLog(ProtocolLoggingHandler.class));
    }

    public ProtocolLoggingHandler(InternalLog log) {
        this.log = log;
    }

    /**
     * Shifts the handler to the end of the pipeline if it is part of the pipeline.
     * <p>
     * This process is necessary as we procedurally build up the pipeline without a guarantee of the
     * handler being present within the pipeline thus requiring us to add handlers to the end of the
     * pipeline regardless of whether logging is enabled or not.
     *
     * @param ctx a handler context.
     */
    public static void shiftToEndIfPresent(ChannelHandlerContext ctx) {
        var handler = ctx.pipeline().get(ProtocolLoggingHandler.class);

        if (handler == null) {
            return;
        }

        ctx.pipeline().remove(handler);

        ctx.pipeline().addLast(handler);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf buf) {
            this.log.info(
                    "[%s] >>> Blob (%d bytes):\n%s",
                    ctx.channel().remoteAddress(), buf.readableBytes(), prettyHexDump(buf));
        } else {
            this.log.info("[%s] >>> %s", ctx.channel().remoteAddress(), msg);
        }

        super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ByteBuf buf) {
            this.log.info(
                    "[%s] <<< Blob (%d bytes):\n%s",
                    ctx.channel().remoteAddress(), buf.readableBytes(), prettyHexDump(buf));
        } else {
            this.log.info("[%s] <<< %s", ctx.channel().remoteAddress(), msg);
        }

        super.write(ctx, msg, promise);
    }
}
