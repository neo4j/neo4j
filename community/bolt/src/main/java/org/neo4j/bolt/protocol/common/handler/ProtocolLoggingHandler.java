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
package org.neo4j.bolt.protocol.common.handler;

import static io.netty.buffer.ByteBufUtil.prettyHexDump;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.net.SocketAddress;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.packstream.io.PackstreamBuf;

public class ProtocolLoggingHandler extends ChannelDuplexHandler {

    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(ProtocolLoggingHandler.class);

    public static final String RAW_NAME = "rawProtocolLoggingHandler";
    public static final String DECODED_NAME = "decodedProtocolLoggingHandler";

    private final InternalLog log;

    public ProtocolLoggingHandler(InternalLogProvider logging) {
        this(logging.getLog(ProtocolLoggingHandler.class));
    }

    public ProtocolLoggingHandler(InternalLog log) {
        this.log = log;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf buf) {
            this.logBlob(ctx.channel().remoteAddress(), true, buf);
        } else if (msg instanceof PackstreamBuf buf) {
            this.logBlob(ctx.channel().remoteAddress(), true, buf.getTarget());
        } else {
            this.log.info("[%s] >>> %s", ctx.channel().remoteAddress(), msg);
        }

        super.channelRead(ctx, msg);
    }

    private void logBlob(SocketAddress remoteAddress, boolean incoming, ByteBuf buf) {
        var direction = "<<<";
        if (incoming) {
            direction = ">>>";
        }

        this.log.info(
                "[%s] %s Blob (%d bytes):\n%s", remoteAddress, direction, buf.readableBytes(), prettyHexDump(buf));
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ByteBuf buf) {
            this.logBlob(ctx.channel().remoteAddress(), false, buf);
        } else if (msg instanceof PackstreamBuf buf) {
            this.logBlob(ctx.channel().remoteAddress(), false, buf.getTarget());
        } else {
            this.log.info("[%s] <<< %s", ctx.channel().remoteAddress(), msg);
        }

        super.write(ctx, msg, promise);
    }
}
