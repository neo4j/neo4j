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
package org.neo4j.bolt.runtime.throttle;

import static org.neo4j.util.Preconditions.checkArgument;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.neo4j.bolt.protocol.common.signal.StateSignal;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public class ChannelReadThrottleHandler extends ChannelDuplexHandler {
    private final int lowWatermark;
    private final int highWatermark;
    private final InternalLog log;

    private boolean throttled;
    private int size;

    public ChannelReadThrottleHandler(int lowWatermark, int highWatermark, InternalLogProvider logging) {
        checkArgument(lowWatermark > 0, "lowWatermark must be positive");
        checkArgument(lowWatermark <= highWatermark, "lowWatermark must be less than or equal to highWatermark");

        this.lowWatermark = lowWatermark;
        this.highWatermark = highWatermark;
        this.log = logging.getLog(ChannelReadThrottleHandler.class);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (++this.size >= this.highWatermark) {
            log.warn(
                    "[%s] Inbound message queue has exceeded high watermark - Disabling message processing",
                    ctx.channel().remoteAddress());
            ctx.channel().config().setAutoRead(false);

            this.throttled = true;
        }

        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg == StateSignal.END_JOB_PROCESSING) {
            if (--this.size <= this.lowWatermark && this.throttled) {
                log.info(
                        "[%s] Inbound message queue has reached low watermark - Enabling message processing",
                        ctx.channel().remoteAddress());
                ctx.channel().config().setAutoRead(true);

                this.throttled = false;
            }
        }

        ctx.write(msg, promise);
    }
}
