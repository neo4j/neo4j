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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import java.util.concurrent.TimeUnit;
import org.neo4j.bolt.protocol.common.signal.StateSignal;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.packstream.signal.FrameSignal;

public class KeepAliveHandler extends IdleStateHandler {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(KeepAliveHandler.class);

    private final Log log;

    /**
     * Enables the handler's legacy behavior emulation.
     * <p>
     * When enabled, NOOPs will only be transmitted while the protocol is streaming results.
     */
    private final boolean legacyMode;

    /**
     * Identifies whether a job is currently being processed within the state machine.
     */
    private boolean processing;

    /**
     * Identifies whether a downstream component is currently streaming results.
     */
    private boolean streaming;

    public KeepAliveHandler(boolean legacyMode, long writerIdleTimeSeconds, InternalLogProvider logging) {
        super(0, writerIdleTimeSeconds, 0, TimeUnit.MILLISECONDS);

        this.legacyMode = legacyMode;
        this.log = logging.getLog(KeepAliveHandler.class);
    }

    @Override
    protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
        if (processing || (streaming && legacyMode)) {
            ctx.writeAndFlush(FrameSignal.NOOP);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof StateSignal signal) {
            switch (signal) {
                case BEGIN_JOB_PROCESSING -> {
                    this.processing = true;

                    if (!legacyMode) {
                        log.debug(
                                "[%s] Engaging client liveliness check",
                                ctx.channel().remoteAddress());
                    }
                }
                case END_JOB_PROCESSING -> {
                    this.processing = false;

                    if (!legacyMode) {
                        log.debug(
                                "[%s] Disengaging client liveliness check",
                                ctx.channel().remoteAddress());
                    }
                }

                    // Legacy mode
                case ENTER_STREAMING -> {
                    this.streaming = true;

                    if (legacyMode) {
                        log.debug(
                                "[%s] Engaging legacy client liveliness check",
                                ctx.channel().remoteAddress());
                    }
                }
                case EXIT_STREAMING -> {
                    this.streaming = false;

                    if (legacyMode) {
                        log.debug(
                                "[%s] Disengaging legacy client liveliness check",
                                ctx.channel().remoteAddress());
                    }
                }
                default -> {
                    return;
                }
            }
        }

        super.write(ctx, msg, promise);
    }
}
