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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.neo4j.bolt.transport.TransportThrottleException;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

/**
 * This handler monitors outgoing writes and the writability of the underlying channel and terminates connections
 * that do not consume results within the configured timeout.
 */
public class ChannelWriteThrottleHandler extends ChannelDuplexHandler {

    private final long maxWriteLockMillis;
    private final InternalLog log;

    private Future<?> reaperFuture;

    public ChannelWriteThrottleHandler(long maxWriteLockMillis, InternalLogProvider logging) {
        this.maxWriteLockMillis = maxWriteLockMillis;
        this.log = logging.getLog(ChannelWriteThrottleHandler.class);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        var reaperFuture = this.reaperFuture;
        if (reaperFuture != null) {
            reaperFuture.cancel(false);
            this.reaperFuture = null;
        }

        super.channelInactive(ctx);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isWritable()) {
            if (reaperFuture != null) {
                reaperFuture.cancel(false);
                reaperFuture = null;
            }
        } else {
            if (reaperFuture == null) {
                reaperFuture = ctx.executor()
                        .schedule(
                                () -> {
                                    var ex = new TransportThrottleException(this.maxWriteLockMillis);
                                    log.error("Fatal error occurred when handling a client connection", ex);

                                    // The client failed to consume a sufficient amount of the incoming network buffer
                                    // within the mandated time period. Close the channel immediately as we no longer
                                    // have the capacity to notify the client about this issue (as its incoming buffer
                                    // as well as out outgoing buffer are currently full).
                                    //
                                    // Note: Typically we would invoke Connection#close here, however this error may be
                                    // triggered while streaming thus preventing us from releasing the worker thread for
                                    // graceful closure. Closing the network channel will release the worker thread and
                                    // cause the connection to be closed correctly.
                                    ctx.close();
                                },
                                maxWriteLockMillis,
                                TimeUnit.MILLISECONDS);
            }
        }

        ctx.fireChannelWritabilityChanged();
    }
}
