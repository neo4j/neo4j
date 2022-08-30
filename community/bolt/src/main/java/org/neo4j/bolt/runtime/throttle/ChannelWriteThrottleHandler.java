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
package org.neo4j.bolt.runtime.throttle;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.neo4j.bolt.transport.TransportThrottleException;

/**
 * This handler monitors outgoing writes and the writability of the underlying channel and terminates connections
 * that do not consume results within the configured timeout.
 */
public class ChannelWriteThrottleHandler extends ChannelDuplexHandler {

    private final List<ChannelPromise> pendingWriteOperations = new ArrayList<>();
    private Future<?> reaperFuture;
    private final long maxWriteLockMillis;

    public ChannelWriteThrottleHandler(long maxWriteLockMillis) {
        this.maxWriteLockMillis = maxWriteLockMillis;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        super.write(ctx, msg, promise);
        pendingWriteOperations.add(promise);
        promise.addListener(future -> pendingWriteOperations.remove(promise));
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
                                    var ex = new TransportThrottleException(maxWriteLockMillis);
                                    // we create a copy because o.w. we run into a ConcurrentModificationException from
                                    // the removal listener
                                    var copyList = new ArrayList<>(pendingWriteOperations);
                                    copyList.forEach(channelPromise -> channelPromise.setFailure(ex));
                                    ctx.fireExceptionCaught(ex);
                                },
                                maxWriteLockMillis,
                                TimeUnit.MILLISECONDS);
            }
        }
        ctx.fireChannelWritabilityChanged();
    }
}
