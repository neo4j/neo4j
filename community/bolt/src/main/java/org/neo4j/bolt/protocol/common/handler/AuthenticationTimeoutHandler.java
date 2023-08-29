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

import static java.lang.String.format;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.error.ClientTimeoutException;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.memory.HeapEstimator;

/**
 * Close the channel directly if we failed to finish authentication within certain timeout specified by {@link
 * BoltConnectorInternalSettings#unsupported_bolt_unauth_connection_timeout}
 */
public class AuthenticationTimeoutHandler extends ChannelInboundHandlerAdapter {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(AuthenticationTimeoutHandler.class);

    private final Duration timeout;
    private Future<?> timeoutFuture;

    private Connection connection;
    private volatile boolean requestReceived;

    public AuthenticationTimeoutHandler(Duration timeout) {
        this.timeout = timeout;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.connection = Connection.getConnection(ctx.channel());

        this.timeoutFuture = ctx.executor()
                .schedule(
                        () -> {
                            try {
                                authTimerEnded(ctx);
                            } catch (Exception e) {
                                ctx.fireExceptionCaught(e);
                            }
                        },
                        timeout.toMillis(),
                        TimeUnit.MILLISECONDS);

        super.handlerAdded(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (this.timeoutFuture != null) {
            this.timeoutFuture.cancel(false);
        }

        this.connection.memoryTracker().releaseHeap(AuthenticationTimeoutHandler.SHALLOW_SIZE);
    }

    protected void authTimerEnded(ChannelHandlerContext ctx) throws Exception {
        ctx.close();

        if (requestReceived) {
            throw new BoltConnectionFatality(
                    format(
                            "Terminated connection '%s' (%s) as the server failed to handle an authentication request within %d ms.",
                            this.connection.id(), ctx.channel(), timeout.toMillis()),
                    null);
        }

        // throw ClientTimeoutException instead of BoltConnectionFatality as we do not consider
        // client induced errors to be as severe as server caused timeouts
        throw new ClientTimeoutException(format(
                "Terminated connection '%s' (%s) as the client failed to authenticate within %d ms.",
                this.connection.id(), ctx.channel(), timeout.toMillis()));
    }

    public void setRequestReceived(boolean requestReceived) {
        this.requestReceived = requestReceived;
    }
}
