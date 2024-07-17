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
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.EventExecutorGroup;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;

public class HouseKeeperHandler extends ChannelInboundHandlerAdapter {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(HouseKeeperHandler.class);
    public static final String HANDLER_NAME = "housekeeper";

    private final InternalLog log;

    private Connection connection;
    private Connector connector;

    private boolean failed;

    public HouseKeeperHandler(InternalLogProvider logging) {
        this.log = logging.getLog(HouseKeeperHandler.class);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.connection = Connection.getConnection(ctx.channel());
        this.connector = this.connection.connector();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (failed || isShuttingDown(ctx)) {
            return;
        }
        failed = true; // log only the first exception to not pollute the log

        try {
            // Netty throws a NativeIoException on connection reset - directly importing that class
            // caused a host of linking errors, because it depends on JNI to work. Hence, we just
            // test on the message we know we'll get.
            var networkError = Exceptions.contains(cause, e -> {
                var simpleName = e.getClass().getSimpleName();
                if (simpleName.contains("StacklessClosedChannel") || simpleName.contains("NativeIoException")) {
                    return true;
                }

                return e.getMessage() != null && e.getMessage().contains("Connection reset by peer");
            });

            if (networkError) {
                this.connector.errorAccountant().notifyNetworkAbort(this.connection, cause);
            } else {
                log.error("Fatal error occurred when handling a client connection: " + ctx.channel(), cause);
            }
        } finally {
            // Note: Typically we would invoke Connection#close here, however this error may be
            // triggered while streaming thus preventing us from releasing the worker thread for
            // graceful closure. Closing the network channel will release the worker thread and
            // cause the connection to be closed correctly.
            ctx.close();
        }
    }

    private static boolean isShuttingDown(ChannelHandlerContext ctx) {
        EventExecutorGroup eventLoopGroup = ctx.executor().parent();
        return eventLoopGroup != null && eventLoopGroup.isShuttingDown();
    }
}
