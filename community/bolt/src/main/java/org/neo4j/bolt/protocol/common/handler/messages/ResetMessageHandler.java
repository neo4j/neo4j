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
package org.neo4j.bolt.protocol.common.handler.messages;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.message.request.connection.ResetMessage;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.memory.HeapEstimator;

public class ResetMessageHandler extends SimpleChannelInboundHandler<ResetMessage> {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(ResetMessageHandler.class);
    private final Log log;

    private Connection connection;

    public ResetMessageHandler(InternalLogProvider logging) {
        this.log = logging.getLog(ResetMessageHandler.class);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.connection = Connection.getConnection(ctx.channel());
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        connection.memoryTracker().releaseHeap(ResetMessageHandler.SHALLOW_SIZE);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ResetMessage msg) throws Exception {
        log.debug("Interrupted state machine");
        this.connection.interrupt();
    }
}
