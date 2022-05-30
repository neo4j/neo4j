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
package org.neo4j.bolt.protocol.common.handler.messages;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.neo4j.bolt.protocol.common.connection.BoltConnection;
import org.neo4j.bolt.protocol.v40.messaging.request.GoodbyeMessage;
import org.neo4j.logging.Log;

public class GoodbyeMessageHandler extends SimpleChannelInboundHandler<GoodbyeMessage> {
    private final BoltConnection connection;
    private final Log log;

    public GoodbyeMessageHandler(BoltConnection connection, Log log) {
        this.connection = connection;
        this.log = log;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, GoodbyeMessage msg) throws Exception {
        this.log.debug(
                "Stopping connection %s due to client request", ctx.channel().remoteAddress());
        this.connection.stop();
    }
}
