/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.messaging.EventHandler;
import org.neo4j.causalclustering.messaging.EventHandlerProvider;
import org.neo4j.causalclustering.messaging.EventId;

import static org.neo4j.causalclustering.catchup.CatchupServerProtocol.State;
import static org.neo4j.causalclustering.messaging.EventHandler.Param.param;

public class CoreSnapshotRequestHandler extends SimpleChannelInboundHandler<CoreSnapshotRequest>
{
    private final CatchupServerProtocol protocol;
    private final CoreSnapshotService snapshotService;
    private final EventHandlerProvider eventHandlerProvider;

    public CoreSnapshotRequestHandler( CatchupServerProtocol protocol, CoreSnapshotService snapshotService, EventHandlerProvider eventHandlerProvider )
    {
        this.protocol = protocol;
        this.snapshotService = snapshotService;
        this.eventHandlerProvider = eventHandlerProvider;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, CoreSnapshotRequest msg ) throws Exception
    {
        EventHandler eventHandler = eventHandlerProvider.eventHandler( EventId.from( msg.messageId() ) );
        sendStates( ctx, snapshotService.snapshot(), eventHandler );
        protocol.expect( State.MESSAGE_TYPE );
    }

    private void sendStates( ChannelHandlerContext ctx, CoreSnapshot coreSnapshot, EventHandler eventHandler )
    {
        ctx.writeAndFlush( ResponseMessageType.CORE_SNAPSHOT );
        ctx.writeAndFlush( coreSnapshot );
        eventHandler.on( EventHandler.EventState.Info, "Sending core snapshot", param( "Core snapshot", coreSnapshot ) );
    }
}
