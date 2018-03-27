/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
