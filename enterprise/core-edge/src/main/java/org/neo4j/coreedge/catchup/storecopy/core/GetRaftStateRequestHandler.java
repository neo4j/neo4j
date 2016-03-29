/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup.storecopy.core;

import java.io.IOException;
import java.util.Map;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.coreedge.catchup.CatchupServerProtocol;
import org.neo4j.coreedge.catchup.ResponseMessageType;
import org.neo4j.coreedge.catchup.storecopy.edge.GetRaftStateRequest;
import org.neo4j.coreedge.raft.state.CoreState;

import static org.neo4j.coreedge.catchup.CatchupServerProtocol.NextMessage;

public class GetRaftStateRequestHandler extends SimpleChannelInboundHandler<GetRaftStateRequest>
{
    private final CatchupServerProtocol protocol;
    private final CoreState coreState;

    public GetRaftStateRequestHandler( CatchupServerProtocol protocol, CoreState coreState )
    {
        this.protocol = protocol;
        this.coreState = coreState;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, GetRaftStateRequest msg ) throws Exception
    {
        sendStates( ctx, coreState.snapshot() );
        protocol.expect( NextMessage.MESSAGE_TYPE );
    }

    private void sendStates( ChannelHandlerContext ctx, Map<RaftStateType, Object> snapshot ) throws IOException
    {
        for ( Map.Entry<RaftStateType, Object> entry : snapshot.entrySet() )
        {
            ctx.writeAndFlush( ResponseMessageType.RAFT_STATE_SNAPSHOT );
            ctx.writeAndFlush( new RaftStateSnapshot( entry.getKey(), entry.getValue() ) );
        }
    }
}
