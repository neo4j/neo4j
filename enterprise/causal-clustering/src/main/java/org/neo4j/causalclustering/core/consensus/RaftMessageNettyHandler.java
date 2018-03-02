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
package org.neo4j.causalclustering.core.consensus;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.causalclustering.messaging.Inbound;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

@ChannelHandler.Sharable
public class RaftMessageNettyHandler extends SimpleChannelInboundHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>>
        implements Inbound<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>>
{
    private Inbound.MessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> actual;
    private Log log;

    public RaftMessageNettyHandler( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void registerHandler( Inbound.MessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> actual )
    {
        this.actual = actual;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext channelHandlerContext, RaftMessages.ReceivedInstantClusterIdAwareMessage<?> incomingMessage )
    {
        try
        {
            actual.handle( incomingMessage );
        }
        catch ( Exception e )
        {
            log.error( format( "Failed to process message %s", incomingMessage ), e );
        }
    }
}
