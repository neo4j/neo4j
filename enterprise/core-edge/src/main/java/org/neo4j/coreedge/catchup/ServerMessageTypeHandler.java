/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.catchup;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import org.neo4j.coreedge.catchup.CatchupServerProtocol;
import org.neo4j.coreedge.catchup.RequestMessageType;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.coreedge.catchup.CatchupServerProtocol.NextMessage;

public class ServerMessageTypeHandler extends ChannelInboundHandlerAdapter
{
    private final Log log;
    private final CatchupServerProtocol protocol;

    public ServerMessageTypeHandler( CatchupServerProtocol protocol, LogProvider logProvider )
    {
        this.protocol = protocol;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        if ( protocol.isExpecting( NextMessage.MESSAGE_TYPE ) )
        {
            RequestMessageType requestMessageType = RequestMessageType.from( ((ByteBuf) msg).readByte() );

            if ( requestMessageType.equals( RequestMessageType.TX_PULL_REQUEST ) )
            {
                protocol.expect( NextMessage.TX_PULL );
            }
            else if ( requestMessageType.equals( RequestMessageType.STORE ) )
            {
                protocol.expect( NextMessage.GET_STORE );
            }
            else if ( requestMessageType.equals( RequestMessageType.LOCK ) )
            {
                protocol.expect( NextMessage.LOCK_REQUEST );
            }
            else
            {
                log.warn( "No handler found for message type %s", requestMessageType );
            }

            ReferenceCountUtil.release( msg );
        }
        else
        {
            ctx.fireChannelRead( msg );
        }
    }
}
