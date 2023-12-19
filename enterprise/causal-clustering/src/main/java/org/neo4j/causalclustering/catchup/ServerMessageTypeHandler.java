/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;


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
    public void channelRead( ChannelHandlerContext ctx, Object msg )
    {
        if ( protocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) )
        {
            RequestMessageType requestMessageType = RequestMessageType.from( ((ByteBuf) msg).readByte() );

            if ( requestMessageType.equals( RequestMessageType.TX_PULL_REQUEST ) )
            {
                protocol.expect( CatchupServerProtocol.State.TX_PULL );
            }
            else if ( requestMessageType.equals( RequestMessageType.STORE_ID ) )
            {
                protocol.expect( CatchupServerProtocol.State.GET_STORE_ID );
            }
            else if ( requestMessageType.equals( RequestMessageType.CORE_SNAPSHOT ) )
            {
                protocol.expect( CatchupServerProtocol.State.GET_CORE_SNAPSHOT );
            }
            else if ( requestMessageType.equals( RequestMessageType.PREPARE_STORE_COPY ) )
            {
                protocol.expect( CatchupServerProtocol.State.PREPARE_STORE_COPY );
            }
            else if ( requestMessageType.equals( RequestMessageType.STORE_FILE ) )
            {
                protocol.expect( CatchupServerProtocol.State.GET_STORE_FILE );
            }
            else if ( requestMessageType.equals( RequestMessageType.INDEX_SNAPSHOT ) )
            {
                protocol.expect( CatchupServerProtocol.State.GET_INDEX_SNAPSHOT );
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
