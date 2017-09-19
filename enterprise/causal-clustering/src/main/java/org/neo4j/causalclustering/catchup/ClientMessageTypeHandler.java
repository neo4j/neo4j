/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.causalclustering.catchup.ResponseMessageType.from;

public class ClientMessageTypeHandler extends ChannelInboundHandlerAdapter
{
    private final Log log;
    private final CatchupClientProtocol protocol;

    public ClientMessageTypeHandler( CatchupClientProtocol protocol, LogProvider logProvider )
    {
        this.protocol = protocol;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        if ( protocol.isExpecting( CatchupClientProtocol.State.MESSAGE_TYPE ) )
        {
            byte byteValue = ((ByteBuf) msg).readByte();
            ResponseMessageType responseMessageType = from( byteValue );

            switch ( responseMessageType )
            {
                case STORE_ID:
                    protocol.expect( CatchupClientProtocol.State.STORE_ID );
                    break;
                case TX:
                    protocol.expect( CatchupClientProtocol.State.TX_PULL_RESPONSE );
                    break;
                case FILE:
                    protocol.expect( CatchupClientProtocol.State.FILE_HEADER );
                    break;
                case STORE_COPY_FINISHED:
                    protocol.expect( CatchupClientProtocol.State.STORE_COPY_FINISHED );
                    break;
                case CORE_SNAPSHOT:
                    protocol.expect( CatchupClientProtocol.State.CORE_SNAPSHOT );
                    break;
                case TX_STREAM_FINISHED:
                    protocol.expect( CatchupClientProtocol.State.TX_STREAM_FINISHED );
                    break;
                default:
                    log.warn( "No handler found for message type %s (%d)", responseMessageType.name(), byteValue );
            }

            ReferenceCountUtil.release( msg );
        }
        else
        {
            ctx.fireChannelRead( msg );
        }
    }
}
