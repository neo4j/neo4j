/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
    public void channelRead( ChannelHandlerContext ctx, Object msg )
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
            case PREPARE_STORE_COPY_RESPONSE:
                protocol.expect( CatchupClientProtocol.State.PREPARE_STORE_COPY_RESPONSE );
                break;
            case INDEX_SNAPSHOT_RESPONSE:
                protocol.expect( CatchupClientProtocol.State.INDEX_SNAPSHOT_RESPONSE );
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
