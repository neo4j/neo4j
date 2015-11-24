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
package org.neo4j.coreedge.catchup.tx.edge;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.coreedge.catchup.CatchupClientProtocol;
import org.neo4j.coreedge.catchup.tx.core.TxStreamFinishedResponse;

import static org.neo4j.coreedge.catchup.CatchupClientProtocol.NextMessage;

public class TxStreamFinishedResponseHandler extends SimpleChannelInboundHandler<TxStreamFinishedResponse>
{
    private final CatchupClientProtocol protocol;
    private final TxStreamCompleteListener listener;

    public TxStreamFinishedResponseHandler( CatchupClientProtocol protocol, TxStreamCompleteListener
            streamingListener )
    {
        this.protocol = protocol;
        this.listener = streamingListener;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, TxStreamFinishedResponse msg ) throws Exception
    {
        listener.onTxStreamingComplete( msg.lastTransactionIdSent() );
        protocol.expect( NextMessage.MESSAGE_TYPE );
    }
}
