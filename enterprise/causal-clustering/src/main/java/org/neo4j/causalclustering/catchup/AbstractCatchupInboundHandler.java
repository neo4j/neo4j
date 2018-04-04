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
package org.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public abstract class AbstractCatchupInboundHandler<T> extends SimpleChannelInboundHandler<T>
{
    protected final CatchUpResponseHandler handler;
    protected final CatchupClientProtocol protocol;

    public AbstractCatchupInboundHandler( CatchUpResponseHandler catchUpResponseHandler, CatchupClientProtocol catchupClientProtocol )
    {
        this.handler = catchUpResponseHandler;
        this.protocol = catchupClientProtocol;
    }

    @Override
    public void channelInactive( ChannelHandlerContext ctx )
    {
        handler.onChannelInactive();
        protocol.expect( CatchupClientProtocol.State.MESSAGE_TYPE );
    }
}
