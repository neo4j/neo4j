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
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.EventHandlerProvider;
import org.neo4j.causalclustering.messaging.EventId;

import static org.neo4j.causalclustering.catchup.CatchupServerProtocol.State;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.Info;
import static org.neo4j.causalclustering.messaging.EventHandler.Param.param;

public class GetStoreIdRequestHandler extends SimpleChannelInboundHandler<GetStoreIdRequest>
{
    private final CatchupServerProtocol protocol;
    private final Supplier<StoreId> storeIdSupplier;
    private final EventHandlerProvider eventHandlerProvider;

    public GetStoreIdRequestHandler( CatchupServerProtocol protocol, Supplier<StoreId> storeIdSupplier, EventHandlerProvider eventHandlerProvider )
    {
        this.protocol = protocol;
        this.storeIdSupplier = storeIdSupplier;
        this.eventHandlerProvider = eventHandlerProvider;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, GetStoreIdRequest msg )
    {
        ctx.writeAndFlush( ResponseMessageType.STORE_ID );
        StoreId storeId = storeIdSupplier.get();
        ctx.writeAndFlush( storeId );
        protocol.expect( State.MESSAGE_TYPE );
        eventHandlerProvider.eventHandler( EventId.from( msg.messageId() ) ).on( Info, "Sent store id", param( "Store id", storeId ) );
    }
}
