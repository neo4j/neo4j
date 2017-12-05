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
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.kernel.NeoStoreDataSource;

import static org.neo4j.causalclustering.catchup.CatchupServerProtocol.State;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH;

public class GetStoreRequestHandler extends SimpleChannelInboundHandler<GetStoreRequest>
{
    private final CatchupServerProtocol protocol;
    private final Supplier<NeoStoreDataSource> dataSource;

    private final StoreStreamingProcess storeStreamingProcess;

    public GetStoreRequestHandler( CatchupServerProtocol protocol, Supplier<NeoStoreDataSource> dataSource, StoreStreamingProcess storeStreamingProcess )
    {
        this.protocol = protocol;
        this.dataSource = dataSource;
        this.storeStreamingProcess = storeStreamingProcess;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, GetStoreRequest msg ) throws Exception
    {
        if ( !msg.expectedStoreId().equalToKernelStoreId( dataSource.get().getStoreId() ) )
        {
            storeStreamingProcess.fail( ctx, E_STORE_ID_MISMATCH );
        }
        else
        {
            storeStreamingProcess.perform( ctx );
        }
        protocol.expect( State.MESSAGE_TYPE );
    }
}
