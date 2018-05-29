/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
