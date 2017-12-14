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
import org.junit.Test;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.kernel.NeoStoreDataSource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH;

public class GetStoreRequestHandlerTest
{
    private final StoreStreamingProcess streamingProcess = mock( StoreStreamingProcess.class );
    private final NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );
    private final ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );

    private final CatchupServerProtocol protocol = new CatchupServerProtocol();

    @Test
    public void shouldInvokedStoreCopyProcess() throws Exception
    {
        // given
        when( dataSource.getStoreId() ).thenReturn( new org.neo4j.kernel.impl.store.StoreId( 1, 2, 0, 3, 4 ) );
        GetStoreRequestHandler handler = new GetStoreRequestHandler( protocol, () -> dataSource, streamingProcess );

        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        GetStoreRequest msg = new GetStoreRequest( storeId );

        // when
        handler.channelRead0( ctx, msg );

        // then
        verify( streamingProcess ).perform( ctx );
        protocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE );
    }

    @Test
    public void shouldFailStoreCopyProcessOnWrongStoreId() throws Exception
    {
        // given
        GetStoreRequestHandler handler = new GetStoreRequestHandler( protocol, () -> dataSource, streamingProcess );
        when( dataSource.getStoreId() ).thenReturn( new org.neo4j.kernel.impl.store.StoreId( 5, 6, 7, 8, 9 ) );

        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        GetStoreRequest msg = new GetStoreRequest( storeId );

        // when
        handler.channelRead0( ctx, msg );

        // then
        verify( streamingProcess ).fail( ctx, E_STORE_ID_MISMATCH );
        protocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE );
    }
}
