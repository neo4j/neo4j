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

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.TriggerInfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GetIndexSnapshotRequestHandlerTest
{
    private static final StoreId STORE_ID_MISMATCHING = new StoreId( 1, 1, 1, 1 );
    private static final StoreId STORE_ID_MATCHING = new StoreId( 1, 2, 3, 4 );
    private final DefaultFileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();

    private final NeoStoreDataSource neoStoreDataSource = mock( NeoStoreDataSource.class );
    private final CheckPointer checkPointer = new FakeCheckPointer();
    private final PageCache pageCache = mock( PageCache.class );
    private EmbeddedChannel embeddedChannel;
    private CatchupServerProtocol catchupServerProtocol;

    @Before
    public void setup()
    {
        catchupServerProtocol = new CatchupServerProtocol();
        catchupServerProtocol.expect( CatchupServerProtocol.State.GET_STORE_FILE );
        GetIndexSnapshotRequestHandler getIndexSnapshotRequestHandler =
                new GetIndexSnapshotRequestHandler( catchupServerProtocol, () -> neoStoreDataSource, () -> checkPointer, new StoreFileStreamingProtocol(),
                        pageCache, fileSystemAbstraction );
        when( neoStoreDataSource.getStoreId() ).thenReturn( new org.neo4j.kernel.impl.store.StoreId( 1, 2, 5, 3, 4 ) );
        embeddedChannel = new EmbeddedChannel( getIndexSnapshotRequestHandler );
    }

    @Test
    public void shouldGiveProperErrorOnStoreIdMismatch()
    {
        embeddedChannel.writeInbound( new GetIndexFilesRequest( STORE_ID_MISMATCHING, 1, 1 ) );

        assertEquals( ResponseMessageType.STORE_COPY_FINISHED, embeddedChannel.readOutbound() );
        StoreCopyFinishedResponse expectedResponse = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH );
        assertEquals( expectedResponse, embeddedChannel.readOutbound() );

        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    @Test
    public void shouldGiveProperErrorOnTxBehind()
    {
        embeddedChannel.writeInbound( new GetIndexFilesRequest( STORE_ID_MATCHING, 1, 2 ) );

        assertEquals( ResponseMessageType.STORE_COPY_FINISHED, embeddedChannel.readOutbound() );
        StoreCopyFinishedResponse expectedResponse = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND );
        assertEquals( expectedResponse, embeddedChannel.readOutbound() );

        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    @Test
    public void shouldResetProtocolAndGiveErrorOnUncheckedException()
    {
        when( neoStoreDataSource.getStoreId() ).thenThrow( new IllegalStateException() );

        try
        {
            embeddedChannel.writeInbound( new GetIndexFilesRequest( STORE_ID_MATCHING, 1, 1 ) );
            fail();
        }
        catch ( IllegalStateException ignore )
        {

        }
        assertEquals( ResponseMessageType.STORE_COPY_FINISHED, embeddedChannel.readOutbound() );
        StoreCopyFinishedResponse expectedResponse = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.E_UNKNOWN );
        assertEquals( expectedResponse, embeddedChannel.readOutbound() );

        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    private class FakeCheckPointer implements CheckPointer
    {
        @Override
        public long checkPointIfNeeded( TriggerInfo triggerInfo )
        {
            return 1;
        }

        @Override
        public long tryCheckPoint( TriggerInfo triggerInfo )
        {
            return 1;
        }

        @Override
        public long forceCheckPoint( TriggerInfo triggerInfo )
        {
            return 1;
        }

        @Override
        public long lastCheckPointedTransactionId()
        {
            return 1;
        }
    }
}
