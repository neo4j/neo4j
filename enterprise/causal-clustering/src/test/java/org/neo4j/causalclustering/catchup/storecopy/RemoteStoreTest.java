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

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.catchup.TxPullRequestResult;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpWriter;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.catchup.tx.TxPullResponseListener;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;

public class RemoteStoreTest
{
    @Test
    public void shouldCopyStoreFilesAndPullTransactions() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        TxPullClient txPullClient = mock( TxPullClient.class );
        when( txPullClient.pullTransactions( any(), any(), anyLong(), any() ) )
                .thenReturn( new TxPullRequestResult( SUCCESS_END_OF_STREAM, 13 ) );
        TransactionLogCatchUpWriter writer = mock( TransactionLogCatchUpWriter.class );

        RemoteStore remoteStore = new RemoteStore( NullLogProvider.getInstance(), mock( FileSystemAbstraction.class ),
                null, storeCopyClient, txPullClient, factory( writer ), new Monitors() );

        // when
        AdvertisedSocketAddress localhost = new AdvertisedSocketAddress( "127.0.0.1", 1234 );
        remoteStore.copy( localhost, storeId, new File( "destination" ) );

        // then
        verify( storeCopyClient ).copyStoreFiles( eq( localhost ), eq( storeId ), any( StoreFileStreams.class ) );
        verify( txPullClient ).pullTransactions( eq( localhost ), eq( storeId ), anyLong(), any( TxPullResponseListener.class ) );
    }

    @Test
    public void shouldSetLastPulledTransactionId() throws Exception
    {
        // given
        long lastFlushedTxId = 12;
        StoreId wantedStoreId = new StoreId( 1, 2, 3, 4 );
        AdvertisedSocketAddress localhost = new AdvertisedSocketAddress( "127.0.0.1", 1234 );

        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        when( storeCopyClient.copyStoreFiles( eq( localhost ), eq( wantedStoreId ), any( StoreFileStreams.class ) ) )
                .thenReturn( lastFlushedTxId );

        TxPullClient txPullClient = mock( TxPullClient.class );
        when( txPullClient.pullTransactions( eq( localhost ), eq( wantedStoreId ), anyLong(), any( TxPullResponseListener.class ) ) )
                .thenReturn( new TxPullRequestResult( SUCCESS_END_OF_STREAM, 13 ) );

        TransactionLogCatchUpWriter writer = mock( TransactionLogCatchUpWriter.class );

        RemoteStore remoteStore = new RemoteStore( NullLogProvider.getInstance(), mock( FileSystemAbstraction.class ),
                null, storeCopyClient, txPullClient, factory( writer ), new Monitors() );

        // when
        remoteStore.copy( localhost, wantedStoreId, new File( "destination" ) );

        // then
        long previousTxId = lastFlushedTxId - 1; // the interface is defined as asking for the one preceding
        verify( txPullClient ).pullTransactions( eq( localhost ), eq( wantedStoreId ), eq( previousTxId ),
                any( TxPullResponseListener.class ) );
    }

    @Test
    public void shouldCloseDownTxLogWriterIfTxStreamingFails() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        TxPullClient txPullClient = mock( TxPullClient.class );
        TransactionLogCatchUpWriter writer = mock( TransactionLogCatchUpWriter.class );

        RemoteStore remoteStore = new RemoteStore( NullLogProvider.getInstance(), mock( FileSystemAbstraction.class ),
                null,
                storeCopyClient, txPullClient, factory( writer ), new Monitors() );

        doThrow( StoreCopyFailedException.class ).when( txPullClient )
                .pullTransactions( any( AdvertisedSocketAddress.class ), eq( storeId ), anyLong(), any( TransactionLogCatchUpWriter.class ) );

        // when
        try
        {
            remoteStore.copy( null, storeId, null );
        }
        catch ( StoreCopyFailedException e )
        {
            // expected
        }

        // then
        verify( writer ).close();
    }

    private TransactionLogCatchUpFactory factory( TransactionLogCatchUpWriter writer ) throws IOException
    {
        TransactionLogCatchUpFactory factory = mock( TransactionLogCatchUpFactory.class );
        when( factory.create( any( File.class ), any( FileSystemAbstraction.class ),
                any( PageCache.class ), any( LogProvider.class ), anyLong(), anyBoolean() ) ).thenReturn( writer );
        return factory;
    }
}
