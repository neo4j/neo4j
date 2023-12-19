/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.TxPullRequestResult;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpWriter;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
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
                null, storeCopyClient, txPullClient, factory( writer ), Config.defaults(), new Monitors() );

        // when
        AdvertisedSocketAddress localhost = new AdvertisedSocketAddress( "127.0.0.1", 1234 );
        CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( localhost );
        remoteStore.copy( catchupAddressProvider, storeId, new File( "destination" ), true );

        // then
        verify( storeCopyClient ).copyStoreFiles( eq( catchupAddressProvider ), eq( storeId ), any( StoreFileStreamProvider.class ), any(), any() );
        verify( txPullClient ).pullTransactions( eq( localhost ), eq( storeId ), anyLong(), isNull() );
    }

    @Test
    public void shouldSetLastPulledTransactionId() throws Exception
    {
        // given
        long lastFlushedTxId = 12;
        StoreId wantedStoreId = new StoreId( 1, 2, 3, 4 );
        AdvertisedSocketAddress localhost = new AdvertisedSocketAddress( "127.0.0.1", 1234 );
        CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( localhost );

        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        when( storeCopyClient.copyStoreFiles( eq( catchupAddressProvider ), eq( wantedStoreId ), any( StoreFileStreamProvider.class ), any(), any() ) )
                .thenReturn( lastFlushedTxId );

        TxPullClient txPullClient = mock( TxPullClient.class );
        when( txPullClient.pullTransactions( eq( localhost ), eq( wantedStoreId ), anyLong(), isNull() ) )
                .thenReturn( new TxPullRequestResult( SUCCESS_END_OF_STREAM, 13 ) );

        TransactionLogCatchUpWriter writer = mock( TransactionLogCatchUpWriter.class );

        RemoteStore remoteStore = new RemoteStore( NullLogProvider.getInstance(), mock( FileSystemAbstraction.class ),
                null, storeCopyClient, txPullClient, factory( writer ), Config.defaults(), new Monitors() );

        // when
        remoteStore.copy( catchupAddressProvider, wantedStoreId, new File( "destination" ), true );

        // then
        long previousTxId = lastFlushedTxId - 1; // the interface is defined as asking for the one preceding
        verify( txPullClient ).pullTransactions( eq( localhost ), eq( wantedStoreId ), eq( previousTxId ),
                isNull() );
    }

    @Test
    public void shouldCloseDownTxLogWriterIfTxStreamingFails() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        TxPullClient txPullClient = mock( TxPullClient.class );
        TransactionLogCatchUpWriter writer = mock( TransactionLogCatchUpWriter.class );
        CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( null );

        RemoteStore remoteStore = new RemoteStore( NullLogProvider.getInstance(), mock( FileSystemAbstraction.class ),
                null,
                storeCopyClient, txPullClient, factory( writer ), Config.defaults(), new Monitors() );

        doThrow( CatchUpClientException.class ).when( txPullClient )
                .pullTransactions( isNull(), eq( storeId ), anyLong(), any() );

        // when
        try
        {
            remoteStore.copy( catchupAddressProvider, storeId, null, true );
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
        when( factory.create( isNull(), any( FileSystemAbstraction.class ), isNull(), any( Config.class ),
                any( LogProvider.class ), anyLong(), anyBoolean(), anyBoolean(), anyBoolean() ) ).thenReturn( writer );
        return factory;
    }
}
