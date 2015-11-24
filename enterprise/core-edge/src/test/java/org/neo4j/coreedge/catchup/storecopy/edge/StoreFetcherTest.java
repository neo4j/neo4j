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
package org.neo4j.coreedge.catchup.storecopy.edge;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import org.neo4j.coreedge.catchup.storecopy.edge.StoreCopyFailedException;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFileStreams;
import org.neo4j.coreedge.catchup.tx.edge.TransactionLogCatchUpFactory;
import org.neo4j.coreedge.catchup.tx.edge.TransactionLogCatchUpWriter;
import org.neo4j.coreedge.catchup.tx.edge.TxPullClient;
import org.neo4j.coreedge.catchup.tx.edge.TxPullResponseListener;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreCopyClient;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;

public class StoreFetcherTest
{
    @Test
    public void shouldCopyStoreFilesAndPullTransactions() throws Exception
    {
        // given
        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        TxPullClient txPullClient = mock( TxPullClient.class );
        TransactionLogCatchUpWriter writer = mock( TransactionLogCatchUpWriter.class );

        StoreFetcher fetcher = new StoreFetcher( NullLogProvider.getInstance(), mock( FileSystemAbstraction.class ),
                null,
                storeCopyClient, txPullClient, factory( writer ) );

        // when
        AdvertisedSocketAddress localhost = address( "localhost:1980" );
        fetcher.copyStore( localhost, new File( "destination" ) );

        // then
        verify( storeCopyClient ).copyStoreFiles( eq( localhost ), any( StoreFileStreams.class ) );
        verify( txPullClient ).pullTransactions( eq( localhost ), anyLong(), any( TxPullResponseListener.class ) );
    }

    @Test
    public void shouldSetLastPulledTransactionId() throws Exception
    {
        // given
        long lastFlushedTxId = 12;
        long lastPulledTxId = 34;

        AdvertisedSocketAddress localhost = address( "localhost:2001" );

        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        when( storeCopyClient.copyStoreFiles( eq( localhost ), any( StoreFileStreams.class ) ) )
                .thenReturn( lastFlushedTxId );

        TxPullClient txPullClient = mock( TxPullClient.class );
        when( txPullClient.pullTransactions( eq( localhost ), anyLong(), any( TxPullResponseListener.class ) ) )
                .thenReturn( lastPulledTxId );

        TransactionLogCatchUpWriter writer = mock( TransactionLogCatchUpWriter.class );

        StoreFetcher fetcher = new StoreFetcher( NullLogProvider.getInstance(), mock( FileSystemAbstraction.class ),
                null,
                storeCopyClient, txPullClient, factory( writer ) );

        // when
        fetcher.copyStore( localhost, new File( "destination" ) );

        // then
        verify( txPullClient ).pullTransactions( eq( localhost ), eq( lastFlushedTxId ), any( TxPullResponseListener.class ) );
        verify( writer ).setCorrectTransactionId( lastPulledTxId );
    }

    @Test
    public void shouldCloseDownTxLogWriterIfTxStreamingFails() throws Exception
    {
        // given
        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        TxPullClient txPullClient = mock( TxPullClient.class );
        TransactionLogCatchUpWriter writer = mock( TransactionLogCatchUpWriter.class );

        StoreFetcher fetcher = new StoreFetcher( NullLogProvider.getInstance(), mock( FileSystemAbstraction.class ),
                null,
                storeCopyClient, txPullClient, factory( writer ) );

        doThrow( StoreCopyFailedException.class ).when( txPullClient )
                .pullTransactions( any( AdvertisedSocketAddress.class ), anyLong(), any( TransactionLogCatchUpWriter.class ) );

        // when
        try
        {
            fetcher.copyStore( null, null );
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
                any( PageCache.class ) ) ).thenReturn( writer );
        return factory;
    }
}
