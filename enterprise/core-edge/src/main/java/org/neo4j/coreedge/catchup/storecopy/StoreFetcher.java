/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup.storecopy;

import java.io.File;
import java.io.IOException;

import org.neo4j.coreedge.catchup.CatchUpClientException;
import org.neo4j.coreedge.catchup.CatchupResult;
import org.neo4j.coreedge.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.coreedge.catchup.tx.TransactionLogCatchUpWriter;
import org.neo4j.coreedge.catchup.tx.TxPullClient;
import org.neo4j.coreedge.discovery.NoKnownAddressesException;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.coreedge.catchup.CatchupResult.SUCCESS;

public class StoreFetcher
{
    private final Log log;
    private FileSystemAbstraction fs;
    private PageCache pageCache;
    private final LogProvider logProvider;
    private StoreCopyClient storeCopyClient;
    private TxPullClient txPullClient;
    private TransactionLogCatchUpFactory transactionLogFactory;

    public StoreFetcher( LogProvider logProvider,
                         FileSystemAbstraction fs, PageCache pageCache,
                         StoreCopyClient storeCopyClient, TxPullClient txPullClient,
                         TransactionLogCatchUpFactory transactionLogFactory )
    {
        this.logProvider = logProvider;
        this.storeCopyClient = storeCopyClient;
        this.txPullClient = txPullClient;
        this.fs = fs;
        this.pageCache = pageCache;
        this.transactionLogFactory = transactionLogFactory;
        log = logProvider.getLog( getClass() );
    }

    public CatchupResult tryCatchingUp( MemberId from, StoreId expectedStoreId, File storeDir ) throws StoreCopyFailedException, IOException
    {
        ReadOnlyTransactionIdStore transactionIdStore = new ReadOnlyTransactionIdStore( pageCache, storeDir );
        long lastCommittedTxId = transactionIdStore.getLastCommittedTransactionId();
        return pullTransactions( from, expectedStoreId, storeDir, lastCommittedTxId - 1 );
    }

    private CatchupResult pullTransactions( MemberId from, StoreId expectedStoreId, File storeDir, long fromTxId ) throws IOException, StoreCopyFailedException
    {
        try ( TransactionLogCatchUpWriter writer = transactionLogFactory.create( storeDir, fs, pageCache, logProvider ) )
        {
            log.info( "Pulling transactions from: %d", fromTxId );
            return txPullClient.pullTransactions( from, expectedStoreId, fromTxId, writer );
        }
        catch ( CatchUpClientException | NoKnownAddressesException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    public void copyStore( MemberId from, StoreId expectedStoreId, File destDir ) throws StoreCopyFailedException
    {
        try
        {
            log.info( "Copying store from %s", from );
            long lastFlushedTxId = storeCopyClient.copyStoreFiles( from, expectedStoreId, new StreamToDisk( destDir, fs ) );

            // We require at least one transaction for extracting the log index of the consensus log.
            // Given there might not have been any activity on the source server we need to ask for the
            // log entry for the lastFlushedTxId even though we've already applied its contents
            long pullTxIndex = lastFlushedTxId - 1;
            log.info( "Store files need to be recovered starting from: %d", pullTxIndex );

            CatchupResult catchupResult = pullTransactions( from, expectedStoreId, destDir, pullTxIndex );
            if ( catchupResult != SUCCESS )
            {
                throw new StoreCopyFailedException( "Failed to pull transactions: " + catchupResult );
            }
        }
        catch ( IOException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    public StoreId getStoreIdOf( MemberId from ) throws StoreIdDownloadFailedException
    {
        String operation = "get store id from " + from;
        long retryInterval = 5_000;
        int attempts = 0;

        while ( attempts++ < 5 )
        {
            log.info( "Attempt #%d to %s.", attempts, operation );

            try
            {
                return storeCopyClient.fetchStoreId( from );
            }
            catch ( StoreIdDownloadFailedException e )
            {
                log.info( "Attempt #%d to %s failed.", attempts, operation );
            }

            try
            {
                log.info( "Next attempt to %s in %d ms.", operation, retryInterval );
                Thread.sleep( retryInterval );
                retryInterval = retryInterval * 2;
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                throw new StoreIdDownloadFailedException( e );
            }
        }

        throw new StoreIdDownloadFailedException( "Failed to " + operation + " after " + (attempts - 1) + " attempts" );
    }
}
