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
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.TxPullRequestResult;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpWriter;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_BATCH;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;

public class StoreFetcher
{
    private final Log log;
    private final Monitors monitors;
    private FileSystemAbstraction fs;
    private PageCache pageCache;
    private final LogProvider logProvider;
    private StoreCopyClient storeCopyClient;
    private TxPullClient txPullClient;
    private TransactionLogCatchUpFactory transactionLogFactory;

    public StoreFetcher( LogProvider logProvider,
                         FileSystemAbstraction fs, PageCache pageCache,
                         StoreCopyClient storeCopyClient, TxPullClient txPullClient,
                         TransactionLogCatchUpFactory transactionLogFactory,
                         Monitors monitors )
    {
        this.logProvider = logProvider;
        this.storeCopyClient = storeCopyClient;
        this.txPullClient = txPullClient;
        this.fs = fs;
        this.pageCache = pageCache;
        this.transactionLogFactory = transactionLogFactory;
        log = logProvider.getLog( getClass() );
        this.monitors = monitors;
    }

    public CatchupResult tryCatchingUp( MemberId from, StoreId expectedStoreId, File storeDir ) throws StoreCopyFailedException, IOException
    {
        ReadOnlyTransactionIdStore transactionIdStore = new ReadOnlyTransactionIdStore( pageCache, storeDir );
        long lastCommittedTxId = transactionIdStore.getLastCommittedTransactionId();
        return pullTransactions( from, expectedStoreId, storeDir, lastCommittedTxId + 1, false );
    }

    public void copyStore( MemberId from, StoreId expectedStoreId, File destDir )
            throws StoreCopyFailedException, StreamingTransactionsFailedException
    {
        try
        {
            log.info( "Copying store from %s", from );
            long lastFlushedTxId = storeCopyClient.copyStoreFiles( from, expectedStoreId, new StreamToDisk( destDir, fs, monitors ) );

            log.info( "Store files need to be recovered starting from: %d", lastFlushedTxId );

            CatchupResult catchupResult = pullTransactions( from, expectedStoreId, destDir, lastFlushedTxId, true );
            if ( catchupResult != SUCCESS_END_OF_STREAM )
            {
                throw new StreamingTransactionsFailedException( "Failed to pull transactions: " + catchupResult );
            }
        }
        catch ( IOException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    private CatchupResult pullTransactions( MemberId from, StoreId expectedStoreId, File storeDir, long fromTxId, boolean asPartOfStoreCopy ) throws IOException, StoreCopyFailedException
    {
        try ( TransactionLogCatchUpWriter writer = transactionLogFactory.create( storeDir, fs, pageCache, logProvider, fromTxId, asPartOfStoreCopy ) )
        {
            log.info( "Pulling transactions from: %d", fromTxId );

            long previousTxId = fromTxId - 1;

            CatchupResult lastStatus;
            do
            {
                TxPullRequestResult result = txPullClient.pullTransactions( from, expectedStoreId, previousTxId, writer );
                lastStatus = result.catchupResult();
                previousTxId = result.lastTxId();
            }
            while ( lastStatus == SUCCESS_END_OF_BATCH );

            return lastStatus;
        }
        catch ( CatchUpClientException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    public StoreId getStoreIdOf( MemberId from ) throws StoreIdDownloadFailedException
    {
        return storeCopyClient.fetchStoreId( from );
    }
}
