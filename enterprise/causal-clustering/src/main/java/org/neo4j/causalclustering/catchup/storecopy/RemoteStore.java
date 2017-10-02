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

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.TxPullRequestResult;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpWriter;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_BATCH;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

/**
 * Entry point for remote store related RPC.
 */
public class RemoteStore
{
    private final Log log;
    private final Monitors monitors;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final LogProvider logProvider;
    private final StoreCopyClient storeCopyClient;
    private final TxPullClient txPullClient;
    private final TransactionLogCatchUpFactory transactionLogFactory;

    public RemoteStore( LogProvider logProvider, FileSystemAbstraction fs, PageCache pageCache, StoreCopyClient storeCopyClient, TxPullClient txPullClient,
            TransactionLogCatchUpFactory transactionLogFactory, Monitors monitors )
    {
        this.logProvider = logProvider;
        this.storeCopyClient = storeCopyClient;
        this.txPullClient = txPullClient;
        this.fs = fs;
        this.pageCache = pageCache;
        this.transactionLogFactory = transactionLogFactory;
        this.monitors = monitors;
        this.log = logProvider.getLog( getClass() );
    }

    /**
     * Later stages of the startup process require at least one transaction to
     * figure out the mapping between the transaction log and the consensus log.
     * <p>
     * If there are no transaction logs then we can pull from and including
     * the index which the metadata store points to. This would be the case
     * for example with a backup taken during an idle period of the system.
     * <p>
     * However, if there are transaction logs then we want to find out where
     * they end and pull from there, excluding the last one so that we do not
     * get duplicate entries.
     */
    private long getPullIndex( File storeDir ) throws IOException
    {
        /* this is the metadata store */
        ReadOnlyTransactionIdStore txIdStore = new ReadOnlyTransactionIdStore( pageCache, storeDir );

        /* Clean as in clean shutdown. Without transaction logs this should be the truth,
        * but otherwise it can be used as a starting point for scanning the logs. */
        long lastCleanTxId = txIdStore.getLastCommittedTransactionId();
        log.info( "Last Clean Tx Id: %d", lastCleanTxId );

        /* these are the transaction logs */
        ReadOnlyTransactionStore txStore = new ReadOnlyTransactionStore( pageCache, fs, storeDir, new Monitors() );

        long lastTxId = BASE_TX_ID;
        try ( Lifespan ignored = new Lifespan( txStore ); TransactionCursor cursor = txStore.getTransactions( lastCleanTxId ) )
        {
            while ( cursor.next() )
            {
                CommittedTransactionRepresentation tx = cursor.get();
                lastTxId = tx.getCommitEntry().getTxId();
            }

            if ( lastTxId < lastCleanTxId )
            {
                throw new IllegalStateException( "Metadata index was higher than transaction log index." );
            }

            // we don't want to pull a transaction we already have in the log, hence +1
            return lastTxId + 1;
        }
        catch ( NoSuchTransactionException e )
        {
            log.info( "No transaction logs found. Will use metadata store as base for pull request." );
            return Math.max( TransactionIdStore.BASE_TX_ID + 1, lastCleanTxId );
        }
    }

    public CatchupResult tryCatchingUp( AdvertisedSocketAddress from, StoreId expectedStoreId, File storeDir ) throws StoreCopyFailedException, IOException
    {
        long pullIndex = getPullIndex( storeDir );
        return pullTransactions( from, expectedStoreId, storeDir, pullIndex, false );
    }

    public void copy( AdvertisedSocketAddress from, StoreId expectedStoreId, File destDir )
            throws StoreCopyFailedException, StreamingTransactionsFailedException
    {
        try
        {
            log.info( "Copying store from %s", from );
            long lastFlushedTxId;
            try ( StreamToDisk storeFileStreams = new StreamToDisk( destDir, fs, pageCache, monitors ) )
            {
                lastFlushedTxId = storeCopyClient.copyStoreFiles( from, expectedStoreId, storeFileStreams );
            }

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

    private CatchupResult pullTransactions( AdvertisedSocketAddress from, StoreId expectedStoreId, File storeDir, long fromTxId, boolean asPartOfStoreCopy )
            throws IOException, StoreCopyFailedException
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

    public StoreId getStoreId( AdvertisedSocketAddress from ) throws StoreIdDownloadFailedException
    {
        return storeCopyClient.fetchStoreId( from );
    }
}
