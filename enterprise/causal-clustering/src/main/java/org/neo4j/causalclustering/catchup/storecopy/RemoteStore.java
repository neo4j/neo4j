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

import java.io.File;
import java.io.IOException;
import java.util.Optional;

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
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_BATCH;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

/**
 * Entry point for remote store related RPC.
 */
public class RemoteStore
{
    private final Log log;
    private final LocalDatabase localDatabase;
    private final Monitors monitors;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final LogProvider logProvider;
    private final StoreCopyClient storeCopyClient;
    private final TxPullClient txPullClient;
    private final TransactionLogCatchUpFactory transactionLogFactory;

    public RemoteStore( LogProvider logProvider, FileSystemAbstraction fs, PageCache pageCache, StoreCopyClient storeCopyClient, TxPullClient txPullClient,
            TransactionLogCatchUpFactory transactionLogFactory, Monitors monitors, LocalDatabase localDatabase )
    {
        this.logProvider = logProvider;
        this.storeCopyClient = storeCopyClient;
        this.txPullClient = txPullClient;
        this.fs = fs;
        this.pageCache = pageCache;
        this.transactionLogFactory = transactionLogFactory;
        this.monitors = monitors;
        this.log = logProvider.getLog( getClass() );
        this.localDatabase = localDatabase;
    }

    private CommitState getStoreState() throws IOException
    {
        ReadOnlyTransactionIdStore metaDataStore = new ReadOnlyTransactionIdStore( pageCache, localDatabase.storeDir() );
        long metaDataStoreTxId = metaDataStore.getLastCommittedTransactionId();

        Optional<Long> latestTransactionLogIndex = getLatestTransactionLogIndex( metaDataStoreTxId );

        //noinspection OptionalIsPresent
        if ( latestTransactionLogIndex.isPresent() )
        {
            return new CommitState( metaDataStoreTxId, latestTransactionLogIndex.get() );
        }
        else
        {
            return new CommitState( metaDataStoreTxId );
        }
    }

    private Optional<Long> getLatestTransactionLogIndex( long startTxId ) throws IOException
    {
        if ( !localDatabase.hasTxLogs() )
        {
            return Optional.empty();
        }

        // this is not really a read-only store, because it will create an empty transaction log if there is none
        ReadOnlyTransactionStore txStore = new ReadOnlyTransactionStore( pageCache, fs, localDatabase.storeDir(), new Monitors() );

        long lastTxId = BASE_TX_ID;
        try ( Lifespan ignored = new Lifespan( txStore ) )
        {
            TransactionCursor cursor;
            try
            {
                cursor = txStore.getTransactions( startTxId );
            }
            catch ( NoSuchTransactionException e )
            {
                return Optional.empty();
            }

            while ( cursor.next() )
            {
                CommittedTransactionRepresentation tx = cursor.get();
                lastTxId = tx.getCommitEntry().getTxId();
            }

            return Optional.of( lastTxId );
        }
    }

    /**
     * Later stages of the startup process require at least one transaction to
     * figure out the mapping between the transaction log and the consensus log.
     *
     * If there are no transaction logs then we can pull from and including
     * the index which the metadata store points to. This would be the case
     * for example with a backup taken during an idle period of the system.
     *
     * However, if there are transaction logs then we want to find out where
     * they end and pull from there, excluding the last one so that we do not
     * get duplicate entries.
     */
    public CatchupResult tryCatchingUp( AdvertisedSocketAddress from, StoreId expectedStoreId ) throws StoreCopyFailedException, IOException
    {
        CommitState commitState = getStoreState();
        log.info( "Store commit state: " + commitState );

        if ( commitState.transactionLogIndex().isPresent() )
        {
            return pullTransactions( from, expectedStoreId, localDatabase.storeDir(), commitState.transactionLogIndex().get() + 1, false );
        }
        else
        {
            CatchupResult catchupResult;
            if ( commitState.metaDataStoreIndex() == BASE_TX_ID )
            {
                return pullTransactions( from, expectedStoreId, localDatabase.storeDir(), commitState.metaDataStoreIndex() + 1, false );
            }
            else
            {
                catchupResult = pullTransactions( from, expectedStoreId, localDatabase.storeDir(), commitState.metaDataStoreIndex(), false );
                if ( catchupResult == E_TRANSACTION_PRUNED )
                {
                    return pullTransactions( from, expectedStoreId, localDatabase.storeDir(), commitState.metaDataStoreIndex() + 1, false );
                }
            }
            return catchupResult;
        }
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

    private CatchupResult pullTransactions( AdvertisedSocketAddress from, StoreId expectedStoreId, File storeDir, long fromTxId, boolean asPartOfStoreCopy ) throws IOException, StoreCopyFailedException
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
