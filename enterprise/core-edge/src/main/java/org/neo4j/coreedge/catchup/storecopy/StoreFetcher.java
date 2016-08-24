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

import org.neo4j.coreedge.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.coreedge.catchup.tx.TransactionLogCatchUpWriter;
import org.neo4j.coreedge.catchup.tx.TxPullClient;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class StoreFetcher
{
    private final Log log;
    private FileSystemAbstraction fs;
    private PageCache pageCache;
    private final LogProvider logProvider;
    private StoreCopyClient storeCopyClient;
    private TxPullClient txPullClient;
    private TransactionLogCatchUpFactory transactionLogFactory;
    private long retryInterval;

    public StoreFetcher( LogProvider logProvider,
                         FileSystemAbstraction fs, PageCache pageCache,
                         StoreCopyClient storeCopyClient, TxPullClient txPullClient,
                         TransactionLogCatchUpFactory transactionLogFactory,
                         long retryInterval )
    {
        this.logProvider = logProvider;
        this.storeCopyClient = storeCopyClient;
        this.txPullClient = txPullClient;
        this.fs = fs;
        this.pageCache = pageCache;
        this.transactionLogFactory = transactionLogFactory;
        this.retryInterval = retryInterval;
        log = logProvider.getLog( getClass() );
    }

    boolean tryCatchingUp( MemberId from, StoreId storeId, File storeDir ) throws StoreCopyFailedException, IOException
    {
        ReadOnlyTransactionIdStore transactionIdStore = new ReadOnlyTransactionIdStore( pageCache, storeDir );
        long lastCommittedTransactionId = transactionIdStore.getLastCommittedTransactionId();

        try ( TransactionLogCatchUpWriter writer = transactionLogFactory.create( storeDir, fs, pageCache, logProvider ) )
        {
            log.info( "Pulling transactions from: %d", lastCommittedTransactionId );
            try
            {
                long lastPulledTxId = txPullClient.pullTransactions( from, storeId, lastCommittedTransactionId, writer );
                log.info( "Txs streamed up to %d", lastPulledTxId );
                return true;
            }
            catch ( StoreCopyFailedException e )
            {
                return false;
            }
        }
    }

    void copyStore( MemberId from, StoreId storeId, File storeDir ) throws StoreCopyFailedException
    {
        try
        {
            log.info( "Copying store from %s", from );
            long lastFlushedTxId = storeCopyClient.copyStoreFiles( from, new StreamToDisk( storeDir, fs ) );
            log.info( "Store files streamed up to %d", lastFlushedTxId );

            try ( TransactionLogCatchUpWriter writer = transactionLogFactory.create( storeDir, fs, pageCache, logProvider ) )
            {
                log.info( "Pulling transactions from: %d", lastFlushedTxId - 1 );
                long lastPulledTxId = txPullClient.pullTransactions( from, storeId, lastFlushedTxId - 1, writer );
                log.info( "Txs streamed up to %d", lastPulledTxId );
            }
        }
        catch ( IOException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    public StoreId storeId( MemberId from ) throws StoreCatchUpFailedException
    {
        int maximumRetries = 5;
        String operation = format( "get store id from %s", from );
        return retry(client -> client.fetchStoreId( from ), operation, maximumRetries, retryInterval);
    }

    private <T> T  retry( ThrowingFunction<StoreCopyClient, T, StoreCatchUpFailedException> function,
                          String operation, int maxRetries, long retryInterval ) throws StoreCatchUpFailedException
    {
        int attempts = 0;
        while ( attempts++ < maxRetries )
        {
            log.info( "Attempt %d to %s.", attempts, operation );
            try
            {
                return function.apply(storeCopyClient);
            }
            catch ( StoreCatchUpFailedException e )
            {
                log.info( "Attempt %d to %s failed.", attempts, operation );
            }
            try
            {
                Thread.sleep( retryInterval );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                throw new StoreCatchUpFailedException( e );
            }
        }

        throw new StoreCatchUpFailedException( format( "Failed to %s after %d attempts", operation, attempts - 1 ) );
    }

}
