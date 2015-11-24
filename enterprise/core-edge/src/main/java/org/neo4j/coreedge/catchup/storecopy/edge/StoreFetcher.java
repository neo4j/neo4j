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

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.catchup.tx.edge.TransactionLogCatchUpFactory;
import org.neo4j.coreedge.catchup.tx.edge.TransactionLogCatchUpWriter;
import org.neo4j.coreedge.catchup.tx.edge.TxPullClient;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class StoreFetcher
{
    private final Log log;
    private FileSystemAbstraction fs;
    private PageCache pageCache;
    private StoreCopyClient storeCopyClient;
    private TxPullClient txPullClient;
    private TransactionLogCatchUpFactory transactionLogFactory;

    public StoreFetcher( LogProvider logProvider,
                         FileSystemAbstraction fs, PageCache pageCache,
                         StoreCopyClient storeCopyClient, TxPullClient txPullClient,
                         TransactionLogCatchUpFactory transactionLogFactory )
    {
        this.storeCopyClient = storeCopyClient;
        this.txPullClient = txPullClient;
        this.fs = fs;
        this.pageCache = pageCache;
        this.transactionLogFactory = transactionLogFactory;
        log = logProvider.getLog( getClass() );
    }

    public void copyStore( AdvertisedSocketAddress from, File storeDir ) throws StoreCopyFailedException
    {
        try
        {
            log.info( "Copying store from %s", from );
            long lastFlushedTxId = storeCopyClient.copyStoreFiles( from, new StreamToDisk( storeDir, fs ) );
            log.info( "Store files streamed up to %d", lastFlushedTxId );

            try ( TransactionLogCatchUpWriter writer = transactionLogFactory.create( storeDir, fs, pageCache ) )
            {
                long lastPulledTxId = txPullClient.pullTransactions( from, lastFlushedTxId, writer );
                log.info( "Txs streamed up to %d", lastPulledTxId );
                writer.setCorrectTransactionId( lastPulledTxId );
            }
        }
        catch ( IOException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }
}
