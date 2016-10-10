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
package org.neo4j.coreedge.catchup.tx;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyLogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;

public class TransactionLogCatchUpWriter implements TxPullResponseListener, AutoCloseable
{
    private final Lifespan lifespan = new Lifespan();
    private final File neoStoreFile;    ;
    private final PageCache pageCache;
    private final Log log;
    private final TransactionLogWriter writer;

    private long txId = -1;

    TransactionLogCatchUpWriter( File storeDir, FileSystemAbstraction fs, PageCache pageCache, LogProvider logProvider )
            throws IOException
    {
        this.neoStoreFile = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        this.pageCache = pageCache;
        this.log = logProvider.getLog( getClass() );

        PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, fs );
        ReadOnlyLogVersionRepository logVersionRepository = new ReadOnlyLogVersionRepository( pageCache, storeDir );
        ReadOnlyTransactionIdStore readOnlyTransactionIdStore = new ReadOnlyTransactionIdStore( pageCache, storeDir );
        LogFile logFile = lifespan.add( new PhysicalLogFile( fs, logFiles, Long.MAX_VALUE /*don't rotate*/,
                () -> readOnlyTransactionIdStore.getLastCommittedTransactionId() - 1, logVersionRepository,
                new Monitors().newMonitor( PhysicalLogFile.Monitor.class ), new LogHeaderCache( 10 ) ) );
        this.writer = new TransactionLogWriter( new LogEntryWriter( logFile.getWriter() ) );
    }

    @Override
    public void onTxReceived( TxPullResponse txPullResponse )
    {
        CommittedTransactionRepresentation tx = txPullResponse.tx();
        try
        {
            txId = tx.getCommitEntry().getTxId();
            writer.append( tx.getTransactionRepresentation(), txId );
        }
        catch ( IOException e )
        {
            log.error( "Failed when appending to transaction log", e );
        }
    }

    @Override
    public void close() throws IOException
    {
        lifespan.close();
        if ( txId != -1 )
        {
            MetaDataStore.setRecord( pageCache, neoStoreFile, LAST_TRANSACTION_ID, txId );
        }
    }
}
