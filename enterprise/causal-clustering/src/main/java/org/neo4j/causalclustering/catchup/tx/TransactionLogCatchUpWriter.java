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
package org.neo4j.causalclustering.catchup.tx;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

public class TransactionLogCatchUpWriter implements TxPullResponseListener, AutoCloseable
{
    private final Lifespan lifespan = new Lifespan();
    private final PageCache pageCache;
    private final Log log;
    private final boolean asPartOfStoreCopy;
    private final TransactionLogWriter writer;
    private final LogFiles logFiles;
    private final File storeDir;

    private long lastTxId = -1;
    private long expectedTxId;

    TransactionLogCatchUpWriter( File storeDir, FileSystemAbstraction fs, PageCache pageCache, Config config,
            LogProvider logProvider, long fromTxId, boolean asPartOfStoreCopy, boolean keepTxLogsInStoreDir ) throws IOException
    {
        this.pageCache = pageCache;
        this.log = logProvider.getLog( getClass() );
        this.asPartOfStoreCopy = asPartOfStoreCopy;
        LogFilesBuilder logFilesBuilder = LogFilesBuilder.activeFilesBuilder( storeDir, fs, pageCache )
                .withLastCommittedTransactionIdSupplier( () -> fromTxId - 1 );

//        new RuntimeException(
//        String.format(
//                "Constructing log catchup writer used for batch update. " +
//                        "KEEP TX LOGS FLAG IS %s; when false then it will add config. %s=%s, %s=%s, %s=%s, currentThread=%s\n",
//                keepTxLogsInStoreDir,
//                GraphDatabaseSettings.keep_logical_logs.name(), config.get( GraphDatabaseSettings.keep_logical_logs ),
//                GraphDatabaseSettings.check_point_interval_time.name(), config.get( GraphDatabaseSettings.check_point_interval_time ),
//                GraphDatabaseSettings.logical_log_rotation_threshold.name(), config.get( GraphDatabaseSettings.logical_log_rotation_threshold ),
//                Thread.currentThread().getName()
//                ) ).printStackTrace( System.out );
        if ( !keepTxLogsInStoreDir )
        {
            logFilesBuilder.withConfig( config );
        }
        this.logFiles = logFilesBuilder.build();
        this.lifespan.add( logFiles );
        this.writer = new TransactionLogWriter( new LogEntryWriter( logFiles.getLogFile().getWriter() ) );
        this.storeDir = storeDir;
        this.expectedTxId = fromTxId;
    }

    @Override
    public synchronized void onTxReceived( TxPullResponse txPullResponse )
    {
        CommittedTransactionRepresentation tx = txPullResponse.tx();
        long receivedTxId = tx.getCommitEntry().getTxId();
        if ( logFiles.getLogFile().rotationNeeded() )
        {
            try
            {
                logFiles.getLogFile().rotate();
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        }

        if ( receivedTxId != expectedTxId )
        {
            throw new RuntimeException( format( "Expected txId: %d but got: %d", expectedTxId, receivedTxId ) );
        }

        lastTxId = receivedTxId;
        expectedTxId++;

        try
        {
            writer.append( tx.getTransactionRepresentation(), lastTxId );
        }
        catch ( IOException e )
        {
            log.error( "Failed when appending to transaction log", e );
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        if ( asPartOfStoreCopy )
        {
            /* A checkpoint which points to the beginning of the log file, meaning that
            all the streamed transactions will be applied as part of recovery. */
            long logVersion = logFiles.getHighestLogVersion();
            writer.checkPoint( new LogPosition( logVersion, LOG_HEADER_SIZE ) );

            // * comment copied from old StoreCopyClient *
            // since we just create new log and put checkpoint into it with offset equals to
            // LOG_HEADER_SIZE we need to update last transaction offset to be equal to this newly defined max
            // offset otherwise next checkpoint that use last transaction offset will be created for non
            // existing offset that is in most of the cases bigger than new log size.
            // Recovery will treat that as last checkpoint and will not try to recover store till new
            // last closed transaction offset will not overcome old one. Till that happens it will be
            // impossible for recovery process to restore the store
            File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
            MetaDataStore.setRecord(
                    pageCache,
                    neoStore,
                    MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET,
                    LOG_HEADER_SIZE );
        }

        lifespan.close();

        if ( lastTxId != -1 )
        {
            File neoStoreFile = new File( storeDir, MetaDataStore.DEFAULT_NAME );
            MetaDataStore.setRecord( pageCache, neoStoreFile, LAST_TRANSACTION_ID, lastTxId );
        }
    }
}
