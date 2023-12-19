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
package org.neo4j.causalclustering.catchup.tx;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier.EMPTY;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
import static org.neo4j.kernel.impl.store.StoreType.META_DATA;
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
    private final NeoStores stores;
    private final boolean rotateTransactionsManually;

    private long lastTxId = -1;
    private long expectedTxId;

    TransactionLogCatchUpWriter( File storeDir, FileSystemAbstraction fs, PageCache pageCache, Config config,
            LogProvider logProvider, long fromTxId, boolean asPartOfStoreCopy, boolean keepTxLogsInStoreDir,
            boolean forceTransactionRotations ) throws IOException
    {
        this.pageCache = pageCache;
        this.log = logProvider.getLog( getClass() );
        this.asPartOfStoreCopy = asPartOfStoreCopy;
        this.rotateTransactionsManually = forceTransactionRotations;
        RecordFormats recordFormats = RecordFormatSelector.selectForStoreOrConfig( Config.defaults(), storeDir, pageCache, logProvider );
        this.stores = new StoreFactory( storeDir, config, new DefaultIdGeneratorFactory( fs ), pageCache, fs, recordFormats, logProvider, EMPTY )
                .openNeoStores( META_DATA );
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( stores.getMetaDataStore() );
        LogFilesBuilder logFilesBuilder = LogFilesBuilder
                .builder( storeDir, fs )
                .withDependencies( dependencies )
                .withLastCommittedTransactionIdSupplier( () -> fromTxId - 1 )
                .withConfig( customisedConfig( config, keepTxLogsInStoreDir, forceTransactionRotations ) )
                .withLogVersionRepository( stores.getMetaDataStore() );
        this.logFiles = logFilesBuilder.build();
        this.lifespan.add( logFiles );
        this.writer = new TransactionLogWriter( new LogEntryWriter( logFiles.getLogFile().getWriter() ) );
        this.storeDir = storeDir;
        this.expectedTxId = fromTxId;
    }

    private Config customisedConfig( Config original, boolean keepTxLogsInStoreDir, boolean forceTransactionRotations )
    {
        Config config = Config.builder()
                .build();
        if ( !keepTxLogsInStoreDir )
        {
            original.getRaw( GraphDatabaseSettings.logical_logs_location.name() )
                    .ifPresent( v -> config.augment( GraphDatabaseSettings.logical_logs_location, v ) );
        }
        if ( forceTransactionRotations )
        {
            original.getRaw( GraphDatabaseSettings.logical_log_rotation_threshold.name() )
                    .ifPresent( v -> config.augment( GraphDatabaseSettings.logical_log_rotation_threshold, v ) );
        }
        return config;
    }

    @Override
    public synchronized void onTxReceived( TxPullResponse txPullResponse )
    {
        CommittedTransactionRepresentation tx = txPullResponse.tx();
        long receivedTxId = tx.getCommitEntry().getTxId();

        // neo4j admin backup clients pull transactions indefinitely and have no monitoring mechanism for tx log rotation
        // Other cases, ex. Read Replicas have an external mechanism that rotates independently of this process and don't need to
        // manually rotate while pulling
        if ( rotateTransactionsManually && logFiles.getLogFile().rotationNeeded() )
        {
            rotateTransactionLogs( logFiles );
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

    private static void rotateTransactionLogs( LogFiles logFiles )
    {
        try
        {
            logFiles.getLogFile().rotate();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        if ( asPartOfStoreCopy )
        {
            /* A checkpoint which points to the beginning of all the log files, meaning that
            all the streamed transactions will be applied as part of recovery. */
            long logVersion = logFiles.getLowestLogVersion();
            LogPosition checkPointPosition = new LogPosition( logVersion, LOG_HEADER_SIZE );

            log.info( "Writing checkpoint as part of store copy: " + checkPointPosition );
            writer.checkPoint( checkPointPosition );

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
                    LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET,
                    checkPointPosition.getByteOffset() );
        }

        lifespan.close();

        if ( lastTxId != -1 )
        {
            File neoStoreFile = new File( storeDir, MetaDataStore.DEFAULT_NAME );
            MetaDataStore.setRecord( pageCache, neoStoreFile, LAST_TRANSACTION_ID, lastTxId );
        }
        stores.close();
    }
}
