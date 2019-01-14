/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.com.Response;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static java.lang.Math.max;
import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;

/**
 * Client-side store copier. Deals with issuing a request to a source of a database, which will
 * reply with a {@link Response} containing the store files and transactions happening while streaming
 * all the files. After the store files have been streamed, the transactions will be applied so that
 * the store will end up in a consistent state.
 *
 * @see StoreCopyServer
 */
public class StoreCopyClient
{

    /**
     * This is built as a pluggable interface to allow backup and HA to use this code independently of each other,
     * each implements it's own version of how to copy a store from a remote location.
     */
    public interface StoreCopyRequester
    {
        Response<?> copyStore( StoreWriter writer );

        void done();
    }

    private final File storeDir;
    private final Config config;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensions;
    private final Log log;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final StoreCopyClientMonitor monitor;
    private final boolean forensics;
    private final FileMoveProvider fileMoveProvider;

    public StoreCopyClient( File storeDir, Config config, Iterable<KernelExtensionFactory<?>> kernelExtensions, LogProvider logProvider,
            FileSystemAbstraction fs, PageCache pageCache, StoreCopyClientMonitor monitor, boolean forensics )
    {
        this( storeDir, config, kernelExtensions, logProvider, fs, pageCache, monitor, forensics, new FileMoveProvider( pageCache,
                fs ) );
    }

    public StoreCopyClient( File storeDir, Config config, Iterable<KernelExtensionFactory<?>> kernelExtensions, LogProvider logProvider,
            FileSystemAbstraction fs, PageCache pageCache, StoreCopyClientMonitor monitor, boolean forensics, FileMoveProvider fileMoveProvider )
    {
        this.storeDir = storeDir;
        this.config = config;
        this.kernelExtensions = kernelExtensions;
        this.log = logProvider.getLog( getClass() );
        this.fs = fs;
        this.pageCache = pageCache;
        this.monitor = monitor;
        this.forensics = forensics;
        this.fileMoveProvider = fileMoveProvider;
    }

    public void copyStore( StoreCopyRequester requester, CancellationRequest cancellationRequest, MoveAfterCopy moveAfterCopy ) throws Exception
    {
        // Create a temp directory (or clean if present)
        File tempStore = new File( storeDir, StoreUtil.TEMP_COPY_DIRECTORY_NAME );
        try
        {
            // The ToFileStoreWriter will add FileMoveActions for *RecordStores* that have to be
            // *moved via the PageCache*!
            // We have to move these files via the page cache, because that is the *only way* that we can communicate
            // with any block storage that might have been configured for this instance.
            List<FileMoveAction> moveActions = new ArrayList<>();
            cleanDirectory( tempStore );

            // Request store files and transactions that will need recovery
            monitor.startReceivingStoreFiles();
            ToFileStoreWriter storeWriter = new ToFileStoreWriter( tempStore, fs, monitor, pageCache, moveActions );
            try ( Response<?> response = requester.copyStore( decorateWithProgressIndicator( storeWriter ) ) )
            {
                monitor.finishReceivingStoreFiles();
                // Update highest archived log id
                // Write transactions that happened during the copy to the currently active logical log
                writeTransactionsToActiveLogFile( tempStore, response );
            }
            finally
            {
                requester.done();
            }

            // This is a good place to check if the switch has been cancelled
            checkCancellation( cancellationRequest, tempStore );

            // Run recovery, so that the transactions we just wrote into the active log will be applied.
            recoverDatabase( tempStore );

            // All is well, move the streamed files to the real store directory.
            // Start with the files written through the page cache. Should only be record store files.
            // Note that the stream is lazy, so the file system traversal won't happen until *after* the store files
            // have been moved. Thus we ensure that we only attempt to move them once.
            moveFromTemporaryLocationToCorrect( moveActions, tempStore, moveAfterCopy );
        }
        finally
        {
            // All done, delete temp directory
            FileUtils.deleteRecursively( tempStore );
        }
    }

    private void moveFromTemporaryLocationToCorrect(
            List<FileMoveAction> storeFileMoveActions, File tempStore, MoveAfterCopy moveAfterCopy ) throws Exception
    {
        LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( storeDir, fs, pageCache ).withConfig( config ).build();

        Stream<FileMoveAction> moveActionStream =
                Stream.concat( storeFileMoveActions.stream(), fileMoveProvider.traverseForMoving( tempStore ) );
        Function<File,File> destinationMapper =
                file -> logFiles.isLogFile( file ) ? logFiles.logFilesDirectory() : storeDir;
        moveAfterCopy.move( moveActionStream, tempStore, destinationMapper );
    }

    private void recoverDatabase( File tempStore )
    {
        monitor.startRecoveringStore();
        GraphDatabaseService graphDatabaseService = newTempDatabase( tempStore );
        graphDatabaseService.shutdown();
        monitor.finishRecoveringStore();
    }

    private void writeTransactionsToActiveLogFile( File tempStoreDir, Response<?> response ) throws Exception
    {
        LifeSupport life = new LifeSupport();
        try
        {
            // Start the log and appender
            LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( tempStoreDir, fs, pageCache ).build();
            life.add( logFiles );
            life.start();

            // Just write all transactions to the active log version. Remember that this is after a store copy
            // where there are no logs, and the transaction stream we're about to write will probably contain
            // transactions that goes some time back, before the last committed transaction id. So we cannot
            // use a TransactionAppender, since it has checks for which transactions one can append.
            FlushableChannel channel = logFiles.getLogFile().getWriter();
            final TransactionLogWriter writer = new TransactionLogWriter( new LogEntryWriter( channel ) );
            final AtomicLong firstTxId = new AtomicLong( BASE_TX_ID );

            response.accept( new Response.Handler()
            {
                @Override
                public void obligation( long txId )
                {
                    throw new UnsupportedOperationException( "Shouldn't be called" );
                }

                @Override
                public Visitor<CommittedTransactionRepresentation,Exception> transactions()
                {
                    return transaction ->
                    {
                        long txId = transaction.getCommitEntry().getTxId();
                        if ( firstTxId.compareAndSet( BASE_TX_ID, txId ) )
                        {
                            monitor.startReceivingTransactions( txId );
                        }
                        writer.append( transaction.getTransactionRepresentation(), txId );
                        return false;
                    };
                }
            } );

            long endTxId = firstTxId.get();
            if ( endTxId != BASE_TX_ID )
            {
                monitor.finishReceivingTransactions( endTxId );
            }

            long currentLogVersion = logFiles.getHighestLogVersion();
            writer.checkPoint( new LogPosition( currentLogVersion, LOG_HEADER_SIZE ) );

            // And since we write this manually we need to set the correct transaction id in the
            // header of the log that we just wrote.
            File currentLogFile = logFiles.getLogFileForVersion( currentLogVersion );
            writeLogHeader( fs, currentLogFile, currentLogVersion, max( BASE_TX_ID, endTxId - 1 ) );

            if ( !forensics )
            {
                // since we just create new log and put checkpoint into it with offset equals to
                // LOG_HEADER_SIZE we need to update last transaction offset to be equal to this newly defined max
                // offset otherwise next checkpoint that use last transaction offset will be created for non
                // existing offset that is in most of the cases bigger than new log size.
                // Recovery will treat that as last checkpoint and will not try to recover store till new
                // last closed transaction offset will not overcome old one. Till that happens it will be
                // impossible for recovery process to restore the store
                File neoStore = new File( tempStoreDir, MetaDataStore.DEFAULT_NAME );
                MetaDataStore.setRecord( pageCache, neoStore, MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET, LOG_HEADER_SIZE );
            }
        }
        finally
        {
            life.shutdown();
        }
    }

    private GraphDatabaseService newTempDatabase( File tempStore )
    {
        ExternallyManagedPageCache.GraphDatabaseFactoryWithPageCacheFactory factory =
                ExternallyManagedPageCache.graphDatabaseFactoryWithPageCache( pageCache );
        return factory
                .setKernelExtensions( kernelExtensions )
                .setUserLogProvider( NullLogProvider.getInstance() )
                .newEmbeddedDatabaseBuilder( tempStore.getAbsoluteFile() )
                .setConfig( "dbms.backup.enabled", Settings.FALSE )
                .setConfig( GraphDatabaseSettings.pagecache_warmup_enabled, Settings.FALSE )
                .setConfig( GraphDatabaseSettings.logs_directory, tempStore.getAbsolutePath() )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.logical_logs_location, tempStore.getAbsolutePath() )
                .setConfig( GraphDatabaseSettings.allow_upgrade, config.get( GraphDatabaseSettings.allow_upgrade ).toString() )
                .newGraphDatabase();
    }

    private StoreWriter decorateWithProgressIndicator( final StoreWriter actual )
    {
        return new StoreWriter()
        {
            private int totalFiles;

            @Override
            public long write( String path, ReadableByteChannel data, ByteBuffer temporaryBuffer,
                    boolean hasData, int requiredElementAlignment ) throws IOException
            {
                log.info( "Copying %s", path );
                long written = actual.write( path, data, temporaryBuffer, hasData, requiredElementAlignment );
                log.info( "Copied %s %s", path, bytes( written ) );
                totalFiles++;
                return written;
            }

            @Override
            public void close()
            {
                actual.close();
                log.info( "Done, copied %s files", totalFiles );
            }
        };
    }

    private void cleanDirectory( File directory ) throws IOException
    {
        if ( !directory.mkdir() )
        {
            FileUtils.deleteRecursively( directory );
            directory.mkdir();
        }
    }

    private void checkCancellation( CancellationRequest cancellationRequest, File tempStore ) throws IOException
    {
        if ( cancellationRequest.cancellationRequested() )
        {
            log.info( "Store copying was cancelled. Cleaning up temp-directories." );
            cleanDirectory( tempStore );
        }
    }
}
