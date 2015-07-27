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
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.com.Response;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyLogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import static java.lang.Math.max;

import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
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
    public interface Monitor
    {
        void startReceivingStoreFiles();

        void finishReceivingStoreFiles();

        void startReceivingStoreFile( File file );

        void finishReceivingStoreFile( File file );

        void startReceivingTransactions( long startTxId );

        void finishReceivingTransactions( long endTxId );

        void startRecoveringStore();

        void finishRecoveringStore();

        class Adapter implements Monitor
        {
            @Override
            public void startReceivingStoreFiles()
            {   // empty
            }

            @Override
            public void finishReceivingStoreFiles()
            {   // empty
            }

            @Override
            public void startReceivingStoreFile( File file )
            {   // empty
            }

            @Override
            public void finishReceivingStoreFile( File file )
            {   // empty
            }

            @Override
            public void startReceivingTransactions( long startTxId )
            {   // empty
            }

            @Override
            public void finishReceivingTransactions( long endTxId )
            {   // empty
            }

            @Override
            public void startRecoveringStore()
            {   // empty
            }

            @Override
            public void finishRecoveringStore()
            {   // empty
            }
        }
    }

    /**
     * This is built as a pluggable interface to allow backup and HA to use this code independently of each other,
     * each implements it's own version of how to copy a store from a remote location.
     */
    public interface StoreCopyRequester
    {
        Response<?> copyStore( StoreWriter writer ) throws IOException;

        void done();
    }

    public static final String TEMP_COPY_DIRECTORY_NAME = "temp-copy";
    private static final FileFilter STORE_FILE_FILTER = new FileFilter()
    {
        @Override
        public boolean accept( File file )
        {
            // Skip log files and tx files from temporary database
            return !file.getName().startsWith( "metrics" )
                   && !file.getName().startsWith( "messages." );
        }
    };
    private final Config config;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensions;
    private final ConsoleLogger console;
    private final Logging logging;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final Monitor monitor;

    public StoreCopyClient( Config config, Iterable<KernelExtensionFactory<?>> kernelExtensions,
                            ConsoleLogger console, Logging logging, FileSystemAbstraction fs,
                            PageCache pageCache, Monitor monitor )
    {
        this.config = config;
        this.kernelExtensions = kernelExtensions;
        this.console = console;
        this.logging = logging;
        this.fs = fs;
        this.pageCache = pageCache;
        this.monitor = monitor;
    }

    public void copyStore( StoreCopyRequester requester, CancellationRequest cancellationRequest )
            throws IOException
    {
        // Clear up the current temp directory if there
        File storeDir = config.get( InternalAbstractGraphDatabase.Configuration.store_dir );
        File tempStore = new File( storeDir, TEMP_COPY_DIRECTORY_NAME );
        cleanDirectory( tempStore );

        // Request store files and transactions that will need recovery
        monitor.startReceivingStoreFiles();
        try ( Response<?> response = requester.copyStore( decorateWithProgressIndicator(
                new ToFileStoreWriter( tempStore, monitor ) ) ) )
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
        monitor.startRecoveringStore();
        GraphDatabaseService graphDatabaseService = newTempDatabase( tempStore );
        graphDatabaseService.shutdown();
        monitor.finishRecoveringStore();

        // All is well, move the streamed files to the real store directory
        for ( File candidate : tempStore.listFiles( STORE_FILE_FILTER ) )
        {
            FileUtils.moveFileToDirectory( candidate, storeDir );
        }
    }

    private void writeTransactionsToActiveLogFile( File storeDir, Response<?> response ) throws IOException
    {
        LifeSupport life = new LifeSupport();
        try
        {
            // Start the log and appender
            PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, fs );
            TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 10, 100 );
            ReadOnlyLogVersionRepository logVersionRepository = new ReadOnlyLogVersionRepository( fs, storeDir );
            LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, Long.MAX_VALUE /*don't rotate*/,
                    new ReadOnlyTransactionIdStore( fs, storeDir ), logVersionRepository,
                    new Monitors().newMonitor( PhysicalLogFile.Monitor.class ),
                    transactionMetadataCache ) );
            life.start();

            // Just write all transactions to the active log version. Remember that this is after a store copy
            // where there are no logs, and the transaction stream we're about to write will probably contain
            // transactions that goes some time back, before the last committed transaction id. So we cannot
            // use a TransactionAppender, since it has checks for which transactions one can append.
            WritableLogChannel channel = logFile.getWriter();
            final TransactionLogWriter writer = new TransactionLogWriter(
                    new LogEntryWriter( channel, new CommandWriter( channel ) ) );
            final AtomicLong firstTxId = new AtomicLong( BASE_TX_ID );

            response.accept( new Response.Handler()
            {
                @Override
                public void obligation( long txId ) throws IOException
                {
                    throw new UnsupportedOperationException( "Shouldn't be called" );
                }

                @Override
                public Visitor<CommittedTransactionRepresentation,IOException> transactions()
                {
                    return new Visitor<CommittedTransactionRepresentation,IOException>()
                    {
                        @Override
                        public boolean visit( CommittedTransactionRepresentation transaction ) throws IOException
                        {
                            long txId = transaction.getCommitEntry().getTxId();
                            if ( firstTxId.compareAndSet( BASE_TX_ID, txId ) )
                            {
                                monitor.startReceivingTransactions( txId );
                            }
                            writer.append( transaction.getTransactionRepresentation(), txId );
                            return false;
                        }
                    };
                }
            } );

            long endTxId = firstTxId.get();
            if ( endTxId != BASE_TX_ID )
            {
                monitor.finishReceivingTransactions( endTxId );
            }

            // And since we write this manually we need to set the correct transaction id in the
            // header of the log that we just wrote.
            writeLogHeader( fs,
                    logFiles.getLogFileForVersion( logVersionRepository.getCurrentLogVersion() ),
                    logVersionRepository.getCurrentLogVersion(), max( BASE_TX_ID, endTxId-1 ) );
        }
        finally
        {
            life.shutdown();
        }
    }

    private GraphDatabaseService newTempDatabase( File tempStore )
    {
        GraphDatabaseFactory factory = ExternallyManagedPageCache.graphDatabaseFactoryWithPageCache( pageCache );
        return factory
                .setLogging( logging )
                .setKernelExtensions( kernelExtensions )
                .newEmbeddedDatabaseBuilder( tempStore.getAbsolutePath() )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade,
                        config.get( GraphDatabaseSettings.allow_store_upgrade ).toString() )
                .setConfig( InternalAbstractGraphDatabase.Configuration.log_configuration_file, logConfigFileName() )
                .newGraphDatabase();
    }

    String logConfigFileName()
    {
        return "neo4j-backup-logback.xml";
    }

    private StoreWriter decorateWithProgressIndicator( final StoreWriter actual )
    {
        return new StoreWriter()
        {
            private int totalFiles;

            @Override
            public long write( String path, ReadableByteChannel data, ByteBuffer temporaryBuffer,
                              boolean hasData ) throws IOException
            {
                console.log( "Copying " + path );
                long written = actual.write( path, data, temporaryBuffer, hasData );
                console.log( "Copied  " + path + " " + bytes( written ) );
                totalFiles++;
                return written;
            }

            @Override
            public void close()
            {
                actual.close();
                console.log( "Done, copied " + totalFiles + " files" );
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
            cleanDirectory( tempStore );
        }
    }
}
