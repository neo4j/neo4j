/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.com.Response;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.nioneo.store.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.xaframework.CommandWriter;
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntryWriterv1;
import org.neo4j.kernel.impl.transaction.xaframework.LogFile;
import org.neo4j.kernel.impl.transaction.xaframework.LogRotationControl;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.xaframework.ReadOnlyLogVersionRepository;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.WritableLogChannel;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.ConsoleLogger;

import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies.NO_PRUNING;

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
    public static final String TEMP_COPY_DIRECTORY_NAME = "temp-copy";
    private final Config config;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensions;
    private final ConsoleLogger console;
    private final FileSystemAbstraction fs;

    /**
     * This is built as a pluggable interface to allow backup and HA to use this code independently of each other,
     * each implements it's own version of how to copy a store from a remote location.
     */
    public interface StoreCopyRequester
    {
        Response<?> copyStore( StoreWriter writer ) throws IOException;

        void done();
    }

    public StoreCopyClient( Config config, Iterable<KernelExtensionFactory<?>> kernelExtensions,
            ConsoleLogger console, FileSystemAbstraction fs )
    {
        this.config = config;
        this.kernelExtensions = kernelExtensions;
        this.console = console;
        this.fs = fs;
    }

    public void copyStore( StoreCopyRequester requester ) throws IOException
    {
        // Clear up the current temp directory if there
        File storeDir = config.get( InternalAbstractGraphDatabase.Configuration.store_dir );
        File tempStore = new File( storeDir, TEMP_COPY_DIRECTORY_NAME );
        cleanDirectory( tempStore );

        // Request store files and transactions that will need recovery
        try ( Response response = requester.copyStore( decorateWithProgressIndicator(
                new ToFileStoreWriter( tempStore ) ) ) )
        {
            // Update highest archived log id
            // Write transactions that happened during the copy to the currently active logical log
            writeTransactionsToActiveLogFile( tempStore, response.getTxs() );
        }
        finally
        {
            requester.done();
        }

        // Run recovery, so that the transactions we just wrote into the active log will be applied.
        newTempDatabase( tempStore ).shutdown();

        // All is well, move the streamed files to the real store directory
        for ( File candidate : tempStore.listFiles( STORE_FILE_FILTER ) )
        {
            FileUtils.moveFileToDirectory( candidate, storeDir );
        }
    }

    private void writeTransactionsToActiveLogFile( File storeDir,
            Iterable<CommittedTransactionRepresentation> txs ) throws IOException
    {
        LifeSupport life = new LifeSupport();
        try
        {
            // Start the log and appender
            PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, fs );
            TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 10, 100 );
            LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, Long.MAX_VALUE /*don't rotate*/,
                    NO_PRUNING, new ReadOnlyTransactionIdStore( fs, storeDir ), new ReadOnlyLogVersionRepository( fs,
                            storeDir ), PhysicalLogFile.NO_MONITOR, LogRotationControl.NO_ROTATION_CONTROL,
                    transactionMetadataCache, new NoRecoveryAssertingVisitor() ) );
            life.start();

            // Just write all transactions to the active log version. Remember that this is after a store copy
            // where there are no logs, and the transaction stream we're about to write will probably contain
            // transactions that goes some time back, before the last committed transaction id. So we cannot
            // use a TransactionAppender, since it has checks for which transactions one can append.
            WritableLogChannel channel = logFile.getWriter();
            TransactionLogWriter writer = new TransactionLogWriter(
                    new LogEntryWriterv1( channel, new CommandWriter( channel ) ) );
            for ( CommittedTransactionRepresentation transaction : txs )
            {
                writer.append( transaction.getTransactionRepresentation(), transaction.getCommitEntry().getTxId() );
            }
        }
        finally
        {
            life.shutdown();
        }
    }

    private GraphDatabaseService newTempDatabase( File tempStore )
    {
        return new GraphDatabaseFactory()
                .setKernelExtensions( kernelExtensions )
                .newEmbeddedDatabaseBuilder( tempStore.getAbsolutePath() )
                .setConfig(
                        GraphDatabaseSettings.keep_logical_logs, Settings.TRUE ).setConfig(
                        GraphDatabaseSettings.allow_store_upgrade,
                        config.get( GraphDatabaseSettings.allow_store_upgrade ).toString() )

                .newGraphDatabase();
    }

    private StoreWriter decorateWithProgressIndicator( final StoreWriter actual )
    {
        return new StoreWriter()
        {
            private int totalFiles;

            @Override
            public int write( String path, ReadableByteChannel data, ByteBuffer temporaryBuffer,
                              boolean hasData ) throws IOException
            {
                console.log( "Copying " + path );
                int written = actual.write( path, data, temporaryBuffer, hasData );
                console.log( "Copied  " + path + " " + bytes( written ) );
                totalFiles++;
                return written;
            }

            @Override
            public void close()
            {
                actual.close();
                console.log( "Done, copied " + totalFiles + " files");
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

    private static final FileFilter STORE_FILE_FILTER = new FileFilter()
    {
        @Override
        public boolean accept( File file )
        {
            // Skip log files and tx files from temporary database
            return !file.getName().startsWith( "metrics" )
                    && !file.getName().equals( StringLogger.DEFAULT_NAME )
                    && !("active_tx_log tm_tx_log.1 tm_tx_log.2").contains( file.getName() );
        }
    };

    public static class NoRecoveryAssertingVisitor implements Visitor<ReadableLogChannel, IOException>
    {
        @Override
        public boolean visit( ReadableLogChannel element ) throws IOException
        {
            throw new UnsupportedOperationException( "There should not be any recovery needed here" );
        }
    }

    public static class TxIdGeneratorPreventor implements TxIdGenerator
    {
        @Override
        public long generate( TransactionRepresentation transaction )
        {
            throw new UnsupportedOperationException( "There should be no need generating transaction ids here" );
        }
    }
}
