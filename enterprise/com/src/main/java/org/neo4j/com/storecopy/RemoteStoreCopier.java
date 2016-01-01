/**
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
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TxExtractor;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.StoreCopyMonitor;

import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.kernel.InternalAbstractGraphDatabase.Configuration;
import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;

public class RemoteStoreCopier
{
    private static final String COPY_FROM_MASTER_TEMP = "temp-copy";

    private final Config config;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensions;
    private final ConsoleLogger console;
    private final Logging logging;
    private final FileSystemAbstraction fs;
    private StoreCopyMonitor storeCopyMonitor;

    /**
     * This is built as a pluggable interface to allow backup and HA to use this code independently of each other,
     * each implements it's own version of how to copy a store from a remote location.
     */
    public interface StoreCopyRequester
    {
        Response<?> copyStore( StoreWriter writer );

        void done();
    }

    public RemoteStoreCopier( Config config, Iterable<KernelExtensionFactory<?>> kernelExtensions,
                              ConsoleLogger console, Logging logging, FileSystemAbstraction fs, Monitors monitors  )
    {
        this.config = config;
        this.kernelExtensions = kernelExtensions;
        this.console = console;
        this.logging = logging;
        this.fs = fs;
        this.storeCopyMonitor = monitors.newMonitor( StoreCopyMonitor.class, getClass() );
    }

    public void copyStore( StoreCopyRequester requester, CancellationRequest cancellationRequest ) throws IOException
    {
        // Clear up the current temp directory if there
        File storeDir = config.get( Configuration.store_dir );
        File tempStore = new File( storeDir, COPY_FROM_MASTER_TEMP );
        Config tempConfig = configForTempStore( tempStore );
        clearTempDirectory( tempStore );

        // Request store files and transactions that will need recovery
        StoreWriter storeWriter = decorateWithProgressIndicator( new ToFileStoreWriter( tempStore ) );
        try ( Response<?> response = requester.copyStore( storeWriter ) )
        {
            // Update highest archived log id
            long highestLogVersion = XaLogicalLog.getHighestHistoryLogVersion( fs, tempStore, LOGICAL_LOG_DEFAULT_NAME );
            if ( highestLogVersion > -1 )
            {
                NeoStore.setVersion( fs, new File( tempStore, NeoStore.DEFAULT_NAME ), highestLogVersion + 1 );
            }

            // Write pending transactions down to the currently active logical log
            writeTransactionsToActiveLogFile( tempConfig, response.transactions() );
        }
        finally
        {
            requester.done();
        }
        storeCopyMonitor.finishedCopyingStoreFiles();

        // This is a good place to check if the switch has been cancelled
        checkCancellation( cancellationRequest, tempStore );

        // Run recovery
        GraphDatabaseAPI copiedDb = newTempDatabase( tempStore );
        copiedDb.shutdown();
        storeCopyMonitor.recoveredStore();

        // All is well, move to the real store directory
        for ( File candidate : tempStore.listFiles( new FileFilter()
        {
            @Override
            public boolean accept( File file )
            {
                // Skip log files and tx files from temporary database
                return !file.getName().startsWith( "metrics" )
                        && !file.getName().equals( StringLogger.DEFAULT_NAME )
                        && !("active_tx_log tm_tx_log.1 tm_tx_log.2").contains( file.getName() );
            }
        } ) )
        {
            FileUtils.moveFileToDirectory( candidate, storeDir );
        }
    }
    
    private void checkCancellation( CancellationRequest cancellationRequest, File tempStore ) throws IOException
    {
        if ( cancellationRequest.cancellationRequested() )
        {
            clearTempDirectory( tempStore );
        }
    }
    
    private void clearTempDirectory( File tempStore ) throws IOException
    {
        if ( !tempStore.mkdir() )
        {
            FileUtils.deleteRecursively( tempStore );
            tempStore.mkdir();
        }
    }

    private Config configForTempStore( File tempStore )
    {
        Map<String, String> params = config.getParams();
        params.put( Configuration.store_dir.name(), tempStore.getAbsolutePath() );
        return new Config( params );
    }

    private void writeTransactionsToActiveLogFile( Config tempConfig, TransactionStream transactions ) throws IOException
    {
        Map</*dsName*/String, LogBufferFactory> logWriters = createLogWriters( tempConfig );
        Map</*dsName*/String, LogBuffer> logFiles = new HashMap<>();
        try
        {
            while ( transactions.hasNext() )
            {
                Triplet<String, Long, TxExtractor> next = transactions.next();
                String dsName = next.first();
                Long txId = next.second();
                LogBuffer log = getOrCreateLogBuffer( logFiles, logWriters, dsName, txId, tempConfig );
                next.third().extract( log );
            }
        }
        finally
        {
            for ( LogBuffer buf : logFiles.values() )
            {
                buf.force();
                buf.getFileChannel().close();
            }
        }
    }

    private static LogBuffer getOrCreateLogBuffer( Map<String, LogBuffer> buffers,
                                                   Map<String, LogBufferFactory> logWriters,
                                                   String dsName, Long txId, Config config ) throws IOException
    {
        LogBuffer buffer = buffers.get( dsName );
        if ( buffer == null )
        {
            if ( logWriters.containsKey( dsName ) )
            {
                buffer = logWriters.get( dsName ).createActiveLogFile( config, txId - 1 );
                buffers.put( dsName, buffer );
            }
            else
            {
                throw new IllegalStateException( "Got transaction for unknown data source, unable to safely copy " +
                        "files. Offending data source was '" + dsName + "', please make sure this data source is " +
                        "available on the classpath." );
            }
        }
        return buffer;
    }

    private Map<String, LogBufferFactory> createLogWriters( Config config ) throws IOException
    {
        Map<String, LogBufferFactory> writers = new HashMap<>();
        File tempStore = new File( config.get( GraphDatabaseSettings.store_dir ).getAbsolutePath() + ".tmp" );
        GraphDatabaseAPI db = newTempDatabase( tempStore );
        try
        {
            XaDataSourceManager dsManager = db.getDependencyResolver().resolveDependency( XaDataSourceManager.class );
            for ( XaDataSource xaDataSource : dsManager.getAllRegisteredDataSources() )
            {
                writers.put( xaDataSource.getName(), xaDataSource.createLogBufferFactory() );
            }
            return writers;
        }
        finally
        {
            db.shutdown();
            FileUtils.deleteRecursively( tempStore );
            tempStore.mkdirs();
        }
    }

    private GraphDatabaseAPI newTempDatabase( File tempStore )
    {
        return (GraphDatabaseAPI) new GraphDatabaseFactory()
                .setLogging( logging )
                .setKernelExtensions( kernelExtensions )
                .newEmbeddedDatabaseBuilder( tempStore.getAbsolutePath() )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.TRUE )
                .setConfig( InternalAbstractGraphDatabase.Configuration.log_configuration_file, logConfigFileName() )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade,
                        config.get( GraphDatabaseSettings.allow_store_upgrade ).toString() )

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
            public void done()
            {
                actual.done();
                console.log( "Done, copied " + totalFiles + " files" );
            }
        };
    }
}
