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
package org.neo4j.backup.impl;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.neo4j.backup.IncrementalBackupNotPossibleException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.ExternallyManagedPageCache;
import org.neo4j.com.storecopy.MoveAfterCopy;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.com.storecopy.ResponseUnpacker.TxHandler;
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.com.storecopy.StoreCopyClientMonitor;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.UnexpectedStoreVersionException;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.impl.transaction.log.MissingLogDataException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.storageengine.api.StoreId;

import static org.neo4j.com.RequestContext.anonymous;
import static org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker.DEFAULT_BATCH_SIZE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logs_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.helpers.Exceptions.rootCause;

/**
 * Client-side convenience service for doing backups from a running database instance.
 */
public class BackupProtocolService implements AutoCloseable
{
    static final String TOO_OLD_BACKUP = "It's been too long since this backup was last updated, and it has " +
            "fallen too far behind the database transaction stream for incremental backup to be possible. You need to" +
            " perform a full backup at this point. You can modify this time interval by setting the '" +
            GraphDatabaseSettings.keep_logical_logs.name() + "' configuration on the database to a higher value.";

    static final String DIFFERENT_STORE_MESSAGE = "Target directory contains full backup of a logically different store.";

    private final Supplier<FileSystemAbstraction> fileSystemSupplier;
    private final LogProvider logProvider;
    private final Log log;
    private final OutputStream logDestination;
    private final Monitors monitors;
    private final BackupPageCacheContainer pageCacheContianer;

    BackupProtocolService( Supplier<FileSystemAbstraction> fileSystemSupplier, LogProvider logProvider, OutputStream logDestination, Monitors monitors,
            BackupPageCacheContainer pageCacheContainer )
    {
        this.fileSystemSupplier = fileSystemSupplier;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.logDestination = logDestination;
        this.monitors = monitors;
        this.pageCacheContianer = pageCacheContainer;
    }

    public BackupOutcome doFullBackup( final String sourceHostNameOrIp, final int sourcePort, DatabaseLayout targetLayout,
                                       ConsistencyCheck consistencyCheck, Config tuningConfiguration,
                                       final long timeout, final boolean forensics )
    {
        try ( FileSystemAbstraction fileSystem = fileSystemSupplier.get() )
        {
            return fullBackup( fileSystem, sourceHostNameOrIp, sourcePort, targetLayout, consistencyCheck,
                    tuningConfiguration, timeout, forensics );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private BackupOutcome fullBackup( FileSystemAbstraction fileSystem, String sourceHostNameOrIp, int sourcePort,
            DatabaseLayout targetLayout, ConsistencyCheck consistencyCheck, Config tuningConfiguration, long timeout, boolean forensics )
    {
        try
        {
            if ( !directoryIsEmpty( targetLayout ) )
            {
                throw new RuntimeException(
                        "Can only perform a full backup into an empty directory but " + targetLayout +
                        " is not empty" );
            }
            long timestamp = System.currentTimeMillis();
            long lastCommittedTx = -1;
            StoreCopyClient storeCopier = new StoreCopyClient( targetLayout, tuningConfiguration,
                    loadKernelExtensions(), logProvider, fileSystem, pageCacheContianer.getPageCache(),
                    monitors.newMonitor( StoreCopyClientMonitor.class, getClass().getName() ), forensics );
            FullBackupStoreCopyRequester storeCopyRequester =
                    new FullBackupStoreCopyRequester( sourceHostNameOrIp, sourcePort, timeout, forensics, monitors );
            storeCopier.copyStore(
                    storeCopyRequester,
                    CancellationRequest.NEVER_CANCELLED,
                    MoveAfterCopy.moveReplaceExisting() );

            tuningConfiguration.augment( logs_directory, targetLayout.databaseDirectory().toPath().toRealPath().toString() );
            File debugLogFile = tuningConfiguration.get( store_internal_log_path );
            bumpDebugDotLogFileVersion( debugLogFile, timestamp );
            boolean consistent = checkDbConsistency( fileSystem, targetLayout, consistencyCheck, tuningConfiguration, pageCacheContianer.getPageCache() );
            clearIdFiles( fileSystem, targetLayout );
            return new BackupOutcome( lastCommittedTx, consistent );
        }
        catch ( RuntimeException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    public BackupOutcome doIncrementalBackup(
            String sourceHostNameOrIp, int sourcePort, DatabaseLayout databaseLayout, ConsistencyCheck consistencyCheck,
            long timeout, Config config )
            throws IncrementalBackupNotPossibleException
    {
        try ( FileSystemAbstraction fileSystem = fileSystemSupplier.get() )
        {
            return incrementalBackup( fileSystem, sourceHostNameOrIp, sourcePort, databaseLayout, consistencyCheck,
                    timeout, config );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private BackupOutcome incrementalBackup( FileSystemAbstraction fileSystem, String sourceHostNameOrIp,
            int sourcePort, DatabaseLayout targetLayout, ConsistencyCheck consistencyCheck, long timeout, Config config )
    {
        try
        {
            if ( !directoryContainsDb( targetLayout ) )
            {
                throw new RuntimeException( targetLayout + " doesn't contain a database" );
            }

            Map<String,String> temporaryDbConfig = getTemporaryDbConfig();
            config.augment( temporaryDbConfig );

            Map<String,String> configParams = config.getRaw();
            GraphDatabaseAPI targetDb = startTemporaryDb( targetLayout.databaseDirectory(), pageCacheContianer.getPageCache(), configParams );
            long backupStartTime = System.currentTimeMillis();
            long lastCommittedTx;
            try
            {
                lastCommittedTx = incrementalWithContext( sourceHostNameOrIp, sourcePort, targetDb, timeout, slaveContextOf( targetDb ) );
            }
            finally
            {
                targetDb.shutdown();
                // as soon as recovery will be extracted we will not gonna need this
                File lockFile = targetLayout.getStoreLayout().storeLockFile();
                if ( lockFile.exists() )
                {
                    FileUtils.deleteFile( lockFile );
                }
            }
            config.augment( logs_directory, targetLayout.databaseDirectory().getCanonicalPath() );
            File debugLogFile = config.get( store_internal_log_path );
            bumpDebugDotLogFileVersion( debugLogFile, backupStartTime );
            boolean consistent = checkDbConsistency( fileSystem, targetLayout, consistencyCheck, config, pageCacheContianer.getPageCache() );
            clearIdFiles( fileSystem, targetLayout );
            return new BackupOutcome( lastCommittedTx, consistent );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private boolean checkDbConsistency( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout,
            ConsistencyCheck consistencyCheck, Config tuningConfiguration, PageCache pageCache )
    {
        boolean consistent = false;
        try
        {
            consistent = consistencyCheck.runFull( databaseLayout, tuningConfiguration,
                    ProgressMonitorFactory.textual( logDestination ), logProvider, fileSystem, pageCache, false,
                            new ConsistencyFlags( tuningConfiguration ) );
        }
        catch ( ConsistencyCheckFailedException e )
        {
            log.error( "Consistency check incomplete", e );
        }
        return consistent;
    }

    private static Map<String,String> getTemporaryDbConfig()
    {
        Map<String,String> tempDbConfig = new HashMap<>();
        tempDbConfig.put( OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE );
        // In case someone deleted the logical log from a full backup
        tempDbConfig.put( GraphDatabaseSettings.keep_logical_logs.name(), Settings.TRUE );
        tempDbConfig.put( GraphDatabaseSettings.pagecache_warmup_enabled.name(), Settings.FALSE );
        return tempDbConfig;
    }

    public BackupOutcome doIncrementalBackupOrFallbackToFull( String sourceHostNameOrIp, int sourcePort,
                                                              DatabaseLayout targetLayout,
                                                              ConsistencyCheck consistencyCheck, Config config,
                                                              long timeout, boolean forensics )
    {
        try ( FileSystemAbstraction fileSystem = fileSystemSupplier.get() )
        {
            if ( directoryIsEmpty( targetLayout ) )
            {
                log.info( "Previous backup not found, a new full backup will be performed." );
                return fullBackup( fileSystem, sourceHostNameOrIp, sourcePort, targetLayout, consistencyCheck,
                        config, timeout, forensics );
            }
            try
            {
                log.info( "Previous backup found, trying incremental backup." );
                return incrementalBackup( fileSystem, sourceHostNameOrIp, sourcePort, targetLayout,
                        consistencyCheck, timeout, config );
            }
            catch ( IncrementalBackupNotPossibleException e )
            {
                try
                {
                    log.warn( "Attempt to do incremental backup failed.", e );
                    log.info( "Existing backup is too far out of date, a new full backup will be performed." );
                    FileUtils.deletePathRecursively( targetLayout.databaseDirectory().toPath() );
                    return fullBackup( fileSystem, sourceHostNameOrIp, sourcePort, targetLayout , consistencyCheck,
                            config, timeout, forensics );
                }
                catch ( Exception fullBackupFailure )
                {
                    RuntimeException exception = new RuntimeException(
                            "Failed to perform incremental backup, fell back to full backup, but that failed as " +
                            "well: '" + fullBackupFailure.getMessage() + "'.", fullBackupFailure );
                    exception.addSuppressed( e );
                    throw exception;
                }
            }
        }
        catch ( RuntimeException e )
        {
            if ( rootCause( e ) instanceof UpgradeNotAllowedByConfigurationException )
            {
                throw new UnexpectedStoreVersionException(
                        "Failed to perform backup because existing backup is from a different version.", e );
            }

            throw e;
        }
        catch ( IOException io )
        {
            throw new RuntimeException( io );
        }
    }

    public static BackupOutcome doIncrementalBackup( String sourceHostNameOrIp, int sourcePort, GraphDatabaseAPI targetDb, long timeout )
            throws IncrementalBackupNotPossibleException
    {
        long lastCommittedTransaction = incrementalWithContext( sourceHostNameOrIp, sourcePort, targetDb, timeout, slaveContextOf( targetDb ) );
        return new BackupOutcome( lastCommittedTransaction, true );
    }

    private static RequestContext slaveContextOf( GraphDatabaseAPI graphDb )
    {
        TransactionIdStore transactionIdStore = graphDb.getDependencyResolver().resolveDependency(
                TransactionIdStore.class );
        return anonymous( transactionIdStore.getLastCommittedTransactionId() );
    }

    private static boolean directoryContainsDb( DatabaseLayout databaseLayout )
    {
        return Files.isRegularFile( databaseLayout.metadataStore().toPath() );
    }

    private static boolean directoryIsEmpty( DatabaseLayout databaseLayout ) throws IOException
    {
        Path path = databaseLayout.databaseDirectory().toPath();
        return Files.notExists( path ) || Files.isDirectory( path ) && FileUtils.countFilesInDirectoryPath( path ) == 0;
    }

    static GraphDatabaseAPI startTemporaryDb(
            File storeDir, PageCache pageCache, Map<String,String> config )
    {
        GraphDatabaseFactory factory = ExternallyManagedPageCache.graphDatabaseFactoryWithPageCache( pageCache );
        return (GraphDatabaseAPI) factory.newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( config )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
    }

    /**
     * Performs an incremental backup based off the given context. This means
     * receiving and applying selectively (i.e. irrespective of the actual state
     * of the target db) a set of transactions starting at the desired txId and
     * spanning up to the latest of the master
     *
     * @param targetDb The database that contains a previous full copy
     * @param context The context, containing transaction id to start streaming transaction from
     * @return last committed transaction id
     */
    private static long incrementalWithContext( String sourceHostNameOrIp, int sourcePort, GraphDatabaseAPI targetDb, long timeout, RequestContext context )
            throws IncrementalBackupNotPossibleException
    {
        DependencyResolver resolver = targetDb.getDependencyResolver();

        ProgressTxHandler handler = new ProgressTxHandler();
        TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker(
                resolver, DEFAULT_BATCH_SIZE, 0 );

        Monitors monitors = resolver.resolveDependency( Monitors.class );
        LogProvider logProvider = resolver.resolveDependency( LogService.class ).getInternalLogProvider();
        BackupClient client = new BackupClient( sourceHostNameOrIp, sourcePort, null, logProvider, targetDb.storeId(),
                timeout, unpacker, monitors.newMonitor( ByteCounterMonitor.class, BackupClient.class.getName() ),
                monitors.newMonitor( RequestMonitor.class, BackupClient.class.getName() ), new VersionAwareLogEntryReader<>() );

        try ( Lifespan lifespan = new Lifespan( unpacker, client ) )
        {
            try ( Response<Void> response = client.incrementalBackup( context ) )
            {
                unpacker.unpackResponse( response, handler );
            }
        }
        catch ( MismatchingStoreIdException e )
        {
            throw new RuntimeException( DIFFERENT_STORE_MESSAGE, e );
        }
        catch ( RuntimeException | IOException e )
        {
            if ( e.getCause() != null && e.getCause() instanceof MissingLogDataException )
            {
                throw new IncrementalBackupNotPossibleException( TOO_OLD_BACKUP, e.getCause() );
            }
            if ( e.getCause() != null && e.getCause() instanceof ConnectException )
            {
                throw new RuntimeException( e.getMessage(), e.getCause() );
            }
            throw new RuntimeException( "Failed to perform incremental backup.", e );
        }
        catch ( Throwable throwable )
        {
            throw new RuntimeException( "Unexpected error", throwable );
        }

        return handler.getLastSeenTransactionId();
    }

    private static void bumpDebugDotLogFileVersion( final File debugLogFile, final long toTimestamp )
    {
        if ( !debugLogFile.exists() )
        {
            return;
        }
        // Build to, from existing parent + new filename
        File to = new File( debugLogFile.getParentFile(), debugLogFile.getName() + "." + toTimestamp );
        debugLogFile.renameTo( to );
    }

    private static List<KernelExtensionFactory<?>> loadKernelExtensions()
    {
        List<KernelExtensionFactory<?>> kernelExtensions = new ArrayList<>();
        for ( KernelExtensionFactory<?> factory : Service.load( KernelExtensionFactory.class ) )
        {
            kernelExtensions.add( factory );
        }
        return kernelExtensions;
    }

    private static void clearIdFiles( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout ) throws IOException
    {
        for ( File file : fileSystem.listFiles( databaseLayout.databaseDirectory() ) )
        {
            if ( !fileSystem.isDirectory( file ) && file.getName().endsWith( ".id" ) )
            {
                long highId = IdGeneratorImpl.readHighId( fileSystem, file );
                fileSystem.deleteFile( file );
                IdGeneratorImpl.createGenerator( fileSystem, file, highId, true );
            }
        }
    }

    @Override
    public void close() throws Exception
    {
        pageCacheContianer.close();
    }

    private static class ProgressTxHandler implements TxHandler
    {
        private long lastSeenTransactionId;

        @Override
        public void accept( long transactionId )
        {
            lastSeenTransactionId = transactionId;
        }

        long getLastSeenTransactionId()
        {
            return lastSeenTransactionId;
        }
    }

    private static class FullBackupStoreCopyRequester implements StoreCopyClient.StoreCopyRequester
    {
        private final String sourceHostNameOrIp;
        private final int sourcePort;
        private final long timeout;
        private final boolean forensics;
        private final Monitors monitors;

        private BackupClient client;

        private FullBackupStoreCopyRequester( String sourceHostNameOrIp, int sourcePort, long timeout,
                                             boolean forensics, Monitors monitors )
        {
            this.sourceHostNameOrIp = sourceHostNameOrIp;
            this.sourcePort = sourcePort;
            this.timeout = timeout;
            this.forensics = forensics;
            this.monitors = monitors;
        }

        @Override
        public Response<?> copyStore( StoreWriter writer )
        {
            client = new BackupClient( sourceHostNameOrIp, sourcePort, null, NullLogProvider.getInstance(),
                    StoreId.DEFAULT, timeout, ResponseUnpacker.NO_OP_RESPONSE_UNPACKER, monitors.newMonitor(
                    ByteCounterMonitor.class ), monitors.newMonitor( RequestMonitor.class ),
                    new VersionAwareLogEntryReader<>() );
            client.start();
            return client.fullBackup( writer, forensics );
        }

        @Override
        public void done()
        {
            client.stop();
        }
    }
}
