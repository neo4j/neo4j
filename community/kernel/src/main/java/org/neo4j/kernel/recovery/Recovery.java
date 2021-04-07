/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.recovery;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Optional;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.DatabaseReadOnlyChecker;
import org.neo4j.dbms.database.DatabasePageCache;
import org.neo4j.index.internal.gbptree.GroupingRecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.DefaultIdController;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.context.VersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.database.DefaultForceOperation;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionFailureStrategies;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.storemigration.LegacyTransactionLogsLocator;
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.RecoveryThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.storeview.FullScanStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.IndexStoreViewFactory;
import org.neo4j.kernel.impl.util.monitoring.LogProgressReporter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.monitoring.PanicEventGenerator;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.RecoveryState;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageFilesState;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.time.Clocks;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.helpers.collection.Iterables.stream;
import static org.neo4j.kernel.impl.constraints.ConstraintSemantics.getConstraintSemantics;
import static org.neo4j.kernel.impl.locking.Locks.NO_LOCKS;
import static org.neo4j.kernel.recovery.RecoveryStartupChecker.EMPTY_CHECKER;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;
import static org.neo4j.scheduler.Group.INDEX_CLEANUP;
import static org.neo4j.scheduler.Group.INDEX_CLEANUP_WORK;
import static org.neo4j.storageengine.api.StorageEngineFactory.selectStorageEngine;
import static org.neo4j.time.Clocks.systemClock;
import static org.neo4j.token.api.TokenHolder.TYPE_LABEL;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;
import static org.neo4j.token.api.TokenHolder.TYPE_RELATIONSHIP_TYPE;

/**
 * Utility class to perform store recovery or check is recovery is required.
 * Recovery is required and can/will be performed on database that have at least one transaction in transaction log after last available checkpoint.
 * During recovery all recorded changes from transaction logs will be replayed and in the end checkpoint will be performed.
 * Please note that recovery will not gonna wait for all affected indexes populations to finish.
 */
public final class Recovery
{
    private Recovery()
    {
    }

    /**
     * Provide recovery helper that can perform recovery of some database described by {@link DatabaseLayout}.
     *
     * @param fs database filesystem
     * @param pageCache page cache used to perform database recovery.
     * @param tracers underlying operation tracers
     * @param config custom configuration
     * @return helper recovery checker
     */
    public static RecoveryFacade recoveryFacade( FileSystemAbstraction fs, PageCache pageCache, Tracers tracers, Config config, MemoryTracker memoryTracker )
    {
        return new RecoveryFacade( fs, pageCache, new DatabaseTracers( tracers ), config, memoryTracker );
    }

    /**
     * Check if recovery is required for a store described by provided layout.
     * Custom root location for transaction logs can be provided using {@link GraphDatabaseSettings#transaction_logs_root_path} config setting value.
     * @param databaseLayout layout of database to check for recovery
     * @param config custom configuration
     * @return true if recovery is required, false otherwise.
     * @throws IOException on any unexpected I/O exception encountered during recovery.
     */
    public static boolean isRecoveryRequired( DatabaseLayout databaseLayout, Config config, MemoryTracker memoryTracker ) throws Exception
    {
        requireNonNull( databaseLayout );
        requireNonNull( config );
        try ( DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction() )
        {
            return isRecoveryRequired( fs, databaseLayout, config, memoryTracker );
        }
    }

    /**
     * Performs recovery of database described by provided layout.
     * Transaction logs should be located in their default location.
     * If recovery is not required nothing will be done to the the database or logs.
     * Note: used by external tools.
     * @param databaseLayout database to recover layout.
     * @param tracers underlying events tracers.
     * @throws IOException on any unexpected I/O exception encountered during recovery.
     */
    public static void performRecovery( DatabaseLayout databaseLayout, DatabaseTracers tracers, MemoryTracker memoryTracker ) throws Exception
    {
        requireNonNull( databaseLayout );
        Config config = defaults();
        try ( DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
                JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
                PageCache pageCache = getPageCache( config, fs, jobScheduler ) )
        {
            performRecovery( fs, pageCache, tracers, config, databaseLayout, memoryTracker );
        }
    }

    /**
     * Check if recovery is required for a store described by provided layout.
     * Custom root location for transaction logs can be provided using {@link GraphDatabaseSettings#transaction_logs_root_path} config setting value.
     *
     * @param fs database filesystem
     * @param databaseLayout layout of database to check for recovery
     * @param config custom configuration
     * @return true if recovery is required, false otherwise.
     * @throws IOException on any unexpected I/O exception encountered during recovery.
     */
    public static boolean isRecoveryRequired( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config,
            MemoryTracker memoryTracker ) throws Exception
    {
        requireNonNull( databaseLayout );
        requireNonNull( config );
        requireNonNull( fs );
        try ( JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
                PageCache pageCache = getPageCache( config, fs, jobScheduler ) )
        {
            StorageEngineFactory storageEngineFactory = StorageEngineFactory.defaultStorageEngine();
            return isRecoveryRequired( fs, pageCache, databaseLayout, storageEngineFactory, config, Optional.empty(), memoryTracker );
        }
    }

    /**
     * Performs recovery of database described by provided layout.
     * <b>Transaction logs should be located in their default location and any provided custom location is ignored.</b>
     * If recovery is not required nothing will be done to the the database or logs.
     *
     * @param fs database filesystem
     * @param pageCache page cache used to perform database recovery.
     * @param config custom configuration
     * @param databaseLayout database to recover layout.
     * @throws IOException on any unexpected I/O exception encountered during recovery.
     */
    public static void performRecovery( FileSystemAbstraction fs, PageCache pageCache, DatabaseTracers tracers,
            Config config, DatabaseLayout databaseLayout, MemoryTracker memoryTracker ) throws IOException
    {
        performRecovery( fs, pageCache, tracers, config, databaseLayout, selectStorageEngine( fs, databaseLayout, pageCache, config ), false, memoryTracker );
    }

    /**
     * Performs recovery of database described by provided layout.
     * <b>Transaction logs should be located in their default location and any provided custom location is ignored.</b>
     * If recovery is not required nothing will be done to the the database or logs.
     *
     * @param fs database filesystem
     * @param pageCache page cache used to perform database recovery.
     * @param tracers underlying operations tracer
     * @param config custom configuration
     * @param databaseLayout database to recover layout.
     * @param storageEngineFactory storage engine factory
     * @param forceRunRecovery to force recovery to run even if the usual checks indicates that it's not required.
     * In specific cases, like after store copy there's always a need for doing a recovery or at least to start the db, checkpoint and shut down,
     * even if the normal "is recovery required" checks says that recovery isn't required.
     * @throws IOException on any unexpected I/O exception encountered during recovery.
     */
    public static void performRecovery( FileSystemAbstraction fs, PageCache pageCache, DatabaseTracers tracers, Config config,
            DatabaseLayout databaseLayout, StorageEngineFactory storageEngineFactory, boolean forceRunRecovery, MemoryTracker memoryTracker ) throws IOException
    {
        requireNonNull( fs );
        requireNonNull( pageCache );
        requireNonNull( config );
        requireNonNull( databaseLayout );
        requireNonNull( storageEngineFactory );
        //remove any custom logical logs location
        Config recoveryConfig = Config.newBuilder().fromConfig( config ).set( GraphDatabaseSettings.transaction_logs_root_path, null ).build();
        performRecovery( fs, pageCache, tracers, recoveryConfig, databaseLayout, StorageEngineFactory.defaultStorageEngine(), forceRunRecovery, NullLogProvider.getInstance(),
                new Monitors(), loadExtensions(), Optional.empty(), EMPTY_CHECKER, memoryTracker, systemClock() );
    }

    /**
     * Performs recovery of database described by provided layout.
     *
     * @param fs database filesystem
     * @param pageCache page cache used to perform database recovery.
     * @param tracers underlying operation tracers
     * @param config custom configuration
     * @param databaseLayout database to recover layout.
     * @param storageEngineFactory {@link StorageEngineFactory} for the storage to recover.
     * @param logProvider log provider
     * @param globalMonitors global server monitors
     * @param extensionFactories extension factories for extensions that should participate in recovery
     * @param providedLogFiles log files from database
     * @param forceRunRecovery to force recovery to run even if the usual checks indicates that it's not required.
     * In specific cases, like after store copy there's always a need for doing a recovery or at least to start the db, checkpoint and shut down,
     * even if the normal "is recovery required" checks says that recovery isn't required.
     * @throws IOException on any unexpected I/O exception encountered during recovery.
     */
    public static void performRecovery( FileSystemAbstraction fs, PageCache pageCache, DatabaseTracers tracers,
            Config config, DatabaseLayout databaseLayout, StorageEngineFactory storageEngineFactory, boolean forceRunRecovery,
            LogProvider logProvider, Monitors globalMonitors,
            Iterable<ExtensionFactory<?>> extensionFactories, Optional<LogFiles> providedLogFiles,
            RecoveryStartupChecker startupChecker, MemoryTracker memoryTracker, Clock clock ) throws IOException
    {
        Log recoveryLog = logProvider.getLog( Recovery.class );
        if ( !forceRunRecovery && !isRecoveryRequired( fs, pageCache, databaseLayout, storageEngineFactory, config, providedLogFiles, memoryTracker ) )
        {
            return;
        }
        checkAllFilesPresence( databaseLayout, fs, pageCache, storageEngineFactory );
        LifeSupport recoveryLife = new LifeSupport();
        Monitors monitors = new Monitors( globalMonitors, logProvider );
        DatabasePageCache databasePageCache = new DatabasePageCache( pageCache, IOController.DISABLED );
        SimpleLogService logService = new SimpleLogService( logProvider );
        VersionAwareLogEntryReader logEntryReader = new VersionAwareLogEntryReader( storageEngineFactory.commandReaderFactory() );
        DatabaseReadOnlyChecker readOnlyChecker = writable();

        DatabaseSchemaState schemaState = new DatabaseSchemaState( logProvider );
        JobScheduler scheduler = JobSchedulerFactory.createInitialisedScheduler();

        VersionContextSupplier versionContextSupplier = EmptyVersionContextSupplier.EMPTY;
        DatabaseHealth databaseHealth = new DatabaseHealth( PanicEventGenerator.NO_OP, recoveryLog );

        TokenHolders tokenHolders = new TokenHolders( new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TYPE_LABEL ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TYPE_RELATIONSHIP_TYPE ) );

        RecoveryCleanupWorkCollector recoveryCleanupCollector =
                new GroupingRecoveryCleanupWorkCollector( scheduler, INDEX_CLEANUP, INDEX_CLEANUP_WORK, databaseLayout.getDatabaseName() );
        DatabaseExtensions extensions = instantiateRecoveryExtensions( databaseLayout, fs, config, logService, databasePageCache, scheduler,
                                                                       DbmsInfo.TOOL, monitors, tokenHolders, recoveryCleanupCollector, readOnlyChecker,
                                                                       extensionFactories, tracers.getPageCacheTracer() );
        DefaultIndexProviderMap indexProviderMap = new DefaultIndexProviderMap( extensions, config );

        StorageEngine storageEngine = storageEngineFactory.instantiate( fs, databaseLayout, config, databasePageCache, tokenHolders, schemaState,
                getConstraintSemantics(), indexProviderMap, NO_LOCK_SERVICE,
                new DefaultIdGeneratorFactory( fs, recoveryCleanupCollector, databaseLayout.getDatabaseName() ),
                new DefaultIdController(), databaseHealth, logService.getInternalLogProvider(), recoveryCleanupCollector, tracers.getPageCacheTracer(),
                true, readOnlyChecker, memoryTracker );

        // Label index
        FullScanStoreView fullScanStoreView = new FullScanStoreView( NO_LOCK_SERVICE, storageEngine::newReader, config, scheduler );
        LabelScanStore labelScanStore = Database.buildLabelIndex( recoveryCleanupCollector, storageEngine, fullScanStoreView, monitors,
                logProvider, databasePageCache, databaseLayout, fs, readOnlyChecker, config, tracers.getPageCacheTracer(), memoryTracker );

        // Schema indexes
        IndexStoreViewFactory indexStoreViewFactory =
                new IndexStoreViewFactory( config, storageEngine::newReader, NO_LOCKS, fullScanStoreView,
                                           labelScanStore, NO_LOCK_SERVICE, logProvider );

        IndexStatisticsStore indexStatisticsStore =
                new IndexStatisticsStore( databasePageCache, databaseLayout, recoveryCleanupCollector, readOnlyChecker, tracers.getPageCacheTracer() );
        IndexingService indexingService = Database.buildIndexingService( storageEngine, schemaState, indexStoreViewFactory, indexStatisticsStore,
                config, scheduler, indexProviderMap, tokenHolders, logProvider, logProvider, monitors.newMonitor( IndexingService.Monitor.class ),
                tracers.getPageCacheTracer(), memoryTracker, databaseLayout.getDatabaseName(), readOnlyChecker );

        MetadataProvider metadataProvider = storageEngine.metadataProvider();

        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( databaseLayout, config, databasePageCache, fs, logProvider, tokenHolders, schemaState, getConstraintSemantics(),
                NO_LOCK_SERVICE, databaseHealth, new DefaultIdGeneratorFactory( fs, recoveryCleanupCollector, databaseLayout.getDatabaseName() ),
                new DefaultIdController(), readOnlyChecker, versionContextSupplier, logService, metadataProvider );

        LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fs )
                .withLogEntryReader( logEntryReader )
                .withConfig( config )
                .withDependencies( dependencies )
                .withMemoryTracker( memoryTracker )
                .build();

        boolean failOnCorruptedLogFiles = config.get( GraphDatabaseInternalSettings.fail_on_corrupted_log_files );
        validateStoreId( logFiles, storageEngine.getStoreId(), config );

        TransactionMetadataCache metadataCache = new TransactionMetadataCache();
        PhysicalLogicalTransactionStore transactionStore = new PhysicalLogicalTransactionStore( logFiles, metadataCache, logEntryReader, monitors,
                failOnCorruptedLogFiles );
        BatchingTransactionAppender transactionAppender = new BatchingTransactionAppender( logFiles, LogRotation.NO_ROTATION, metadataCache,
                metadataProvider, databaseHealth );

        LifeSupport schemaLife = new LifeSupport();
        schemaLife.add( storageEngine.schemaAndTokensLifecycle() );
        schemaLife.add( indexingService );

        var doParallelRecovery = config.get( GraphDatabaseInternalSettings.do_parallel_recovery );
        TransactionLogsRecovery transactionLogsRecovery =
                transactionLogRecovery( fs, metadataProvider, monitors.newMonitor( RecoveryMonitor.class ),
                                        monitors.newMonitor( RecoveryStartInformationProvider.Monitor.class ), logFiles, storageEngine,
                                        transactionStore, metadataProvider, schemaLife, databaseLayout, failOnCorruptedLogFiles, recoveryLog,
                                        startupChecker, tracers.getPageCacheTracer(), memoryTracker, doParallelRecovery );

        CheckPointerImpl.ForceOperation forceOperation = new DefaultForceOperation( indexingService, labelScanStore, storageEngine );
        var checkpointAppender = logFiles.getCheckpointFile().getCheckpointAppender();
        CheckPointerImpl checkPointer =
                new CheckPointerImpl( metadataProvider, RecoveryThreshold.INSTANCE, forceOperation, LogPruning.NO_PRUNING, checkpointAppender,
                        databaseHealth, logProvider, tracers, IOController.DISABLED, new StoreCopyCheckPointMutex(), versionContextSupplier, clock );
        recoveryLife.add( scheduler );
        recoveryLife.add( recoveryCleanupCollector );
        recoveryLife.add( extensions );
        recoveryLife.add( indexProviderMap );
        recoveryLife.add( storageEngine );
        recoveryLife.add( new MissingTransactionLogsCheck( databaseLayout, config, fs, logFiles, recoveryLog ) );
        recoveryLife.add( labelScanStore );
        recoveryLife.add( logFiles );
        recoveryLife.add( transactionLogsRecovery );
        recoveryLife.add( transactionAppender );
        recoveryLife.add( checkPointer );
        try
        {
            recoveryLife.start();

            if ( databaseHealth.isHealthy() )
            {
                checkPointer.forceCheckPoint( new SimpleTriggerInfo( "Recovery completed." ) );
            }
        }
        finally
        {
            recoveryLife.shutdown();
        }
    }

    public static void validateStoreId( LogFiles logFiles, StoreId storeId, Config config )
    {
        if ( !config.get( GraphDatabaseInternalSettings.recovery_ignore_store_id_validation ) )
        {
            StoreId txStoreId = logFiles.getTailInformation().lastStoreId;
            if ( !StoreId.UNKNOWN.equals( txStoreId ) )
            {
                if ( !storeId.equalsIgnoringVersion( txStoreId ) )
                {
                    throw new RuntimeException( "Mismatching store id. Store StoreId: " + storeId +
                            ". Transaction log StoreId: " + txStoreId );
                }
            }
        }
    }

    private static void checkAllFilesPresence( DatabaseLayout databaseLayout, FileSystemAbstraction fs, PageCache pageCache,
            StorageEngineFactory storageEngineFactory )
    {
        StorageFilesState state = storageEngineFactory.checkStoreFileState( fs, databaseLayout, pageCache );
        if ( state.getRecoveryState() == RecoveryState.UNRECOVERABLE )
        {
            throw new RuntimeException( format( "Store files %s is(are) missing and recovery is not possible. Please restore from a consistent backup.",
                    state.getMissingFiles() ) );
        }
    }

    private static TransactionLogsRecovery transactionLogRecovery( FileSystemAbstraction fileSystemAbstraction, TransactionIdStore transactionIdStore,
            RecoveryMonitor recoveryMonitor, RecoveryStartInformationProvider.Monitor positionMonitor, LogFiles logFiles,
            StorageEngine storageEngine, LogicalTransactionStore logicalTransactionStore, LogVersionRepository logVersionRepository,
            Lifecycle schemaLife, DatabaseLayout databaseLayout, boolean failOnCorruptedLogFiles, Log log, RecoveryStartupChecker startupChecker,
            PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker, boolean doParallelRecovery )
    {
        RecoveryService recoveryService = new DefaultRecoveryService( storageEngine, transactionIdStore, logicalTransactionStore,
                                                                      logVersionRepository, logFiles, positionMonitor, log, doParallelRecovery );
        CorruptedLogsTruncator logsTruncator =
                new CorruptedLogsTruncator( databaseLayout.databaseDirectory(), logFiles, fileSystemAbstraction, memoryTracker );
        ProgressReporter progressReporter = new LogProgressReporter( log );
        return new TransactionLogsRecovery( recoveryService, logsTruncator, schemaLife, recoveryMonitor, progressReporter, failOnCorruptedLogFiles,
                startupChecker, pageCacheTracer );
    }

    private static Iterable<ExtensionFactory<?>> loadExtensions()
    {
        return Iterables.cast( Services.loadAll( ExtensionFactory.class ) );
    }

    private static DatabaseExtensions instantiateRecoveryExtensions( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem, Config config,
                                                                     LogService logService, PageCache pageCache, JobScheduler jobScheduler,
                                                                     DbmsInfo dbmsInfo, Monitors monitors, TokenHolders tokenHolders,
                                                                     RecoveryCleanupWorkCollector recoveryCleanupCollector,
                                                                     DatabaseReadOnlyChecker readOnlyChecker,
                                                                     Iterable<ExtensionFactory<?>> extensionFactories,
                                                                     PageCacheTracer pageCacheTracer )
    {
        List<ExtensionFactory<?>> recoveryExtensions = stream( extensionFactories )
                .filter( extension -> extension.getClass().isAnnotationPresent( RecoveryExtension.class ) )
                .collect( toList() );

        Dependencies deps = new Dependencies();
        NonListenableMonitors nonListenableMonitors = new NonListenableMonitors( monitors, logService.getInternalLogProvider() );
        deps.satisfyDependencies( fileSystem, config, logService, pageCache, nonListenableMonitors, jobScheduler,
                tokenHolders, recoveryCleanupCollector, pageCacheTracer, databaseLayout, readOnlyChecker );
        DatabaseExtensionContext extensionContext = new DatabaseExtensionContext( databaseLayout, dbmsInfo, deps );
        return new DatabaseExtensions( extensionContext, recoveryExtensions, deps, ExtensionFailureStrategies.fail() );
    }

    private static boolean isRecoveryRequired( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout databaseLayout,
            StorageEngineFactory storageEngineFactory, Config config, Optional<LogFiles> logFiles, MemoryTracker memoryTracker ) throws IOException
    {
        RecoveryRequiredChecker requiredChecker = new RecoveryRequiredChecker( fs, pageCache, config, storageEngineFactory );
        return logFiles.isPresent() ? requiredChecker.isRecoveryRequiredAt( databaseLayout, logFiles.get() )
                                          : requiredChecker.isRecoveryRequiredAt( databaseLayout, memoryTracker );
    }

    private static PageCache getPageCache( Config config, FileSystemAbstraction fs, JobScheduler jobScheduler )
    {
        ConfiguringPageCacheFactory pageCacheFactory =
                new ConfiguringPageCacheFactory( fs, config, PageCacheTracer.NULL, NullLog.getInstance(), jobScheduler, Clocks.nanoClock(), new MemoryPools() );
        return pageCacheFactory.getOrCreatePageCache();
    }

    static void throwUnableToCleanRecover( Throwable t )
    {
        throw new RuntimeException(
                "Error reading transaction logs, recovery not possible. To force the database to start anyway, you can specify '" +
                        GraphDatabaseInternalSettings.fail_on_corrupted_log_files.name() + "=false'. This will try to recover as much " +
                        "as possible and then truncate the corrupt part of the transaction log. Doing this means your database " +
                        "integrity might be compromised, please consider restoring from a consistent backup instead.", t );
    }

    // We need to create monitors that do not allow listener registration here since at this point another version of extensions already stared by owning
    // database life and if we will allow registration of listeners here we will end-up having same event captured by multiple listeners resulting in
    // for example duplicated logging records in user facing logs
    private static class NonListenableMonitors extends Monitors
    {
        NonListenableMonitors( Monitors monitors, LogProvider logProvider )
        {
            super( monitors, logProvider );
        }

        @Override
        public void addMonitorListener( Object monitorListener, String... tags )
        {
        }
    }

    private static class MissingTransactionLogsCheck extends LifecycleAdapter
    {
        private final DatabaseLayout databaseLayout;
        private final Config config;
        private final FileSystemAbstraction fs;
        private final LogFiles logFiles;
        private final Log log;

        MissingTransactionLogsCheck( DatabaseLayout databaseLayout, Config config, FileSystemAbstraction fs,
                                     LogFiles logFiles, Log log )
        {
            this.databaseLayout = databaseLayout;
            this.config = config;
            this.fs = fs;
            this.logFiles = logFiles;
            this.log = log;
        }

        @Override
        public void init() throws IOException
        {
            checkForMissingLogFiles();
        }

        private void checkForMissingLogFiles() throws IOException
        {
            if ( logFiles.getTailInformation().logsMissing() )
            {
                if ( config.get( GraphDatabaseSettings.fail_on_missing_files ) )
                {
                    log.error( "Transaction logs are missing and recovery is not possible." );
                    log.info( "To force the database to start anyway, you can specify '%s=false'. " +
                            "This will create new transaction log and will update database metadata accordingly. " +
                            "Doing this means your database integrity might be compromised, " +
                            "please consider restoring from a consistent backup instead.",
                            GraphDatabaseSettings.fail_on_missing_files.name() );

                    Path[] logFiles = findLegacyLogFiles();
                    if ( logFiles.length > 0 )
                    {
                        log.warn( "Transaction log files were found in database directory, rather than the transaction log directory." );
                        log.warn( "Please move or remove the following %s misplaced transaction log file or files:", logFiles.length );
                        for ( Path logFile : logFiles )
                        {
                            log.warn( logFile.toAbsolutePath().toString() );
                        }
                    }

                    throw new RuntimeException( "Transaction logs are missing and recovery is not possible." );
                }
                log.warn( "No transaction logs were detected, but recovery was forced by user." );
            }
        }

        private Path[] findLegacyLogFiles() throws IOException
        {
            LegacyTransactionLogsLocator locator = new LegacyTransactionLogsLocator( Config.defaults(), databaseLayout );
            Path logsDirectory = locator.getTransactionLogsDirectory();
            TransactionLogFilesHelper logFilesHelper = new TransactionLogFilesHelper( fs, logsDirectory );
            return logFilesHelper.getMatchedFiles();
        }
    }
}
