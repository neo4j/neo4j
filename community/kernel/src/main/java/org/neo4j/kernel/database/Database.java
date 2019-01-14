/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.database;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.dbms.database.DatabaseConfig;
import org.neo4j.dbms.database.DatabasePageCache;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LoggingMonitor;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailability;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.SettingChangeListener;
import org.neo4j.kernel.diagnostics.providers.DbmsDiagnosticsManager;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.KernelTransactionsSnapshot;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.StackingQueryRegistrationOperations;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionHooks;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.operations.QueryRegistrationOperations;
import org.neo4j.kernel.impl.api.scan.FullLabelStream;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitor;
import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitorScheduler;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.pagecache.PageCacheLifecycle;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.id.IdController;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.stats.IdBasedStoreEntityCounters;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.LogVersionUpgradeChecker;
import org.neo4j.kernel.impl.transaction.log.LoggingLogFileMonitor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointScheduler;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckpointerLifecycle;
import org.neo4j.kernel.impl.transaction.log.checkpoint.DefaultCheckPointMonitor;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFileCreationMonitor;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruningImpl;
import org.neo4j.kernel.impl.transaction.log.reverse.ReverseTransactionCursorLoggingMonitor;
import org.neo4j.kernel.impl.transaction.log.reverse.ReversedSingleFileTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotationImpl;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.DefaultLogRotationMonitor;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.state.DatabaseFileListing;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.storeview.DynamicIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.internal.DatabaseEventHandlers;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.TransactionEventHandlers;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.kernel.recovery.LoggingLogTailScannerMonitor;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.migration.DatabaseMigrator;
import org.neo4j.storageengine.migration.DatabaseMigratorFactory;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;
import static org.neo4j.helpers.Exceptions.throwIfUnchecked;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.extension.ExtensionFailureStrategies.fail;
import static org.neo4j.kernel.recovery.Recovery.performRecovery;

public class Database extends LifecycleAdapter
{
    private final Monitors globalMonitors;
    private final DependencyResolver globalDependencies;
    private final PageCache globalPageCache;
    private final Tracers globalTracers;
    private final Config globalConfig;

    private final Log msgLog;
    private final LogService logService;
    private final LogProvider logProvider;
    private final LogProvider userLogProvider;
    private final TokenNameLookup tokenNameLookup;
    private final TokenHolders tokenHolders;
    private final StatementLocksFactory statementLocksFactory;
    private final SchemaWriteGuard schemaWriteGuard;
    private final TransactionEventHandlers transactionEventHandlers;
    private final IdGeneratorFactory idGeneratorFactory;
    private final JobScheduler scheduler;
    private final LockService lockService;
    private final FileSystemAbstraction fs;
    private final TransactionMonitor transactionMonitor;
    private final DatabaseHealth databaseHealth;
    private final TransactionHeaderInformationFactory transactionHeaderInformationFactory;
    private final CommitProcessFactory commitProcessFactory;
    private final ConstraintSemantics constraintSemantics;
    private final Procedures procedures;
    private final IOLimiter ioLimiter;
    private final DatabaseAvailabilityGuard databaseAvailabilityGuard;
    private final SystemNanoClock clock;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final CollectionsFactorySupplier collectionsFactorySupplier;
    private final Locks locks;
    private final DatabaseAvailability databaseAvailability;
    private final DatabaseEventHandlers eventHandlers;
    private final DatabaseMigratorFactory databaseMigratorFactory;

    private Dependencies databaseDependencies;
    private LifeSupport life;
    private IndexProviderMap indexProviderMap;
    private DatabaseConfig config;
    private final String databaseName;
    private final DatabaseLayout databaseLayout;
    private final boolean readOnly;
    private final IdController idController;
    private final DatabaseInfo databaseInfo;
    private final VersionContextSupplier versionContextSupplier;
    private final AccessCapability accessCapability;

    private StorageEngine storageEngine;
    private QueryExecutionEngine executionEngine;
    private DatabaseKernelModule kernelModule;
    private final Iterable<ExtensionFactory<?>> extensionFactories;
    private final Function<DatabaseLayout,DatabaseLayoutWatcher> watcherServiceFactory;
    private final GraphDatabaseFacade facade;
    private final Iterable<QueryEngineProvider> engineProviders;
    private volatile boolean started;
    private Monitors databaseMonitors;
    private DatabasePageCache databasePageCache;
    private CheckpointerLifecycle checkpointerLifecycle;

    public Database( DatabaseCreationContext context )
    {
        this.databaseName = context.getDatabaseName();
        this.databaseLayout = context.getDatabaseLayout();
        this.config = new DatabaseConfig( context.getConfig() );
        this.idGeneratorFactory = context.getIdGeneratorFactory();
        this.tokenNameLookup = context.getTokenNameLookup();
        this.globalDependencies = context.getGlobalDependencies();
        this.scheduler = context.getScheduler();
        this.logService = context.getLogService();
        this.storeCopyCheckPointMutex = context.getStoreCopyCheckPointMutex();
        this.logProvider = context.getLogService().getInternalLogProvider();
        this.userLogProvider = context.getLogService().getUserLogProvider();
        this.tokenHolders = context.getTokenHolders();
        this.locks = context.getLocks();
        this.statementLocksFactory = context.getStatementLocksFactory();
        this.schemaWriteGuard = context.getSchemaWriteGuard();
        this.transactionEventHandlers = context.getTransactionEventHandlers();
        this.fs = context.getFs();
        this.transactionMonitor = context.getTransactionMonitor();
        this.databaseHealth = context.getDatabaseHealth();
        this.transactionHeaderInformationFactory = context.getTransactionHeaderInformationFactory();
        this.constraintSemantics = context.getConstraintSemantics();
        this.globalMonitors = context.getMonitors();
        this.globalTracers = context.getTracers();
        this.globalConfig = context.getConfig();
        this.procedures = context.getProcedures();
        this.ioLimiter = context.getIoLimiter();
        this.databaseAvailabilityGuard = context.getDatabaseAvailabilityGuard();
        this.clock = context.getClock();
        this.accessCapability = context.getAccessCapability();
        this.eventHandlers = context.getEventHandlers();

        this.readOnly = context.getConfig().get( GraphDatabaseSettings.read_only );
        this.idController = context.getIdController();
        this.databaseInfo = context.getDatabaseInfo();
        this.versionContextSupplier = context.getVersionContextSupplier();
        this.extensionFactories = context.getExtensionFactories();
        this.watcherServiceFactory = context.getWatcherServiceFactory();
        this.facade = context.getFacade();
        this.engineProviders = context.getEngineProviders();
        this.msgLog = logProvider.getLog( getClass() );
        this.lockService = new ReentrantLockService();
        this.commitProcessFactory = context.getCommitProcessFactory();
        this.globalPageCache = context.getPageCache();
        this.collectionsFactorySupplier = context.getCollectionsFactorySupplier();
        this.databaseAvailability = context.getDatabaseAvailability();
        this.databaseMigratorFactory = context.getDatabaseMigratorFactory();
    }

    @Override
    public void start()
    {
        if ( started )
        {
            return;
        }
        try
        {
            config = new DatabaseConfig( globalConfig );
            databaseDependencies = new Dependencies( globalDependencies );
            databasePageCache = new DatabasePageCache( globalPageCache );
            databaseMonitors = new Monitors( globalMonitors );

            life = new LifeSupport();
            life.add( config );

            LogFileCreationMonitor physicalLogMonitor = databaseMonitors.newMonitor( LogFileCreationMonitor.class );

            databaseDependencies.satisfyDependency( this );
            databaseDependencies.satisfyDependency( config );
            databaseDependencies.satisfyDependency( databaseMonitors );
            databaseDependencies.satisfyDependency( databasePageCache );
            databaseDependencies.satisfyDependency( tokenHolders );
            databaseDependencies.satisfyDependency( facade );
            databaseDependencies.satisfyDependency( databaseHealth );
            databaseDependencies.satisfyDependency( storeCopyCheckPointMutex );
            databaseDependencies.satisfyDependency( transactionMonitor );
            databaseDependencies.satisfyDependency( locks );
            databaseDependencies.satisfyDependency( databaseAvailabilityGuard );
            databaseDependencies.satisfyDependency( databaseAvailability );
            databaseDependencies.satisfyDependency( idGeneratorFactory );
            databaseDependencies.satisfyDependency( idController );
            databaseDependencies.satisfyDependency( new IdBasedStoreEntityCounters( this.idGeneratorFactory ) );
            databaseDependencies.satisfyDependency( lockService );

            DefaultLogRotationMonitor logRotationMonitor = new DefaultLogRotationMonitor();
            DefaultCheckPointMonitor checkPointMonitor = new DefaultCheckPointMonitor();

            databaseMonitors.addMonitorListener( logRotationMonitor );
            databaseMonitors.addMonitorListener( checkPointMonitor );

            databaseDependencies.satisfyDependency( logRotationMonitor );
            databaseDependencies.satisfyDependency( checkPointMonitor );

            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.immediate();
            databaseDependencies.satisfyDependencies( recoveryCleanupWorkCollector );

            life.add( new PageCacheLifecycle( databasePageCache ) );
            life.add( initializeExtensions( databaseDependencies ) );

            DatabaseLayoutWatcher watcherService = watcherServiceFactory.apply( databaseLayout );
            life.add( watcherService );
            databaseDependencies.satisfyDependency( watcherService );

            // Upgrade the store before we begin
            upgradeStore();

            // Check the tail of transaction logs and validate version
            final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();

            LogFiles logFiles =
                    LogFilesBuilder.builder( databaseLayout, fs ).withLogEntryReader( logEntryReader ).withLogFileMonitor( physicalLogMonitor ).withConfig(
                            config ).withDependencies( databaseDependencies ).build();

            databaseMonitors.addMonitorListener( new LoggingLogFileMonitor( msgLog ) );
            databaseMonitors.addMonitorListener( new LoggingLogTailScannerMonitor( logService.getInternalLog( LogTailScanner.class ) ) );
            databaseMonitors.addMonitorListener(
                    new ReverseTransactionCursorLoggingMonitor( logService.getInternalLog( ReversedSingleFileTransactionCursor.class ) ) );
            LogTailScanner tailScanner =
                    new LogTailScanner( logFiles, logEntryReader, databaseMonitors, config.get( GraphDatabaseSettings.fail_on_corrupted_log_files ) );
            LogVersionUpgradeChecker.check( tailScanner, config );

            performRecovery( fs, databasePageCache, config, databaseLayout, logProvider, databaseMonitors, extensionFactories,
                    Optional.of( tailScanner ) );

            // Build all modules and their services
            DatabaseSchemaState databaseSchemaState = new DatabaseSchemaState( logProvider );

            Supplier<KernelTransactionsSnapshot> transactionsSnapshotSupplier = () -> kernelModule.kernelTransactions().get();
            idController.initialize( transactionsSnapshotSupplier );

            storageEngine = buildStorageEngine( databasePageCache, databaseSchemaState, versionContextSupplier );
            life.add( logFiles );

            // Label index
            NeoStoreIndexStoreView neoStoreIndexStoreView = new NeoStoreIndexStoreView( lockService, storageEngine::newReader );
            LabelScanStore labelScanStore =
                    buildLabelIndex( databasePageCache, recoveryCleanupWorkCollector, storageEngine, neoStoreIndexStoreView, databaseMonitors );

            // Schema indexes
            DynamicIndexStoreView indexStoreView =
                    new DynamicIndexStoreView( neoStoreIndexStoreView, labelScanStore, lockService, storageEngine::newReader, logProvider );
            IndexStatisticsStore indexStatisticsStore = new IndexStatisticsStore( databasePageCache, databaseLayout, recoveryCleanupWorkCollector );
            IndexingService indexingService = buildIndexingService( storageEngine, databaseSchemaState, indexStoreView, indexStatisticsStore );

            TransactionIdStore transactionIdStore = databaseDependencies.resolveDependency( TransactionIdStore.class );

            versionContextSupplier.init( transactionIdStore::getLastClosedTransactionId );

            CheckPointerImpl.ForceOperation forceOperation = new DefaultForceOperation( indexingService, labelScanStore, storageEngine );
            DatabaseTransactionLogModule transactionLogModule =
                    buildTransactionLogs( logFiles, config, logProvider, scheduler, forceOperation,
                            logEntryReader, transactionIdStore, databaseMonitors );
            transactionLogModule.satisfyDependencies( databaseDependencies );

            final DatabaseKernelModule kernelModule = buildKernel(
                    logFiles,
                    transactionLogModule.transactionAppender(),
                    indexingService,
                    databaseSchemaState,
                    labelScanStore,
                    storageEngine,
                    transactionIdStore,
                    databaseAvailabilityGuard,
                    clock,
                    indexStatisticsStore );

            kernelModule.satisfyDependencies( databaseDependencies );

            // Do these assignments last so that we can ensure no cyclical dependencies exist
            this.kernelModule = kernelModule;

            databaseDependencies.satisfyDependency( databaseSchemaState );
            databaseDependencies.satisfyDependency( logEntryReader );
            databaseDependencies.satisfyDependency( storageEngine );
            databaseDependencies.satisfyDependency( labelScanStore );
            databaseDependencies.satisfyDependency( indexingService );
            databaseDependencies.satisfyDependency( indexStoreView );
            databaseDependencies.satisfyDependency( indexStatisticsStore );
            databaseDependencies.satisfyDependency( indexProviderMap );
            databaseDependencies.satisfyDependency( forceOperation );

            this.executionEngine = QueryEngineProvider.initialize( databaseDependencies, facade, engineProviders );
            this.checkpointerLifecycle = new CheckpointerLifecycle( transactionLogModule.checkPointer(), databaseHealth );

            databaseDependencies.resolveDependency( DbmsDiagnosticsManager.class ).dumpDatabaseDiagnostics( this );

            life.add( databaseAvailability );
            life.setLast( checkpointerLifecycle );
        }
        catch ( Throwable e )
        {
            // Something unexpected happened during startup
            msgLog.warn( "Exception occurred while setting up store modules. Attempting to close things down.", e );
            try
            {
                // Close the storage engine, so that locks are released properly
                if ( storageEngine != null )
                {
                    storageEngine.forceClose();
                }
            }
            catch ( Exception closeException )
            {
                msgLog.error( "Couldn't close database after startup failure", closeException );
            }
            throwIfUnchecked( e );
            throw new RuntimeException( e );
        }
        try
        {
            life.start();
        }
        catch ( Throwable e )
        {
            // Something unexpected happened during startup
            msgLog.warn( "Exception occurred while starting the datasource. Attempting to close things down.", e );
            try
            {
                life.shutdown();
                storageEngine.forceClose();
            }
            catch ( Exception closeException )
            {
                msgLog.error( "Couldn't close database after startup failure", closeException );
            }
            throw new RuntimeException( e );
        }
        /*
         * At this point recovery has completed and the datasource is ready for use. Whatever panic might have
         * happened before has been healed. So we can safely set the kernel health to ok.
         * This right now has any real effect only in the case of internal restarts (for example, after a store copy).
         * Standalone instances will have to be restarted by the user, as is proper for all database panics.
         */
        databaseHealth.healed();
        started = true;
    }

    private LifeSupport initializeExtensions( Dependencies dependencies )
    {
        LifeSupport extensionsLife = new LifeSupport();

        extensionsLife.add( new DatabaseExtensions( new DatabaseExtensionContext( databaseLayout, databaseInfo, dependencies ), extensionFactories,
                dependencies, fail() ) );

        indexProviderMap = extensionsLife.add( new DefaultIndexProviderMap( dependencies, config ) );
        dependencies.satisfyDependency( indexProviderMap );
        extensionsLife.init();
        return extensionsLife;
    }

    private void upgradeStore()
    {
        final DatabaseMigrator databaseMigrator = databaseMigratorFactory.createDatabaseMigrator( databaseLayout, databaseDependencies );
        databaseMigrator.migrate();
    }

    private StorageEngine buildStorageEngine( PageCache pageCache, SchemaState schemaState, VersionContextSupplier versionContextSupplier )
    {
        RecordStorageEngine storageEngine =
                new RecordStorageEngine( databaseLayout, config, pageCache, fs, logProvider, tokenHolders,
                        schemaState, constraintSemantics,
                        lockService, databaseHealth,
                        idGeneratorFactory, idController,
                        versionContextSupplier );

        // We pretend that the storage engine abstract hides all details within it. Whereas that's mostly
        // true it's not entirely true for the time being. As long as we need this call below, which
        // makes available one or more internal things to the outside world, there are leaks to plug.
        storageEngine.satisfyDependencies( databaseDependencies );

        return life.add( storageEngine );
    }

    /**
     * Builds an {@link IndexingService} and adds it to this database's {@link LifeSupport}.
     */
    private IndexingService buildIndexingService(
            StorageEngine storageEngine,
            DatabaseSchemaState databaseSchemaState,
            DynamicIndexStoreView indexStoreView,
            IndexStatisticsStore indexStatisticsStore )
    {
        return life.add( buildIndexingService( storageEngine, databaseSchemaState, indexStoreView, indexStatisticsStore, config, scheduler, indexProviderMap,
                tokenNameLookup, logProvider, userLogProvider, databaseMonitors.newMonitor( IndexingService.Monitor.class ) ) );
    }

    /**
     * Convenience method for building am {@link IndexingService}. Doesn't add it to a {@link LifeSupport}.
     */
    public static IndexingService buildIndexingService(
            StorageEngine storageEngine,
            DatabaseSchemaState databaseSchemaState,
            DynamicIndexStoreView indexStoreView,
            IndexStatisticsStore indexStatisticsStore,
            Config config,
            JobScheduler jobScheduler,
            IndexProviderMap indexProviderMap,
            TokenNameLookup tokenNameLookup,
            LogProvider internalLogProvider,
            LogProvider userLogProvider,
            IndexingService.Monitor indexingServiceMonitor )
    {
        IndexingService indexingService = IndexingServiceFactory.createIndexingService( config, jobScheduler, indexProviderMap, indexStoreView,
                tokenNameLookup, initialSchemaRulesLoader( storageEngine ), internalLogProvider, userLogProvider, indexingServiceMonitor,
                databaseSchemaState, indexStatisticsStore );
        storageEngine.addIndexUpdateListener( indexingService );
        return indexingService;
    }

    /**
     * Builds a {@link LabelScanStore} and adds it to this database's {@link LifeSupport}.
     */
    private LabelScanStore buildLabelIndex(
            PageCache pageCache,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            StorageEngine storageEngine,
            NeoStoreIndexStoreView neoStoreIndexStoreView,
            Monitors monitors )
    {
        return life.add( buildLabelIndex( recoveryCleanupWorkCollector, storageEngine, neoStoreIndexStoreView, monitors, logProvider,
                pageCache, databaseLayout, fs, readOnly ) );
    }

    /**
     * Convenience method for building a {@link LabelScanStore}. Doesn't add it to a {@link LifeSupport}.
     */
    public static LabelScanStore buildLabelIndex(
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            StorageEngine storageEngine,
            IndexStoreView indexStoreView,
            Monitors monitors,
            LogProvider logProvider,
            PageCache pageCache,
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fs,
            boolean readOnly )
    {
        monitors.addMonitorListener( new LoggingMonitor( logProvider.getLog( NativeLabelScanStore.class ) ) );
        NativeLabelScanStore labelScanStore = new NativeLabelScanStore( pageCache, databaseLayout, fs, new FullLabelStream( indexStoreView ),
                readOnly, monitors, recoveryCleanupWorkCollector );
        storageEngine.addNodeLabelUpdateListener( labelScanStore );
        return labelScanStore;
    }

    private DatabaseTransactionLogModule buildTransactionLogs( LogFiles logFiles, Config config,
            LogProvider logProvider, JobScheduler scheduler, CheckPointerImpl.ForceOperation forceOperation,
            LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader, TransactionIdStore transactionIdStore, Monitors monitors )
    {
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache();
        if ( config.get( GraphDatabaseSettings.ephemeral ) )
        {
            config.augmentDefaults( GraphDatabaseSettings.keep_logical_logs, "1 files" );
        }

        final LogPruning logPruning =
                new LogPruningImpl( fs, logFiles, logProvider, new LogPruneStrategyFactory(), clock, config );

        final LogRotation logRotation =
                new LogRotationImpl( logFiles, clock, databaseHealth, monitors.newMonitor( LogRotationMonitor.class ) );

        final TransactionAppender appender = life.add( new BatchingTransactionAppender(
                logFiles, logRotation, transactionMetadataCache, transactionIdStore, databaseHealth ) );
        final LogicalTransactionStore logicalTransactionStore =
                new PhysicalLogicalTransactionStore( logFiles, transactionMetadataCache, logEntryReader, monitors, true );

        CheckPointThreshold threshold = CheckPointThreshold.createThreshold( config, clock, logPruning, logProvider );

        final CheckPointerImpl checkPointer = new CheckPointerImpl(
                transactionIdStore, threshold, forceOperation, logPruning, appender, databaseHealth, logProvider,
                globalTracers.getCheckPointTracer(), ioLimiter, storeCopyCheckPointMutex, monitors.newMonitor( CheckPointerMonitor.class ) );

        long recurringPeriod = threshold.checkFrequencyMillis();
        CheckPointScheduler checkPointScheduler = new CheckPointScheduler( checkPointer, ioLimiter, scheduler,
                recurringPeriod, databaseHealth );

        life.add( checkPointer );
        life.add( checkPointScheduler );

        return new DatabaseTransactionLogModule( logicalTransactionStore, logFiles, logRotation, checkPointer, appender );
    }

    private DatabaseKernelModule buildKernel( LogFiles logFiles, TransactionAppender appender,
            IndexingService indexingService, DatabaseSchemaState databaseSchemaState, LabelScanStore labelScanStore,
            StorageEngine storageEngine, TransactionIdStore transactionIdStore,
            AvailabilityGuard databaseAvailabilityGuard, SystemNanoClock clock,
            IndexStatisticsStore indexStatisticsStore )
    {
        AtomicReference<CpuClock> cpuClockRef = setupCpuClockAtomicReference();
        AtomicReference<HeapAllocation> heapAllocationRef = setupHeapAllocationAtomicReference();

        TransactionCommitProcess transactionCommitProcess = commitProcessFactory.create( appender, storageEngine, config );

        /*
         * This is used by explicit indexes and constraint indexes whenever a transaction is to be spawned
         * from within an existing transaction. It smells, and we should look over alternatives when time permits.
         */
        Supplier<Kernel> kernelProvider = () -> kernelModule.kernelAPI();

        ConstraintIndexCreator constraintIndexCreator = new ConstraintIndexCreator( kernelProvider, indexingService, logProvider );

        StatementOperationParts statementOperationParts = databaseDependencies.satisfyDependency(
                buildStatementOperations( cpuClockRef, heapAllocationRef ) );

        TransactionHooks hooks = new TransactionHooks();

        KernelTransactions kernelTransactions = life.add(
                new KernelTransactions( config, statementLocksFactory, constraintIndexCreator, statementOperationParts, schemaWriteGuard,
                        transactionHeaderInformationFactory, transactionCommitProcess, hooks,
                        transactionMonitor, databaseAvailabilityGuard, globalTracers, storageEngine, procedures, transactionIdStore, clock, cpuClockRef,
                        heapAllocationRef, accessCapability, versionContextSupplier, collectionsFactorySupplier,
                        constraintSemantics, databaseSchemaState, tokenHolders, getDatabaseName(), indexingService, labelScanStore, indexStatisticsStore,
                        databaseDependencies, databaseMonitors ) );

        buildTransactionMonitor( kernelTransactions, clock, config );

        final KernelImpl kernel = new KernelImpl( kernelTransactions, hooks, databaseHealth, transactionMonitor, procedures,
                config, storageEngine );

        kernel.registerTransactionHook( transactionEventHandlers );
        life.add( kernel );

        final DatabaseFileListing fileListing = new DatabaseFileListing( databaseLayout, logFiles, labelScanStore, indexingService, storageEngine );
        databaseDependencies.satisfyDependency( fileListing );

        return new DatabaseKernelModule( transactionCommitProcess, kernel, kernelTransactions, fileListing );
    }

    private AtomicReference<CpuClock> setupCpuClockAtomicReference()
    {
        AtomicReference<CpuClock> cpuClock = new AtomicReference<>( CpuClock.NOT_AVAILABLE );
        SettingChangeListener<Boolean> cpuClockUpdater = ( before, after ) ->
        {
            if ( after )
            {
                cpuClock.set( CpuClock.CPU_CLOCK );
            }
            else
            {
                cpuClock.set( CpuClock.NOT_AVAILABLE );
            }
        };
        cpuClockUpdater.accept( null, config.get( GraphDatabaseSettings.track_query_cpu_time ) );
        config.registerDynamicUpdateListener( GraphDatabaseSettings.track_query_cpu_time, cpuClockUpdater );
        return cpuClock;
    }

    private AtomicReference<HeapAllocation> setupHeapAllocationAtomicReference()
    {
        AtomicReference<HeapAllocation> heapAllocation = new AtomicReference<>( HeapAllocation.NOT_AVAILABLE );
        SettingChangeListener<Boolean> heapAllocationUpdater = ( before, after ) ->
        {
            if ( after )
            {
                heapAllocation.set( HeapAllocation.HEAP_ALLOCATION );
            }
            else
            {
                heapAllocation.set( HeapAllocation.NOT_AVAILABLE );
            }
        };
        heapAllocationUpdater.accept( null, config.get( GraphDatabaseSettings.track_query_allocation ) );
        config.registerDynamicUpdateListener( GraphDatabaseSettings.track_query_allocation, heapAllocationUpdater );
        return heapAllocation;
    }

    private void buildTransactionMonitor( KernelTransactions kernelTransactions, Clock clock, Config config )
    {
        KernelTransactionMonitor kernelTransactionTimeoutMonitor = new KernelTransactionMonitor( kernelTransactions, clock, logService );
        databaseDependencies.satisfyDependency( kernelTransactionTimeoutMonitor );
        KernelTransactionMonitorScheduler transactionMonitorScheduler =
                new KernelTransactionMonitorScheduler( kernelTransactionTimeoutMonitor, scheduler,
                        config.get( GraphDatabaseSettings.transaction_monitor_check_interval ).toMillis() );
        life.add( transactionMonitorScheduler );
    }

    @Override
    public synchronized void stop()
    {
        if ( !started )
        {
            return;
        }

        life.stop();
        awaitAllClosingTransactions();
        life.shutdown();
        eventHandlers.shutdown();
        started = false;
    }

    public void drop()
    {
        prepareForDrop();
        stop();
        dropDatabaseFiles();
    }

    private void dropDatabaseFiles()
    {
        try
        {
            fs.deleteRecursively( databaseLayout.databaseDirectory() );
            fs.deleteRecursively( databaseLayout.getTransactionLogsDirectory() );
        }
        catch ( IOException e )
        {
            logService.getInternalLog( Database.class ).warn( format( "Failed to remove dropped database '%s' files.", databaseName ), e );
            throw new UncheckedIOException( e );
        }
    }

    private void awaitAllClosingTransactions()
    {
        KernelTransactions kernelTransactions = kernelModule.kernelTransactions();
        kernelTransactions.terminateTransactions();

        while ( kernelTransactions.haveClosingTransaction() )
        {
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
        }
    }

    public StoreId getStoreId()
    {
        return storageEngine.getStoreId();
    }

    public DatabaseLayout getDatabaseLayout()
    {
        return databaseLayout;
    }

    public Monitors getMonitors()
    {
        return databaseMonitors;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public QueryExecutionEngine getExecutionEngine()
    {
        return executionEngine;
    }

    public InwardKernel getKernel()
    {
        return kernelModule.kernelAPI();
    }

    public ResourceIterator<StoreFileMetadata> listStoreFiles( boolean includeLogs ) throws IOException
    {
        DatabaseFileListing.StoreFileListingBuilder fileListingBuilder = getDatabaseFileListing().builder();
        if ( !includeLogs )
        {
            fileListingBuilder.excludeLogFiles();
        }
        return fileListingBuilder.build();
    }

    public DatabaseFileListing getDatabaseFileListing()
    {
        return kernelModule.fileListing();
    }

    public Dependencies getDependencyResolver()
    {
        return databaseDependencies;
    }

    public JobScheduler getScheduler()
    {
        return scheduler;
    }

    private StatementOperationParts buildStatementOperations( AtomicReference<CpuClock> cpuClockRef,
            AtomicReference<HeapAllocation> heapAllocationRef )
    {
        QueryRegistrationOperations queryRegistrationOperations =
                new StackingQueryRegistrationOperations( clock, cpuClockRef, heapAllocationRef );

        return new StatementOperationParts( queryRegistrationOperations );
    }

    public StoreCopyCheckPointMutex getStoreCopyCheckPointMutex()
    {
        return storeCopyCheckPointMutex;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public TokenHolders getTokenHolders()
    {
        return tokenHolders;
    }

    public DatabaseEventHandlers getEventHandlers()
    {
        return eventHandlers;
    }

    public TransactionEventHandlers getTransactionEventHandlers()
    {
        return transactionEventHandlers;
    }

    public DatabaseAvailabilityGuard getDatabaseAvailabilityGuard()
    {
        return databaseAvailabilityGuard;
    }

    private void prepareForDrop()
    {
        for ( PagedFile pagedFile : databasePageCache.listExistingMappings() )
        {
            pagedFile.setDeleteOnClose( true );
        }
        checkpointerLifecycle.setCheckpointOnShutdown( false );
    }

    @VisibleForTesting
    public LifeSupport getLife()
    {
        return life;
    }

    public static Iterable<StorageIndexReference> initialSchemaRulesLoader( StorageEngine storageEngine )
    {
        return () ->
        {
            try ( StorageReader reader = storageEngine.newReader() )
            {
                return asList( reader.indexesGetAll() ).iterator();
            }
        };
    }
}
