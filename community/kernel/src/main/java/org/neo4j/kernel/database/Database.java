/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingChangeListener;
import org.neo4j.configuration.helpers.ReadOnlyDatabaseChecker;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.dbms.database.DatabaseConfig;
import org.neo4j.dbms.database.DatabasePageCache;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailability;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.diagnostics.providers.DbmsDiagnosticsManager;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.scan.FullLabelStream;
import org.neo4j.kernel.impl.api.scan.FullRelationshipTypeStream;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitor;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitorScheduler;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.factory.FacadeKernelTransactionFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.impl.index.schema.FullStoreChangeStream;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.LoggingMonitor;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStore;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.pagecache.PageCacheLifecycle;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.store.stats.DatabaseEntityCounters;
import org.neo4j.kernel.impl.storemigration.DatabaseMigrator;
import org.neo4j.kernel.impl.storemigration.DatabaseMigratorFactory;
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.LoggingLogFileMonitor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointScheduler;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckpointerLifecycle;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.AbstractLogTailScanner;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruningImpl;
import org.neo4j.kernel.impl.transaction.log.reverse.ReverseTransactionCursorLoggingMonitor;
import org.neo4j.kernel.impl.transaction.log.reverse.ReversedSingleFileTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.state.DatabaseFileListing;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.storeview.DynamicIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.internal.locker.FileLockerService;
import org.neo4j.kernel.internal.locker.LockerLifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.kernel.recovery.LoggingLogTailScannerMonitor;
import org.neo4j.kernel.recovery.RecoveryStartupChecker;
import org.neo4j.lock.LockService;
import org.neo4j.lock.ReentrantLockService;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ScopedMemoryPool;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.resources.CpuClock;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.function.ThrowingAction.executeAll;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.database.DatabaseFileHelper.filesToDeleteOnTruncation;
import static org.neo4j.kernel.database.DatabaseFileHelper.filesToKeepOnTruncation;
import static org.neo4j.kernel.extension.ExtensionFailureStrategies.fail;
import static org.neo4j.kernel.impl.index.schema.TokenScanStore.LABEL_SCAN_STORE_MONITOR_TAG;
import static org.neo4j.kernel.impl.index.schema.TokenScanStore.RELATIONSHIP_TYPE_SCAN_STORE_MONITOR_TAG;
import static org.neo4j.kernel.impl.index.schema.TokenScanStore.labelScanStore;
import static org.neo4j.kernel.impl.index.schema.TokenScanStore.toggledRelationshipTypeScanStore;
import static org.neo4j.kernel.impl.transaction.log.rotation.FileLogRotation.transactionLogRotation;
import static org.neo4j.kernel.recovery.Recovery.performRecovery;
import static org.neo4j.kernel.recovery.Recovery.validateStoreId;

public class Database extends LifecycleAdapter
{
    private static final String STORE_ID_VALIDATOR_TAG = "storeIdValidator";
    private final Monitors parentMonitors;
    private final DependencyResolver globalDependencies;
    private final PageCache globalPageCache;

    private final Log msgLog;
    private final DatabaseLogService databaseLogService;
    private final DatabaseLogProvider internalLogProvider;
    private final DatabaseLogProvider userLogProvider;
    private final TokenHolders tokenHolders;
    private final StatementLocksFactory statementLocksFactory;
    private final GlobalTransactionEventListeners transactionEventListeners;
    private final IdGeneratorFactory idGeneratorFactory;
    private final JobScheduler scheduler;
    private final LockService lockService;
    private final FileSystemAbstraction fs;
    private final DatabaseTransactionStats transactionStats;
    private final CommitProcessFactory commitProcessFactory;
    private final ConstraintSemantics constraintSemantics;
    private final GlobalProcedures globalProcedures;
    private final IOLimiter ioLimiter;
    private final SystemNanoClock clock;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final CollectionsFactorySupplier collectionsFactorySupplier;
    private final Locks locks;
    private final DatabaseEventListeners eventListeners;
    private final DatabaseTracers tracers;
    private final AccessCapabilityFactory accessCapabilityFactory;
    private final LeaseService leaseService;

    private Dependencies databaseDependencies;
    private LifeSupport life;
    private IndexProviderMap indexProviderMap;
    private DatabaseHealth databaseHealth;
    private final DatabaseAvailabilityGuard databaseAvailabilityGuard;
    private final DatabaseConfig databaseConfig;
    private final NamedDatabaseId namedDatabaseId;
    private final DatabaseLayout databaseLayout;
    private final boolean readOnly;
    private final ReadOnlyDatabaseChecker readOnlyDatabaseChecker;
    private final IdController idController;
    private final DbmsInfo dbmsInfo;
    private final VersionContextSupplier versionContextSupplier;
    private AccessCapability accessCapability;

    private final StorageEngineFactory storageEngineFactory;
    private StorageEngine storageEngine;
    private QueryExecutionEngine executionEngine;
    private DatabaseKernelModule kernelModule;
    private final Iterable<ExtensionFactory<?>> extensionFactories;
    private final Function<DatabaseLayout,DatabaseLayoutWatcher> watcherServiceFactory;
    private final Factory<DatabaseHealth> databaseHealthFactory;
    private final QueryEngineProvider engineProvider;
    private volatile boolean initialized;
    private volatile boolean started;
    private Monitors databaseMonitors;
    private DatabasePageCache databasePageCache;
    private CheckpointerLifecycle checkpointerLifecycle;
    private ScopedMemoryPool otherDatabasePool;
    private final GraphDatabaseFacade databaseFacade;
    private final FileLockerService fileLockerService;
    private final KernelTransactionFactory kernelTransactionFactory;
    private final DatabaseStartupController startupController;
    private final GlobalMemoryGroupTracker transactionsMemoryPool;
    private final GlobalMemoryGroupTracker otherMemoryPool;
    private MemoryTracker otherDatabaseMemoryTracker;
    private RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private DatabaseAvailability databaseAvailability;

    public Database( DatabaseCreationContext context )
    {
        this.namedDatabaseId = context.getNamedDatabaseId();
        this.databaseLayout = context.getDatabaseLayout();
        this.databaseConfig = context.getDatabaseConfig();
        this.idGeneratorFactory = context.getIdGeneratorFactory();
        this.globalDependencies = context.getGlobalDependencies();
        this.scheduler = context.getScheduler();
        this.transactionsMemoryPool = context.getTransactionsMemoryPool();
        this.otherMemoryPool = context.getOtherMemoryPool();
        this.databaseLogService = context.getDatabaseLogService();
        this.storeCopyCheckPointMutex = context.getStoreCopyCheckPointMutex();
        this.internalLogProvider = context.getDatabaseLogService().getInternalLogProvider();
        this.userLogProvider = context.getDatabaseLogService().getUserLogProvider();
        this.tokenHolders = context.getTokenHolders();
        this.locks = context.getLocks();
        this.statementLocksFactory = context.getStatementLocksFactory();
        this.transactionEventListeners = context.getTransactionEventListeners();
        this.fs = context.getFs();
        this.transactionStats = context.getTransactionStats();
        this.databaseHealthFactory = context.getDatabaseHealthFactory();
        this.constraintSemantics = context.getConstraintSemantics();
        this.parentMonitors = context.getMonitors();
        this.globalProcedures = context.getGlobalProcedures();
        this.ioLimiter = context.getIoLimiter();
        this.clock = context.getClock();
        this.eventListeners = context.getDatabaseEventListeners();
        this.accessCapabilityFactory = context.getAccessCapabilityFactory();

        this.readOnly = databaseConfig.get( read_only );
        this.idController = context.getIdController();
        this.dbmsInfo = context.getDbmsInfo();
        this.versionContextSupplier = context.getVersionContextSupplier();
        this.extensionFactories = context.getExtensionFactories();
        this.watcherServiceFactory = context.getWatcherServiceFactory();
        this.engineProvider = context.getEngineProvider();
        this.msgLog = internalLogProvider.getLog( getClass() );
        this.lockService = new ReentrantLockService();
        this.commitProcessFactory = context.getCommitProcessFactory();
        this.globalPageCache = context.getPageCache();
        this.collectionsFactorySupplier = context.getCollectionsFactorySupplier();
        this.storageEngineFactory = context.getStorageEngineFactory();
        long availabilityGuardTimeout = databaseConfig.get( GraphDatabaseInternalSettings.transaction_start_timeout ).toMillis();
        this.databaseAvailabilityGuard = context.getDatabaseAvailabilityGuardFactory().apply( availabilityGuardTimeout );
        this.databaseFacade = new GraphDatabaseFacade( this, databaseConfig, dbmsInfo, databaseAvailabilityGuard );
        this.kernelTransactionFactory = new FacadeKernelTransactionFactory( databaseConfig, databaseFacade );
        this.tracers = new DatabaseTracers( context.getTracers() );
        this.fileLockerService = context.getFileLockerService();
        this.leaseService = context.getLeaseService();
        this.startupController = context.getStartupController();
        this.readOnlyDatabaseChecker = new ReadOnlyDatabaseChecker.Default( databaseConfig );
    }

    /**
     * Initialize the database, and bring it to a state where its version can be examined, and it can be
     * upgraded if necessary.
     */
    @Override
    public synchronized void init()
    {
        if ( initialized )
        {
            return;
        }
        try
        {
            databaseDependencies = new Dependencies( globalDependencies );
            databasePageCache = new DatabasePageCache( globalPageCache, versionContextSupplier, namedDatabaseId.name() );
            databaseMonitors = new Monitors( parentMonitors );

            life = new LifeSupport();
            life.add( new LockerLifecycleAdapter( fileLockerService.createDatabaseLocker( fs, databaseLayout ) ) );
            life.add( databaseConfig );

            databaseHealth = databaseHealthFactory.newInstance();
            accessCapability = accessCapabilityFactory.newAccessCapability( databaseConfig );
            databaseAvailability = new DatabaseAvailability(
                    databaseAvailabilityGuard, transactionStats, clock, getAwaitActiveTransactionDeadlineMillis() );

            databaseDependencies.satisfyDependency( this );
            databaseDependencies.satisfyDependency( startupController );
            databaseDependencies.satisfyDependency( databaseConfig );
            databaseDependencies.satisfyDependency( databaseMonitors );
            databaseDependencies.satisfyDependency( databaseLogService );
            databaseDependencies.satisfyDependency( databasePageCache );
            databaseDependencies.satisfyDependency( tokenHolders );
            databaseDependencies.satisfyDependency( databaseFacade );
            databaseDependencies.satisfyDependency( kernelTransactionFactory );
            databaseDependencies.satisfyDependency( databaseHealth );
            databaseDependencies.satisfyDependency( storeCopyCheckPointMutex );
            databaseDependencies.satisfyDependency( transactionStats );
            databaseDependencies.satisfyDependency( locks );
            databaseDependencies.satisfyDependency( databaseAvailabilityGuard );
            databaseDependencies.satisfyDependency( databaseAvailability );
            databaseDependencies.satisfyDependency( idGeneratorFactory );
            databaseDependencies.satisfyDependency( idController );
            databaseDependencies.satisfyDependency( lockService );
            databaseDependencies.satisfyDependency( versionContextSupplier );
            databaseDependencies.satisfyDependency( tracers.getDatabaseTracer() );

            recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.immediate();
            databaseDependencies.satisfyDependency( recoveryCleanupWorkCollector );

            life.add( new PageCacheLifecycle( databasePageCache ) );
            life.add( initializeExtensions( databaseDependencies ) );

            DatabaseLayoutWatcher watcherService = watcherServiceFactory.apply( databaseLayout );
            life.add( watcherService );
            databaseDependencies.satisfyDependency( watcherService );

            otherDatabasePool = otherMemoryPool.newDatabasePool( namedDatabaseId.name(), 0, null );
            life.add( onShutdown( () -> otherDatabasePool.close() ) );
            otherDatabaseMemoryTracker = otherDatabasePool.getPoolMemoryTracker();

            databaseDependencies.satisfyDependency( new DatabaseMemoryTrackers( otherDatabaseMemoryTracker ) );

            initialized = true;
        }
        catch ( Throwable e )
        {
            handleStartupFailure( e );
        }
    }

    /**
     * Start the database and make it ready for transaction processing.
     * A database will automatically recover itself, if necessary, when started.
     * If the store files are obsolete (older than oldest supported version), then start will throw an exception.
     */
    @Override
    public synchronized void start()
    {
        if ( started )
        {
            return;
        }
        init(); // Ensure we're initialized
        try
        {
            // Upgrade the store before we begin
            upgradeStore( databaseConfig, databasePageCache, otherDatabaseMemoryTracker );

            // Check the tail of transaction logs and validate version
            LogEntryReader logEntryReader = new VersionAwareLogEntryReader( storageEngineFactory.commandReaderFactory() );

            LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fs ).withLogEntryReader( logEntryReader )
                    .withConfig( databaseConfig )
                    .withDependencies( databaseDependencies )
                    .withLogProvider( internalLogProvider )
                    .withDatabaseTracers( tracers )
                    .withMemoryTracker( otherDatabaseMemoryTracker )
                    .withMonitors( databaseMonitors )
                    .withClock( clock )
                    .withCommandReaderFactory( storageEngineFactory.commandReaderFactory() )
                    .build();

            databaseMonitors.addMonitorListener( new LoggingLogFileMonitor( msgLog ) );
            databaseMonitors.addMonitorListener( new LoggingLogTailScannerMonitor( internalLogProvider.getLog( AbstractLogTailScanner.class ) ) );
            databaseMonitors.addMonitorListener(
                    new ReverseTransactionCursorLoggingMonitor( internalLogProvider.getLog( ReversedSingleFileTransactionCursor.class ) ) );

            var pageCacheTracer = tracers.getPageCacheTracer();

            boolean storageExists = storageEngineFactory.storageExists( fs, databaseLayout, databasePageCache );
            validateStoreAndTxLogs( logFiles, pageCacheTracer, storageExists );

            performRecovery( fs, databasePageCache, tracers, databaseConfig, databaseLayout, storageEngineFactory, internalLogProvider, databaseMonitors,
                    extensionFactories, Optional.of( logFiles ), new RecoveryStartupChecker( startupController, namedDatabaseId ),
                    otherDatabaseMemoryTracker, clock );

            // Build all modules and their services
            DatabaseSchemaState databaseSchemaState = new DatabaseSchemaState( internalLogProvider );

            Supplier<IdController.ConditionSnapshot> transactionsSnapshotSupplier = () -> kernelModule.kernelTransactions().get();
            idController.initialize( transactionsSnapshotSupplier );

            storageEngine = storageEngineFactory.instantiate( fs, databaseLayout, databaseConfig, databasePageCache, tokenHolders, databaseSchemaState,
                    constraintSemantics, indexProviderMap, lockService, idGeneratorFactory, idController, databaseHealth, internalLogProvider,
                    recoveryCleanupWorkCollector, pageCacheTracer, !storageExists, otherDatabaseMemoryTracker );

            life.add( storageEngine );
            life.add( storageEngine.schemaAndTokensLifecycle() );
            life.add( logFiles );

            // Token indexes
            NeoStoreIndexStoreView neoStoreIndexStoreView = new NeoStoreIndexStoreView( lockService, storageEngine::newReader );
            LabelScanStore labelScanStore =
                    buildLabelIndex( databasePageCache, recoveryCleanupWorkCollector, storageEngine, neoStoreIndexStoreView, databaseMonitors,
                            pageCacheTracer, otherDatabaseMemoryTracker );
            RelationshipTypeScanStore relationshipTypeScanStore =
                    buildRelationshipTypeIndex( databasePageCache, recoveryCleanupWorkCollector, storageEngine, neoStoreIndexStoreView, databaseMonitors,
                            databaseConfig, otherDatabaseMemoryTracker );

            // Schema indexes
            DynamicIndexStoreView indexStoreView =
                    new DynamicIndexStoreView( neoStoreIndexStoreView, labelScanStore, relationshipTypeScanStore, lockService, storageEngine::newReader,
                            internalLogProvider, databaseConfig );
            IndexStatisticsStore indexStatisticsStore = new IndexStatisticsStore( databasePageCache, databaseLayout, recoveryCleanupWorkCollector,
                    readOnly, pageCacheTracer );
            IndexingService indexingService = buildIndexingService( storageEngine, databaseSchemaState, indexStoreView, indexStatisticsStore,
                    pageCacheTracer, otherDatabaseMemoryTracker );

            MetadataProvider metadataProvider = storageEngine.metadataProvider();
            databaseDependencies.satisfyDependency( metadataProvider );
            databaseDependencies.satisfyDependency( storageEngine.countsAccessor() );

            versionContextSupplier.init( metadataProvider::getLastClosedTransactionId );

            CheckPointerImpl.ForceOperation forceOperation =
                    new DefaultForceOperation( indexingService, labelScanStore, relationshipTypeScanStore, storageEngine );
            DatabaseTransactionLogModule transactionLogModule =
                    buildTransactionLogs( logFiles, databaseConfig, internalLogProvider, scheduler, forceOperation,
                            logEntryReader, metadataProvider, databaseMonitors, databaseDependencies );

            final DatabaseKernelModule kernelModule = buildKernel(
                    logFiles,
                    transactionLogModule.transactionAppender(),
                    indexingService,
                    databaseSchemaState,
                    labelScanStore,
                    relationshipTypeScanStore,
                    storageEngine,
                    metadataProvider,
                    databaseAvailabilityGuard,
                    clock,
                    indexStatisticsStore, databaseFacade, leaseService );

            kernelModule.satisfyDependencies( databaseDependencies );

            // Do these assignments last so that we can ensure no cyclical dependencies exist
            this.kernelModule = kernelModule;

            databaseDependencies.satisfyDependency( databaseSchemaState );
            databaseDependencies.satisfyDependency( logEntryReader );
            databaseDependencies.satisfyDependency( storageEngine );
            databaseDependencies.satisfyDependency( labelScanStore );
            databaseDependencies.satisfyDependency( relationshipTypeScanStore );
            databaseDependencies.satisfyDependency( indexingService );
            databaseDependencies.satisfyDependency( indexStoreView );
            databaseDependencies.satisfyDependency( indexStatisticsStore );
            databaseDependencies.satisfyDependency( indexProviderMap );
            databaseDependencies.satisfyDependency( forceOperation );
            databaseDependencies.satisfyDependency(
                    new DatabaseEntityCounters( this.idGeneratorFactory, databaseDependencies.resolveDependency( CountsAccessor.class ) ) );

            var providerSpi = QueryEngineProvider.spi( internalLogProvider, databaseMonitors, scheduler, life, getKernel(), databaseConfig );
            this.executionEngine = QueryEngineProvider.initialize( databaseDependencies, databaseFacade, engineProvider, isSystem(), providerSpi );

            this.checkpointerLifecycle = new CheckpointerLifecycle( transactionLogModule.checkPointer(), databaseHealth );

            life.add( databaseHealth );
            life.add( databaseAvailabilityGuard );
            life.add( databaseAvailability );
            life.setLast( checkpointerLifecycle );

            databaseDependencies.resolveDependency( DbmsDiagnosticsManager.class ).dumpDatabaseDiagnostics( this );
            life.start();
            eventListeners.databaseStart( namedDatabaseId );

            /*
             * At this point recovery has completed and the database is ready for use. Whatever panic might have
             * happened before has been healed. So we can safely set the kernel health to ok.
             * This right now has any real effect only in the case of internal restarts (for example, after a store copy).
             * Standalone instances will have to be restarted by the user, as is proper for all database panics.
             */
            databaseHealth.healed();
            started = true;
        }
        catch ( Throwable e )
        {
            handleStartupFailure( e );
        }
    }

    private void validateStoreAndTxLogs( LogFiles logFiles, PageCacheTracer pageCacheTracer, boolean storageExists )
            throws IOException
    {
        if ( storageExists )
        {
            checkStoreId( logFiles, pageCacheTracer );
        }
        else
        {
            validateLogsAndStoreAbsence( logFiles );
        }
    }

    private void validateLogsAndStoreAbsence( LogFiles logFiles )
    {
        if ( ArrayUtils.isNotEmpty( logFiles.logFiles() ) )
        {
            throw new RuntimeException( format( "Fail to start '%s' since transaction logs were found, while database " +
                    "files are missing.", namedDatabaseId ) );
        }
    }

    private void handleStartupFailure( Throwable e )
    {
        // Something unexpected happened during startup
        databaseAvailabilityGuard.startupFailure( e );
        msgLog.warn( "Exception occurred while starting the database. Trying to stop already started components.", e );
        try
        {
            safeCleanup();
        }
        catch ( Exception closeException )
        {
            msgLog.error( "Couldn't close database after startup failure", closeException );
        }
        throw new RuntimeException( e );
    }

    private void checkStoreId( LogFiles logFiles, PageCacheTracer pageCacheTracer ) throws IOException
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( STORE_ID_VALIDATOR_TAG ) )
        {
            validateStoreId( logFiles, storageEngineFactory.storeId( databaseLayout, databasePageCache, cursorTracer ), databaseConfig );
        }
    }

    private LifeSupport initializeExtensions( Dependencies dependencies )
    {
        LifeSupport extensionsLife = new LifeSupport();

        extensionsLife.add( new DatabaseExtensions( new DatabaseExtensionContext( databaseLayout, dbmsInfo, dependencies ), extensionFactories,
                dependencies, fail() ) );

        indexProviderMap = extensionsLife.add( new DefaultIndexProviderMap( dependencies, databaseConfig ) );
        dependencies.satisfyDependency( indexProviderMap );
        extensionsLife.init();
        return extensionsLife;
    }

    /**
     * A database can be upgraded <em>after</em> it has been {@link #init() initialized},
     * and <em>before</em> it is {@link #start() started}.
     */
    private void upgradeStore( DatabaseConfig databaseConfig, DatabasePageCache databasePageCache, MemoryTracker memoryTracker ) throws IOException
    {
        createDatabaseMigrator( databaseConfig, databasePageCache, memoryTracker ).migrate( false );
    }

    private DatabaseMigrator createDatabaseMigrator( DatabaseConfig databaseConfig, DatabasePageCache databasePageCache, MemoryTracker memoryTracker )
    {
        var factory = new DatabaseMigratorFactory( fs, databaseConfig, databaseLogService, databasePageCache, scheduler, namedDatabaseId,
                tracers.getPageCacheTracer(), memoryTracker, databaseHealth );
        return factory.createDatabaseMigrator( databaseLayout, storageEngineFactory, databaseDependencies );
    }

    /**
     * Builds an {@link IndexingService} and adds it to this database's {@link LifeSupport}.
     */
    private IndexingService buildIndexingService(
            StorageEngine storageEngine,
            DatabaseSchemaState databaseSchemaState,
            DynamicIndexStoreView indexStoreView,
            IndexStatisticsStore indexStatisticsStore,
            PageCacheTracer pageCacheTracer,
            MemoryTracker memoryTracker )
    {
        return life.add( buildIndexingService( storageEngine, databaseSchemaState, indexStoreView, indexStatisticsStore, databaseConfig, scheduler,
                indexProviderMap, tokenHolders, internalLogProvider, userLogProvider, databaseMonitors.newMonitor( IndexingService.Monitor.class ),
                pageCacheTracer, memoryTracker, namedDatabaseId.name(), readOnly ) );
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
            IndexingService.Monitor indexingServiceMonitor,
            PageCacheTracer pageCacheTracer,
            MemoryTracker memoryTracker,
            String databaseName,
            boolean readOnly )
    {
        IndexingService indexingService = IndexingServiceFactory.createIndexingService( config, jobScheduler, indexProviderMap, indexStoreView,
                tokenNameLookup, initialSchemaRulesLoader( storageEngine ), internalLogProvider, userLogProvider, indexingServiceMonitor,
                databaseSchemaState, indexStatisticsStore, pageCacheTracer, memoryTracker, databaseName, readOnly );
        storageEngine.addIndexUpdateListener( indexingService );
        return indexingService;
    }

    public boolean isSystem()
    {
        return namedDatabaseId.isSystemDatabase();
    }

    /**
     * Builds a {@link LabelScanStore} and adds it to this database's {@link LifeSupport}.
     */
    private LabelScanStore buildLabelIndex( PageCache pageCache, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, StorageEngine storageEngine,
            NeoStoreIndexStoreView neoStoreIndexStoreView, Monitors monitors, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker )
    {
        return life.add( buildLabelIndex( recoveryCleanupWorkCollector, storageEngine, neoStoreIndexStoreView, monitors, internalLogProvider,
                pageCache, databaseLayout, fs, readOnly, databaseConfig, pageCacheTracer, memoryTracker ) );
    }

    /**
     * Builds a {@link RelationshipTypeScanStore} and adds it to this database's {@link LifeSupport}.
     */
    private RelationshipTypeScanStore buildRelationshipTypeIndex(
            PageCache pageCache,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            StorageEngine storageEngine,
            NeoStoreIndexStoreView neoStoreIndexStoreView,
            Monitors monitors,
            Config config,
            MemoryTracker memoryTracker )
    {
        return life.add( buildRelationshipTypeIndex( recoveryCleanupWorkCollector, storageEngine, neoStoreIndexStoreView, monitors, internalLogProvider,
                pageCache, databaseLayout, fs, readOnly, config, tracers.getPageCacheTracer(), memoryTracker ) );
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
            boolean readOnly,
            Config config,
            PageCacheTracer cacheTracer,
            MemoryTracker memoryTracker )
    {
        monitors.addMonitorListener( new LoggingMonitor( logProvider.getLog( LabelScanStore.class ), EntityType.NODE ), LABEL_SCAN_STORE_MONITOR_TAG );
        FullStoreChangeStream labelStream = new FullLabelStream( indexStoreView );
        LabelScanStore labelScanStore = labelScanStore( pageCache, databaseLayout, fs, labelStream, readOnly, monitors, recoveryCleanupWorkCollector,
                config, cacheTracer, memoryTracker );
        storageEngine.addNodeLabelUpdateListener( labelScanStore.updateListener() );
        return labelScanStore;
    }

    /**
     * Convenience method for building a {@link RelationshipTypeScanStore}. Doesn't add it to a {@link LifeSupport}.
     */
    public static RelationshipTypeScanStore buildRelationshipTypeIndex(
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            StorageEngine storageEngine,
            IndexStoreView indexStoreView,
            Monitors monitors,
            LogProvider logProvider,
            PageCache pageCache,
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fs,
            boolean readOnly,
            Config config,
            PageCacheTracer cacheTracer,
            MemoryTracker memoryTracker )
    {
        monitors.addMonitorListener( new LoggingMonitor( logProvider.getLog( RelationshipTypeScanStore.class ), EntityType.RELATIONSHIP ),
                RELATIONSHIP_TYPE_SCAN_STORE_MONITOR_TAG );
        FullStoreChangeStream relationshipTypeStream = new FullRelationshipTypeStream( indexStoreView );
        RelationshipTypeScanStore relationshipTypeScanStore =
                toggledRelationshipTypeScanStore( pageCache, databaseLayout, fs, relationshipTypeStream, readOnly, monitors, recoveryCleanupWorkCollector,
                        config, cacheTracer, memoryTracker );
        storageEngine.addRelationshipTypeUpdateListener( relationshipTypeScanStore.updateListener() );
        return relationshipTypeScanStore;
    }

    private DatabaseTransactionLogModule buildTransactionLogs( LogFiles logFiles, Config config, LogProvider logProvider, JobScheduler scheduler,
            CheckPointerImpl.ForceOperation forceOperation, LogEntryReader logEntryReader, MetadataProvider metadataProvider, Monitors monitors,
            Dependencies databaseDependencies )
    {
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache();

        final LogPruning logPruning =
                new LogPruningImpl( fs, logFiles, logProvider, new LogPruneStrategyFactory(), clock, config );

        final LogRotation logRotation = transactionLogRotation( logFiles, clock, databaseHealth, monitors.newMonitor( LogRotationMonitor.class ) );

        final BatchingTransactionAppender appender = life.add( new BatchingTransactionAppender(
                logFiles, logRotation, transactionMetadataCache, metadataProvider, databaseHealth ) );

        final LogicalTransactionStore logicalTransactionStore =
                new PhysicalLogicalTransactionStore( logFiles, transactionMetadataCache, logEntryReader, monitors, true );

        CheckPointThreshold threshold = CheckPointThreshold.createThreshold( config, clock, logPruning, logProvider );

        var checkpointAppender = logFiles.getCheckpointFile().getCheckpointAppender();
        final CheckPointerImpl checkPointer =
                new CheckPointerImpl( metadataProvider, threshold, forceOperation, logPruning, checkpointAppender, databaseHealth, logProvider,
                        tracers, ioLimiter, storeCopyCheckPointMutex, clock );

        long recurringPeriod = threshold.checkFrequencyMillis();
        CheckPointScheduler checkPointScheduler = new CheckPointScheduler( checkPointer, ioLimiter, scheduler,
                recurringPeriod, databaseHealth, namedDatabaseId.name() );

        life.add( checkPointer );
        life.add( checkPointScheduler );

        databaseDependencies.satisfyDependencies( checkPointer, logFiles, logicalTransactionStore, logRotation, appender );

        return new DatabaseTransactionLogModule( checkPointer, appender );
    }

    private DatabaseKernelModule buildKernel( LogFiles logFiles, TransactionAppender appender,
            IndexingService indexingService, DatabaseSchemaState databaseSchemaState, LabelScanStore labelScanStore,
            RelationshipTypeScanStore relationshipTypeScanStore, StorageEngine storageEngine, TransactionIdStore transactionIdStore,
            AvailabilityGuard databaseAvailabilityGuard, SystemNanoClock clock,
            IndexStatisticsStore indexStatisticsStore, GraphDatabaseFacade facade,
            LeaseService leaseService )
    {
        AtomicReference<CpuClock> cpuClockRef = setupCpuClockAtomicReference();

        TransactionCommitProcess transactionCommitProcess = commitProcessFactory.create( appender, storageEngine, namedDatabaseId, readOnlyDatabaseChecker );

        /*
         * This is used by explicit indexes and constraint indexes whenever a transaction is to be spawned
         * from within an existing transaction. It smells, and we should look over alternatives when time permits.
         */
        Supplier<Kernel> kernelProvider = () -> kernelModule.kernelAPI();

        ConstraintIndexCreator constraintIndexCreator = new ConstraintIndexCreator( kernelProvider, indexingService, internalLogProvider );

        DatabaseTransactionEventListeners databaseTransactionEventListeners =
                new DatabaseTransactionEventListeners( facade, transactionEventListeners, namedDatabaseId );
        KernelTransactions kernelTransactions = life.add(
                new KernelTransactions( databaseConfig, statementLocksFactory, constraintIndexCreator,
                        transactionCommitProcess, databaseTransactionEventListeners, transactionStats,
                        databaseAvailabilityGuard,
                        storageEngine, globalProcedures, transactionIdStore, clock, cpuClockRef,
                        accessCapability, versionContextSupplier, collectionsFactorySupplier,
                        constraintSemantics, databaseSchemaState, tokenHolders, getNamedDatabaseId(), indexingService, labelScanStore,
                        relationshipTypeScanStore, indexStatisticsStore, databaseDependencies,
                        tracers, leaseService, transactionsMemoryPool, readOnlyDatabaseChecker ) );

        buildTransactionMonitor( kernelTransactions, databaseConfig );

        KernelImpl kernel = new KernelImpl( kernelTransactions, databaseHealth, transactionStats, globalProcedures, databaseConfig, storageEngine );

        life.add( kernel );

        final DatabaseFileListing fileListing =
                new DatabaseFileListing( databaseLayout, logFiles, labelScanStore, relationshipTypeScanStore, indexingService, storageEngine,
                        idGeneratorFactory );
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
        cpuClockUpdater.accept( null, databaseConfig.get( GraphDatabaseSettings.track_query_cpu_time ) );
        databaseConfig.addListener( GraphDatabaseSettings.track_query_cpu_time, cpuClockUpdater );
        return cpuClock;
    }

    private void buildTransactionMonitor( KernelTransactions kernelTransactions, Config config )
    {
        KernelTransactionMonitor kernelTransactionTimeoutMonitor = new KernelTransactionMonitor( kernelTransactions, clock, databaseLogService );
        databaseDependencies.satisfyDependency( kernelTransactionTimeoutMonitor );
        TransactionMonitorScheduler transactionMonitorScheduler =
                new TransactionMonitorScheduler( kernelTransactionTimeoutMonitor, scheduler,
                        config.get( GraphDatabaseSettings.transaction_monitor_check_interval ).toMillis(), namedDatabaseId.name() );
        life.add( transactionMonitorScheduler );
    }

    @Override
    public synchronized void stop()
    {
        if ( !started )
        {
            return;
        }

        eventListeners.databaseShutdown( namedDatabaseId );
        life.stop();
        awaitAllClosingTransactions();
        life.shutdown();
        started = false;
        initialized = false;
    }

    @Override
    public synchronized void shutdown() throws Exception
    {
        safeCleanup();
        started = false;
        initialized = false;
    }

    private void safeCleanup() throws Exception
    {
        executeAll( () -> safeLifeShutdown( life ), () -> safeStorageEngineClose( storageEngine ), () -> safePoolRelease( otherDatabasePool ) );
    }

    public void prepareToDrop()
    {
        prepareStop( alwaysTrue() );
        checkpointerLifecycle.setCheckpointOnShutdown( false );
    }

    public synchronized void drop()
    {
        if ( started )
        {
            prepareToDrop();
            stop();
        }
        deleteDatabaseFiles( List.of( databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory() ) );
    }

    public synchronized void truncate()
    {
        boolean truncateStartedDatabase = started;
        List<Path> filesToKeep = filesToKeepOnTruncation( databaseLayout );
        Path[] transactionLogsFiles = databaseDependencies != null ? databaseDependencies.resolveDependency( LogFiles.class ).logFiles()
                : new TransactionLogFilesHelper( fs, databaseLayout.getTransactionLogsDirectory() ).getMatchedFiles();

        final Path[] transactionLogs = Arrays.stream( transactionLogsFiles ).toArray( Path[]::new );
        if ( truncateStartedDatabase )
        {
            prepareStop( pagedFile -> !filesToKeep.contains( pagedFile.path() ) );
            stop();
        }

        List<Path> filesToDelete = filesToDeleteOnTruncation( filesToKeep, databaseLayout, transactionLogs );
        deleteDatabaseFiles( filesToDelete );
        if ( truncateStartedDatabase )
        {
            start();
        }
    }

    private void deleteDatabaseFiles( List<Path> files )
    {
        try
        {
            for ( Path fileToDelete : files )
            {
                FileSystemUtils.deleteFile( fs, fileToDelete );
            }
        }
        catch ( IOException e )
        {
            internalLogProvider.getLog( Database.class ).error( format( "Failed to delete '%s' files.", namedDatabaseId ), e );
            throw new UncheckedIOException( e );
        }
    }

    public synchronized void upgrade( boolean startAfterUpgrade ) throws IOException
    {
        if ( started )
        {
            stop();
        }

        init();
        DatabaseMigrator migrator = createDatabaseMigrator( databaseConfig, databasePageCache, otherDatabaseMemoryTracker );
        migrator.migrate( true );
        start(); // Start is required to bring the database to a "complete" state (ideally this should not be needed)

        if ( !startAfterUpgrade )
        {
            stop();
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

    public Config getConfig()
    {
        return databaseConfig;
    }

    public DatabaseLogService getLogService()
    {
        return databaseLogService;
    }

    public DatabaseLogProvider getInternalLogProvider()
    {
        return internalLogProvider;
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

    public Kernel getKernel()
    {
        return kernelModule.kernelAPI();
    }

    public ResourceIterator<StoreFileMetadata> listStoreFiles( boolean includeLogs ) throws IOException
    {
        DatabaseFileListing.StoreFileListingBuilder fileListingBuilder = getDatabaseFileListing().builder();
        fileListingBuilder.excludeIdFiles();
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

    public StoreCopyCheckPointMutex getStoreCopyCheckPointMutex()
    {
        return storeCopyCheckPointMutex;
    }

    public NamedDatabaseId getNamedDatabaseId()
    {
        return namedDatabaseId;
    }

    public TokenHolders getTokenHolders()
    {
        return tokenHolders;
    }

    public DatabaseAvailabilityGuard getDatabaseAvailabilityGuard()
    {
        return databaseAvailabilityGuard;
    }

    public GraphDatabaseFacade getDatabaseFacade()
    {
        return databaseFacade;
    }

    public DatabaseHealth getDatabaseHealth()
    {
        return databaseHealth;
    }

    public VersionContextSupplier getVersionContextSupplier()
    {
        return versionContextSupplier;
    }

    public StorageEngineFactory getStorageEngineFactory()
    {
        return storageEngineFactory;
    }

    private void prepareStop( Predicate<PagedFile> deleteFilePredicate )
    {
        databasePageCache.listExistingMappings()
                .stream().filter( deleteFilePredicate )
                .forEach( file -> file.setDeleteOnClose( true ) );
    }

    private long getAwaitActiveTransactionDeadlineMillis()
    {
        return databaseConfig.get( GraphDatabaseSettings.shutdown_transaction_end_timeout ).toMillis();
    }

    @VisibleForTesting
    public LifeSupport getLife()
    {
        return life;
    }

    public static Iterable<IndexDescriptor> initialSchemaRulesLoader( StorageEngine storageEngine )
    {
        return () ->
        {
            try ( StorageReader reader = storageEngine.newReader() )
            {
                return asList( reader.indexesGetAll() ).iterator();
            }
        };
    }

    public boolean isStarted()
    {
        return started;
    }

    private static void safeStorageEngineClose( StorageEngine storageEngine )
    {
        if ( storageEngine != null )
        {
            storageEngine.forceClose();
        }
    }

    private static void safePoolRelease( ScopedMemoryPool pool )
    {
        if ( pool != null )
        {
            pool.close();
        }
    }

    private static void safeLifeShutdown( LifeSupport life )
    {
        if ( life != null )
        {
            life.shutdown();
        }
    }
}
