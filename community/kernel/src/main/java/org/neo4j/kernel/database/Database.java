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
package org.neo4j.kernel.database;

import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingChangeListener;
import org.neo4j.dbms.database.DatabasePageCache;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaNameUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.VersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.database.transaction.TransactionLogServiceImpl;
import org.neo4j.kernel.api.impl.fulltext.DefaultFulltextAdapter;
import org.neo4j.kernel.api.impl.fulltext.FulltextIndexProvider;
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
import org.neo4j.kernel.impl.api.ExternalIdReuseConditionProvider;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitor;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitorScheduler;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.factory.FacadeKernelTransactionFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.pagecache.IOControllerService;
import org.neo4j.kernel.impl.pagecache.PageCacheLifecycle;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionExecutionMonitor;
import org.neo4j.kernel.impl.store.StoreFileListing;
import org.neo4j.kernel.impl.storemigration.DatabaseMigrator;
import org.neo4j.kernel.impl.storemigration.DatabaseMigratorFactory;
import org.neo4j.kernel.impl.transaction.log.LoggingLogFileMonitor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
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
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.AbstractLogTailScanner;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruningImpl;
import org.neo4j.kernel.impl.transaction.log.reverse.ReverseTransactionCursorLoggingMonitor;
import org.neo4j.kernel.impl.transaction.log.reverse.ReversedSingleFileTransactionCursor;
import org.neo4j.kernel.impl.transaction.state.StaticIndexProviderMapFactory;
import org.neo4j.kernel.impl.transaction.state.storeview.FullScanStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.IndexStoreViewFactory;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.internal.locker.FileLockerService;
import org.neo4j.kernel.internal.locker.LockerLifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.kernel.recovery.LoggingLogTailScannerMonitor;
import org.neo4j.kernel.recovery.RecoveryPredicate;
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
import org.neo4j.storageengine.api.KernelVersionRepository;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.function.ThrowingAction.executeAll;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.schema.IndexType.LOOKUP;
import static org.neo4j.kernel.extension.ExtensionFailureStrategies.fail;
import static org.neo4j.kernel.impl.transaction.log.TransactionAppenderFactory.createTransactionAppender;
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
    private final GlobalTransactionEventListeners transactionEventListeners;
    private final IdGeneratorFactory idGeneratorFactory;
    private final JobScheduler scheduler;
    private final LockService lockService;
    private final FileSystemAbstraction fs;
    private final DatabaseTransactionStats transactionStats;
    private final CommitProcessFactory commitProcessFactory;
    private final ConstraintSemantics constraintSemantics;
    private final GlobalProcedures globalProcedures;
    private final IOControllerService ioControllerService;
    private final SystemNanoClock clock;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final CollectionsFactorySupplier collectionsFactorySupplier;
    private final Locks locks;
    private final DatabaseEventListeners eventListeners;
    private final DatabaseTracers tracers;
    private final AccessCapabilityFactory accessCapabilityFactory;
    private final LeaseService leaseService;
    private final ExternalIdReuseConditionProvider externalIdReuseConditionProvider;

    private Dependencies databaseDependencies;
    private LifeSupport life;
    private IndexProviderMap indexProviderMap;
    private DatabaseHealth databaseHealth;
    private final DatabaseAvailabilityGuard databaseAvailabilityGuard;
    private final DatabaseConfig databaseConfig;
    private final NamedDatabaseId namedDatabaseId;
    private final DatabaseLayout databaseLayout;
    private final DatabaseReadOnlyChecker readOnlyDatabaseChecker;
    private final IdController idController;
    private final DbmsInfo dbmsInfo;
    private final VersionContextSupplier versionContextSupplier;

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
    private DatabaseTransactionEventListeners databaseTransactionEventListeners;
    private IOController ioController;

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
        this.transactionEventListeners = context.getTransactionEventListeners();
        this.fs = context.getFs();
        this.transactionStats = context.getTransactionStats();
        this.databaseHealthFactory = context.getDatabaseHealthFactory();
        this.constraintSemantics = context.getConstraintSemantics();
        this.parentMonitors = context.getMonitors();
        this.globalProcedures = context.getGlobalProcedures();
        this.ioControllerService = context.getIoControllerService();
        this.clock = context.getClock();
        this.eventListeners = context.getDatabaseEventListeners();
        this.accessCapabilityFactory = context.getAccessCapabilityFactory();

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
        this.tracers = context.getTracers();
        this.fileLockerService = context.getFileLockerService();
        this.leaseService = context.getLeaseService();
        this.startupController = context.getStartupController();
        this.readOnlyDatabaseChecker = context.getDbmsReadOnlyChecker().forDatabase( namedDatabaseId );
        this.externalIdReuseConditionProvider = context.externalIdReuseConditionProvider();
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
            ioController = ioControllerService.createIOController( databaseConfig, clock );
            databasePageCache = new DatabasePageCache( globalPageCache, ioController );
            databaseMonitors = new Monitors( parentMonitors, internalLogProvider );

            life = new LifeSupport();
            life.add( new LockerLifecycleAdapter( fileLockerService.createDatabaseLocker( fs, databaseLayout ) ) );
            life.add( databaseConfig );

            databaseHealth = databaseHealthFactory.newInstance();
            databaseAvailability = new DatabaseAvailability(
                    databaseAvailabilityGuard, transactionStats, clock, getAwaitActiveTransactionDeadlineMillis() );

            databaseDependencies.satisfyDependency( this );
            databaseDependencies.satisfyDependency( ioController );
            databaseDependencies.satisfyDependency( readOnlyDatabaseChecker );
            databaseDependencies.satisfyDependency( databaseLayout );
            databaseDependencies.satisfyDependency( namedDatabaseId );
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
            databaseDependencies.satisfyDependency( tracers );
            databaseDependencies.satisfyDependency( tracers.getDatabaseTracer() );
            databaseDependencies.satisfyDependency( tracers.getPageCacheTracer() );
            databaseDependencies.satisfyDependency( storageEngineFactory );

            recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.immediate();
            databaseDependencies.satisfyDependency( recoveryCleanupWorkCollector );

            life.add( new PageCacheLifecycle( databasePageCache ) );
            life.add( initializeExtensions( databaseDependencies ) );
            life.add( initializeIndexProviderMap( databaseDependencies ) );

            DatabaseLayoutWatcher watcherService = watcherServiceFactory.apply( databaseLayout );
            life.add( watcherService );
            databaseDependencies.satisfyDependency( watcherService );

            otherDatabasePool = otherMemoryPool.newDatabasePool( namedDatabaseId.name(), 0, null );
            life.add( onShutdown( () -> otherDatabasePool.close() ) );
            otherDatabaseMemoryTracker = otherDatabasePool.getPoolMemoryTracker();

            databaseDependencies.satisfyDependency( new DatabaseMemoryTrackers( otherDatabaseMemoryTracker ) );

            eventListeners.databaseCreate( namedDatabaseId );

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

            LogFiles logFiles = getLogFiles( logEntryReader );

            databaseMonitors.addMonitorListener( new LoggingLogFileMonitor( msgLog ) );
            databaseMonitors.addMonitorListener( new LoggingLogTailScannerMonitor( internalLogProvider.getLog( AbstractLogTailScanner.class ) ) );
            databaseMonitors.addMonitorListener(
                    new ReverseTransactionCursorLoggingMonitor( internalLogProvider.getLog( ReversedSingleFileTransactionCursor.class ) ) );

            var pageCacheTracer = tracers.getPageCacheTracer();

            boolean storageExists = storageEngineFactory.storageExists( fs, databaseLayout, databasePageCache );
            validateStoreAndTxLogs( logFiles, pageCacheTracer, storageExists );

            performRecovery( fs, databasePageCache, tracers, databaseConfig, databaseLayout, storageEngineFactory, false, internalLogProvider, databaseMonitors,
                    extensionFactories, Optional.of( logFiles ), new RecoveryStartupChecker( startupController, namedDatabaseId ),
                    otherDatabaseMemoryTracker, clock, RecoveryPredicate.ALL );

            // Build all modules and their services
            DatabaseSchemaState databaseSchemaState = new DatabaseSchemaState( internalLogProvider );

            idController.initialize( () -> kernelModule.kernelTransactions().get(), otherDatabaseMemoryTracker );

            storageEngine = storageEngineFactory.instantiate( fs, databaseLayout, databaseConfig, databasePageCache, tokenHolders, databaseSchemaState,
                    constraintSemantics, indexProviderMap, lockService, idGeneratorFactory, idController, databaseHealth, internalLogProvider, userLogProvider,
                    recoveryCleanupWorkCollector, pageCacheTracer, !storageExists, readOnlyDatabaseChecker, otherDatabaseMemoryTracker );

            MetadataProvider metadataProvider = storageEngine.metadataProvider();
            databaseDependencies.satisfyDependency( metadataProvider );

            //Recreate the logFiles after storage engine to get access to dependencies
            logFiles = getLogFiles( logEntryReader );

            life.add( storageEngine );
            life.add( storageEngine.schemaAndTokensLifecycle() );
            life.add( logFiles );

            // Token indexes
            FullScanStoreView fullScanStoreView =
                    new FullScanStoreView( lockService, storageEngine::newReader, storageEngine::createStorageCursors, databaseConfig, scheduler );
            IndexStoreViewFactory indexStoreViewFactory =
                    new IndexStoreViewFactory( databaseConfig, storageEngine::createStorageCursors, storageEngine::newReader, locks, fullScanStoreView,
                            lockService, internalLogProvider );

            // Schema indexes
            IndexStatisticsStore indexStatisticsStore = new IndexStatisticsStore( databasePageCache, databaseLayout, recoveryCleanupWorkCollector,
                    readOnlyDatabaseChecker, pageCacheTracer );
            IndexingService indexingService = buildIndexingService( storageEngine, databaseSchemaState, indexStoreViewFactory, indexStatisticsStore,
                    pageCacheTracer, otherDatabaseMemoryTracker );

            databaseDependencies.satisfyDependency( storageEngine.countsAccessor() );

            versionContextSupplier.init( metadataProvider::getLastClosedTransactionId, namedDatabaseId.name() );

            CheckPointerImpl.ForceOperation forceOperation =
                    new DefaultForceOperation( indexingService, storageEngine );
            DatabaseTransactionLogModule transactionLogModule =
                    buildTransactionLogs( logFiles, databaseConfig, internalLogProvider, scheduler, forceOperation,
                            logEntryReader, metadataProvider, databaseMonitors, databaseDependencies );

            databaseTransactionEventListeners = new DatabaseTransactionEventListeners( databaseFacade, transactionEventListeners, namedDatabaseId );
            life.add( databaseTransactionEventListeners );
            final DatabaseKernelModule kernelModule = buildKernel(
                    logFiles,
                    transactionLogModule.transactionAppender(),
                    indexingService,
                    databaseSchemaState,
                    storageEngine,
                    metadataProvider,
                    metadataProvider,
                    databaseAvailabilityGuard,
                    clock,
                    indexStatisticsStore, leaseService );

            kernelModule.satisfyDependencies( databaseDependencies );

            // Do these assignments last so that we can ensure no cyclical dependencies exist
            this.kernelModule = kernelModule;

            databaseDependencies.satisfyDependency( databaseSchemaState );
            databaseDependencies.satisfyDependency( logEntryReader );
            databaseDependencies.satisfyDependency( storageEngine );
            databaseDependencies.satisfyDependency( indexingService );
            databaseDependencies.satisfyDependency( indexStoreViewFactory );
            databaseDependencies.satisfyDependency( indexStatisticsStore );
            databaseDependencies.satisfyDependency( indexProviderMap );
            databaseDependencies.satisfyDependency( forceOperation );
            databaseDependencies.satisfyDependency( storageEngine.storeEntityCounters() );

            var providerSpi = QueryEngineProvider.spi( internalLogProvider, databaseMonitors, scheduler, life, getKernel(), databaseConfig );
            this.executionEngine = QueryEngineProvider.initialize( databaseDependencies, databaseFacade, engineProvider, isSystem(), providerSpi );

            this.checkpointerLifecycle = new CheckpointerLifecycle( transactionLogModule.checkPointer(), databaseHealth, ioController );

            life.add( onStart( this::registerUpgradeListener ) );
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

            postStartupInit( storageExists );
        }
        catch ( Throwable e )
        {
            handleStartupFailure( e );
        }
    }

    private void postStartupInit( boolean storageExists ) throws KernelException
    {
        if ( !storageExists )
        {
            if ( databaseConfig.get( GraphDatabaseInternalSettings.skip_default_indexes_on_creation ) )
            {
                return;
            }
            try ( var tx = kernelModule.kernelAPI().beginTransaction( KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED ) )
            {
                createLookupIndex( tx, EntityType.NODE );
                createLookupIndex( tx, EntityType.RELATIONSHIP );
                tx.commit();
            }
        }
    }

    private void createLookupIndex( KernelTransaction tx, EntityType entityType ) throws KernelException
    {
        var descriptor = SchemaDescriptors.forAnyEntityTokens( entityType );

        IndexPrototype prototype = IndexPrototype.forSchema( descriptor ).withIndexType( LOOKUP )
                                                 .withIndexProvider( indexProviderMap.getTokenIndexProvider().getProviderDescriptor() );
        prototype = prototype.withName( SchemaNameUtil.generateName( prototype, new String[]{}, new String[]{} ) );

        tx.schemaWrite().indexCreate( prototype );
    }

    private LogFiles getLogFiles( LogEntryReader logEntryReader ) throws IOException
    {
        return LogFilesBuilder.builder( databaseLayout, fs ).withLogEntryReader( logEntryReader )
                .withConfig( databaseConfig )
                .withDependencies( databaseDependencies )
                .withLogProvider( internalLogProvider )
                .withDatabaseTracers( tracers )
                .withMemoryTracker( otherDatabaseMemoryTracker )
                .withMonitors( databaseMonitors )
                .withClock( clock )
                .withStorageEngineFactory( storageEngineFactory )
                .build();
    }

    private void registerUpgradeListener()
    {
        DatabaseUpgradeTransactionHandler handler =
                new DatabaseUpgradeTransactionHandler( storageEngine, globalDependencies.resolveDependency( DbmsRuntimeRepository.class ),
                        storageEngine.metadataProvider(), databaseTransactionEventListeners, UpgradeLocker.DEFAULT, internalLogProvider );

        handler.registerUpgradeListener( commands ->
        {
            PhysicalTransactionRepresentation transactionRepresentation =
                    new PhysicalTransactionRepresentation( commands );
            long time = clock.millis();
            transactionRepresentation.setHeader( EMPTY_BYTE_ARRAY, time, storageEngine.metadataProvider().getLastClosedTransactionId(), time,
                    leaseService.newClient().leaseId(), AuthSubject.AUTH_DISABLED );
            try ( var commitEvent = tracers.getDatabaseTracer().beginAsyncCommit();
                  var storeCursors = storageEngine.createStorageCursors( CursorContext.NULL ) )
            {
                TransactionToApply toApply =
                        new TransactionToApply( transactionRepresentation, CursorContext.NULL, storeCursors );

                TransactionCommitProcess commitProcess = databaseDependencies.resolveDependency( TransactionCommitProcess.class );
                commitProcess.commit( toApply, commitEvent, TransactionApplicationMode.INTERNAL );
            }
        } );

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

    private void validateLogsAndStoreAbsence( LogFiles logFiles ) throws IOException
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
            shutdown();
        }
        catch ( Exception closeException )
        {
            msgLog.error( "Couldn't close database after startup failure", closeException );
        }
        throw new RuntimeException( e );
    }

    private void checkStoreId( LogFiles logFiles, PageCacheTracer pageCacheTracer ) throws IOException
    {
        try ( var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( STORE_ID_VALIDATOR_TAG ) ) )
        {
            validateStoreId( logFiles, storageEngineFactory.storeId( fs, databaseLayout, databasePageCache, cursorContext ), databaseConfig );
        }
    }

    private LifeSupport initializeExtensions( Dependencies dependencies )
    {
        LifeSupport extensionsLife = new LifeSupport();

        extensionsLife.add( new DatabaseExtensions( new DatabaseExtensionContext( databaseLayout, dbmsInfo, dependencies ), extensionFactories,
                dependencies, fail() ) );

        extensionsLife.init();
        return extensionsLife;
    }

    private Lifecycle initializeIndexProviderMap( Dependencies dependencies )
    {
        var indexProvidersLife = new LifeSupport();

        var indexProviderMap = StaticIndexProviderMapFactory.create(
                indexProvidersLife, databaseConfig, databasePageCache, fs, databaseLogService, databaseMonitors, readOnlyDatabaseChecker, dbmsInfo,
                recoveryCleanupWorkCollector, tracers.getPageCacheTracer(), databaseLayout, tokenHolders, scheduler, dependencies );
        this.indexProviderMap = indexProvidersLife.add( indexProviderMap );
        dependencies.satisfyDependency( this.indexProviderMap );
        // fulltextadapter for FulltextProcedures
        dependencies.satisfyDependency( new DefaultFulltextAdapter( (FulltextIndexProvider) this.indexProviderMap.getFulltextProvider() ) );
        indexProvidersLife.init();
        return indexProvidersLife;
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
            IndexStoreViewFactory indexStoreViewFactory,
            IndexStatisticsStore indexStatisticsStore,
            PageCacheTracer pageCacheTracer,
            MemoryTracker memoryTracker )
    {
        return life.add( buildIndexingService( storageEngine, databaseSchemaState, indexStoreViewFactory, indexStatisticsStore, databaseConfig, scheduler,
                indexProviderMap, tokenHolders, internalLogProvider, userLogProvider, databaseMonitors.newMonitor( IndexMonitor.class ),
                pageCacheTracer, memoryTracker, namedDatabaseId.name(), readOnlyDatabaseChecker ) );
    }

    /**
     * Convenience method for building am {@link IndexingService}. Doesn't add it to a {@link LifeSupport}.
     */
    public static IndexingService buildIndexingService(
            StorageEngine storageEngine,
            DatabaseSchemaState databaseSchemaState,
            IndexStoreViewFactory indexStoreViewFactory,
            IndexStatisticsStore indexStatisticsStore,
            Config config,
            JobScheduler jobScheduler,
            IndexProviderMap indexProviderMap,
            TokenNameLookup tokenNameLookup,
            LogProvider internalLogProvider,
            LogProvider userLogProvider,
            IndexMonitor indexMonitor,
            PageCacheTracer pageCacheTracer,
            MemoryTracker memoryTracker,
            String databaseName,
            DatabaseReadOnlyChecker readOnlyChecker )
    {
        IndexingService indexingService = IndexingServiceFactory.createIndexingService( config, jobScheduler, indexProviderMap, indexStoreViewFactory,
                tokenNameLookup, initialSchemaRulesLoader( storageEngine ), internalLogProvider, userLogProvider, indexMonitor,
                databaseSchemaState, indexStatisticsStore, pageCacheTracer, memoryTracker, databaseName, readOnlyChecker );
        storageEngine.addIndexUpdateListener( indexingService );
        return indexingService;
    }

    public boolean isSystem()
    {
        return namedDatabaseId.isSystemDatabase();
    }

    private DatabaseTransactionLogModule buildTransactionLogs( LogFiles logFiles, Config config, LogProvider logProvider, JobScheduler scheduler,
            CheckPointerImpl.ForceOperation forceOperation, LogEntryReader logEntryReader, MetadataProvider metadataProvider, Monitors monitors,
            Dependencies databaseDependencies )
    {
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache();

        Lock pruneLock = new ReentrantLock();
        final LogPruning logPruning =
                new LogPruningImpl( fs, logFiles, logProvider, new LogPruneStrategyFactory(), clock, config, pruneLock );

        var transactionAppender =
                createTransactionAppender( logFiles, metadataProvider, transactionMetadataCache, config, databaseHealth, scheduler, logProvider );
        life.add( transactionAppender );

        final LogicalTransactionStore logicalTransactionStore =
                new PhysicalLogicalTransactionStore( logFiles, transactionMetadataCache, logEntryReader, monitors, true, config );

        CheckPointThreshold threshold = CheckPointThreshold.createThreshold( config, clock, logPruning, logProvider );

        var checkpointAppender = logFiles.getCheckpointFile().getCheckpointAppender();
        final CheckPointerImpl checkPointer =
                new CheckPointerImpl( metadataProvider, threshold, forceOperation, logPruning, checkpointAppender, databaseHealth, logProvider,
                        tracers, ioController, storeCopyCheckPointMutex, versionContextSupplier, clock );

        long recurringPeriod = threshold.checkFrequencyMillis();
        CheckPointScheduler checkPointScheduler = new CheckPointScheduler( checkPointer, ioController, scheduler,
                recurringPeriod, databaseHealth, namedDatabaseId.name() );

        life.add( checkPointer );
        life.add( checkPointScheduler );

        TransactionLogServiceImpl transactionLogService =
                new TransactionLogServiceImpl( metadataProvider, logFiles, logicalTransactionStore, pruneLock, databaseAvailabilityGuard );
        databaseDependencies.satisfyDependencies( checkPointer, logFiles, logicalTransactionStore, transactionAppender, transactionLogService );

        return new DatabaseTransactionLogModule( checkPointer, transactionAppender );
    }

    private DatabaseKernelModule buildKernel( LogFiles logFiles, TransactionAppender appender,
            IndexingService indexingService, DatabaseSchemaState databaseSchemaState,
            StorageEngine storageEngine, TransactionIdStore transactionIdStore,
            KernelVersionRepository kernelVersionRepository,
            AvailabilityGuard databaseAvailabilityGuard, SystemNanoClock clock,
            IndexStatisticsStore indexStatisticsStore,
            LeaseService leaseService )
    {
        AtomicReference<CpuClock> cpuClockRef = setupCpuClockAtomicReference();

        TransactionCommitProcess transactionCommitProcess =
                commitProcessFactory.create( appender, storageEngine, namedDatabaseId, readOnlyDatabaseChecker, databaseHealth );

        /*
         * This is used by explicit indexes and constraint indexes whenever a transaction is to be spawned
         * from within an existing transaction. It smells, and we should look over alternatives when time permits.
         */
        Supplier<Kernel> kernelProvider = () -> kernelModule.kernelAPI();

        ConstraintIndexCreator constraintIndexCreator = new ConstraintIndexCreator( kernelProvider, indexingService, internalLogProvider );

        TransactionExecutionMonitor transactionExecutionMonitor = getMonitors().newMonitor( TransactionExecutionMonitor.class );
        KernelTransactions kernelTransactions = life.add(
                new KernelTransactions( databaseConfig, locks, constraintIndexCreator,
                                        transactionCommitProcess, databaseTransactionEventListeners, transactionStats,
                                        databaseAvailabilityGuard, storageEngine, globalProcedures, transactionIdStore,
                                        globalDependencies.resolveDependency( DbmsRuntimeRepository.class ), kernelVersionRepository,
                                        clock, cpuClockRef,
                                        accessCapabilityFactory, versionContextSupplier, collectionsFactorySupplier,
                                        constraintSemantics, databaseSchemaState, tokenHolders, getNamedDatabaseId(), indexingService,
                                        indexStatisticsStore, databaseDependencies,
                                        tracers, leaseService, transactionsMemoryPool, readOnlyDatabaseChecker, transactionExecutionMonitor,
                                        externalIdReuseConditionProvider, internalLogProvider ) );

        buildTransactionMonitor( kernelTransactions, databaseConfig );

        KernelImpl kernel = new KernelImpl( kernelTransactions, databaseHealth, transactionStats, globalProcedures, databaseConfig, storageEngine,
                                            transactionExecutionMonitor );

        life.add( kernel );

        final StoreFileListing fileListing =
                new StoreFileListing( databaseLayout, logFiles, indexingService, storageEngine, idGeneratorFactory );
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
        eventListeners.databaseDrop( namedDatabaseId );
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
        msgLog.info( "Waiting for closing transactions." );
        KernelTransactions kernelTransactions = kernelModule.kernelTransactions();
        kernelTransactions.terminateTransactions();

        while ( kernelTransactions.haveClosingTransaction() )
        {
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
        }
        msgLog.info( "All transactions are closed." );
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
        StoreFileListing.Builder fileListingBuilder = getStoreFileListing().builder();
        fileListingBuilder.excludeIdFiles();
        if ( !includeLogs )
        {
            fileListingBuilder.excludeLogFiles();
        }
        return fileListingBuilder.build();
    }

    public StoreFileListing getStoreFileListing()
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

    public DatabaseTracers getTracers()
    {
        return tracers;
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
