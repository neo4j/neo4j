/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.database;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.function.ThrowingAction.executeAll;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.id.BufferingIdGeneratorFactory.PAGED_ID_BUFFER_FILE_NAME;
import static org.neo4j.internal.schema.IndexType.LOOKUP;
import static org.neo4j.kernel.extension.ExtensionFailureStrategies.fail;
import static org.neo4j.kernel.impl.transaction.log.TransactionAppenderFactory.createTransactionAppender;
import static org.neo4j.kernel.recovery.Recovery.context;
import static org.neo4j.kernel.recovery.Recovery.validateStoreId;
import static org.neo4j.scheduler.Group.INDEX_CLEANUP;
import static org.neo4j.scheduler.Group.INDEX_CLEANUP_WORK;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.dbms.database.DatabasePageCache;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.internal.gbptree.GroupingRecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.Upgrade;
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
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.OldestTransactionIdFactory;
import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.io.pagecache.context.TransactionIdSnapshotFactory;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.DefaultElementIdMapperV1;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SpdKernelTransactionDecorator;
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
import org.neo4j.kernel.impl.api.CommandCommitListeners;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.ExternalIdReuseConditionProvider;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionIdSequence;
import org.neo4j.kernel.impl.api.TransactionRegistry;
import org.neo4j.kernel.impl.api.TransactionVisibilityProvider;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitor;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitorScheduler;
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.factory.FacadeKernelTransactionFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.impl.index.DatabaseIndexStats;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.locking.multiversion.MultiVersionLockManager;
import org.neo4j.kernel.impl.pagecache.IOControllerService;
import org.neo4j.kernel.impl.pagecache.PageCacheLifecycle;
import org.neo4j.kernel.impl.pagecache.VersionStorageFactory;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionExecutionMonitor;
import org.neo4j.kernel.impl.store.StoreFileListing;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.UnableToMigrateException;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.LoggingLogFileMonitor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointScheduler;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckpointerLifecycle;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.DetachedLogTailScanner;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruningImpl;
import org.neo4j.kernel.impl.transaction.log.reverse.ReverseTransactionCursorLoggingMonitor;
import org.neo4j.kernel.impl.transaction.log.reverse.ReversedSingleFileCommandBatchCursor;
import org.neo4j.kernel.impl.transaction.state.StaticIndexProviderMapFactory;
import org.neo4j.kernel.impl.transaction.state.storeview.FullScanStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.IndexStoreViewFactory;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.internal.locker.FileLockerService;
import org.neo4j.kernel.internal.locker.LockerLifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.recovery.LogTailExtractor;
import org.neo4j.kernel.recovery.LoggingLogTailScannerMonitor;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.kernel.recovery.RecoveryPredicate;
import org.neo4j.kernel.recovery.RecoveryStartupChecker;
import org.neo4j.lock.LockService;
import org.neo4j.lock.ReentrantLockService;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ScopedMemoryPool;
import org.neo4j.monitoring.Monitors;
import org.neo4j.resources.CpuClock;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.enrichment.ApplyEnrichmentStrategy;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.ElementIdMapper;

public class Database extends AbstractDatabase {
    private static final String STORE_ID_VALIDATOR_TAG = "storeIdValidator";

    private final ServerIdentity serverIdentity;
    private final PageCache globalPageCache;

    private final TokenHolders tokenHolders;
    private final GlobalTransactionEventListeners transactionEventListeners;
    private final IdGeneratorFactory idGeneratorFactory;
    private final LockService lockService;
    private final FileSystemAbstraction fs;
    private final DatabaseTransactionStats transactionStats;
    private final DatabaseIndexStats indexStats;
    private final CommitProcessFactory commitProcessFactory;
    private final ConstraintSemantics constraintSemantics;
    private final GlobalProcedures globalProcedures;
    private final IOControllerService ioControllerService;
    private final SystemNanoClock clock;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final CollectionsFactorySupplier collectionsFactorySupplier;
    private final DatabaseTracers tracers;
    private final AccessCapabilityFactory accessCapabilityFactory;
    private final LeaseService leaseService;
    private final ExternalIdReuseConditionProvider externalIdReuseConditionProvider;
    private final StorageEngineFactorySupplier storageEngineFactorySupplier;

    private TransactionIdSequence transactionIdSequence;
    private IndexProviderMap indexProviderMap;
    private final DatabaseReadOnlyChecker readOnlyDatabaseChecker;
    private final IdController idController;
    private final DbmsInfo dbmsInfo;
    private final HostedOnMode mode;
    private MetadataCache metadataCache;
    private StorageEngineFactory storageEngineFactory;
    private LockManager databaseLockManager;
    private DatabaseLayout databaseLayout;
    private StorageEngine storageEngine;
    private QueryExecutionEngine executionEngine;
    private DatabaseKernelModule kernelModule;
    private final Iterable<ExtensionFactory<?>> extensionFactories;
    private final Function<DatabaseLayout, DatabaseLayoutWatcher> watcherServiceFactory;
    private final QueryEngineProvider engineProvider;
    private DatabasePageCache databasePageCache;
    private CheckpointerLifecycle checkpointerLifecycle;
    private ScopedMemoryPool otherDatabasePool;
    private final GraphDatabaseFacade databaseFacade;
    private final FileLockerService fileLockerService;
    private final KernelTransactionFactory kernelTransactionFactory;
    private final DatabaseStartupController startupController;
    private final GlobalMemoryGroupTracker transactionsMemoryPool;
    private final GlobalMemoryGroupTracker otherMemoryPool;
    private final CursorContextFactory cursorContextFactory;
    private final VersionStorageFactory versionStorageFactory;
    private final CommandCommitListeners commandCommitListeners;
    private final SpdKernelTransactionDecorator spdKernelTransactionDecorator;
    private MemoryTracker otherDatabaseMemoryTracker;
    private RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private DatabaseAvailability databaseAvailability;
    private DatabaseTransactionEventListeners databaseTransactionEventListeners;
    private IOController ioController;
    private ElementIdMapper elementIdMapper;
    private boolean storageExists;
    private TransactionCommitmentFactory commitmentFactory;
    private VersionStorage versionStorage;

    public Database(DatabaseCreationContext context) {
        super(
                context.getGlobalDependencies(),
                context.getNamedDatabaseId(),
                context.getDatabaseConfig(),
                context.getDatabaseEventListeners(),
                context.getMonitors(),
                context.getDatabaseLogService(),
                context.getScheduler(),
                context.getDatabaseAvailabilityGuardFactory(),
                context.getDatabaseHealthFactory());
        this.serverIdentity = context.getServerIdentity();
        this.databaseLayout = context.getDatabaseLayout();
        this.idGeneratorFactory = context.getIdGeneratorFactory();
        this.transactionsMemoryPool = context.getTransactionsMemoryPool();
        this.otherMemoryPool = context.getOtherMemoryPool();
        this.storeCopyCheckPointMutex = context.getStoreCopyCheckPointMutex();
        this.tokenHolders = context.getTokenHolders();
        this.transactionEventListeners = context.getTransactionEventListeners();
        this.fs = context.getFs();
        this.transactionStats = context.getTransactionStats();
        this.indexStats = context.getIndexStats();
        this.constraintSemantics = context.getConstraintSemantics();
        this.globalProcedures = context.getGlobalProcedures();
        this.ioControllerService = context.getIoControllerService();
        this.clock = context.getClock();
        this.accessCapabilityFactory = context.getAccessCapabilityFactory();

        this.idController = context.getIdController();
        this.dbmsInfo = context.getDbmsInfo();
        this.mode = context.getMode();
        this.cursorContextFactory = context.getContextFactory();
        this.versionStorageFactory = context.getVersionStorageFactory();
        this.extensionFactories = context.getExtensionFactories();
        this.watcherServiceFactory = context.getWatcherServiceFactory();
        this.engineProvider = context.getEngineProvider();
        this.lockService = createLockService(databaseConfig);
        this.commitProcessFactory = context.getCommitProcessFactory();
        this.globalPageCache = context.getPageCache();
        this.collectionsFactorySupplier = context.getCollectionsFactorySupplier();
        this.storageEngineFactorySupplier = context.getStorageEngineFactorySupplier();

        this.databaseFacade = new GraphDatabaseFacade(this, databaseConfig, dbmsInfo, mode, databaseAvailabilityGuard);
        this.kernelTransactionFactory = new FacadeKernelTransactionFactory(databaseConfig, databaseFacade);
        this.tracers = context.getTracers();
        this.fileLockerService = context.getFileLockerService();
        this.leaseService = context.getLeaseService();
        this.startupController = context.getStartupController();
        this.readOnlyDatabaseChecker = context.getDbmsReadOnlyChecker().forDatabase(namedDatabaseId);
        this.externalIdReuseConditionProvider = context.externalIdReuseConditionProvider();
        this.commandCommitListeners = context.getCommandCommitListeners();
        this.spdKernelTransactionDecorator = context.getSpdKernelTransactionDecorator();
    }

    /**
     * Initialize the database, and bring it to a state where its version can be examined, and it can be
     * upgraded if necessary.
     */
    @Override
    protected void specificInit() throws IOException {
        this.storageEngineFactory = storageEngineFactorySupplier.create();
        var storageLockManager = storageEngineFactory.createLockManager(databaseConfig, this.clock);
        this.databaseLockManager = isNotMultiVersioned(databaseConfig)
                ? storageLockManager
                : new MultiVersionLockManager(storageLockManager);
        this.databaseLayout = storageEngineFactory.formatSpecificDatabaseLayout(databaseLayout);
        new DatabaseDirectoriesCreator(fs, databaseLayout).createDirectories();
        ioController = ioControllerService.createIOController(databaseConfig, clock);
        transactionIdSequence = new TransactionIdSequence();
        this.versionStorage = versionStorageFactory.createVersionStorage(
                globalPageCache,
                ioController,
                scheduler,
                internalLogProvider,
                databaseDependencies,
                tracers,
                databaseLayout,
                databaseConfig);
        databasePageCache = new DatabasePageCache(globalPageCache, ioController, versionStorage, databaseConfig);

        life.add(versionStorage);
        life.add(onShutdown(() -> databaseLockManager.close()));
        life.add(new LockerLifecycleAdapter(fileLockerService.createDatabaseLocker(fs, databaseLayout)));
        life.add(databaseConfig);

        databaseAvailability = new DatabaseAvailability(
                databaseAvailabilityGuard, transactionStats, clock, getAwaitActiveTransactionDeadlineMillis());

        databaseDependencies.satisfyDependency(ioController);
        databaseDependencies.satisfyDependency(transactionIdSequence);
        databaseDependencies.satisfyDependency(readOnlyDatabaseChecker);
        databaseDependencies.satisfyDependency(databaseLayout);
        databaseDependencies.satisfyDependency(startupController);
        databaseDependencies.satisfyDependency(databasePageCache);
        databaseDependencies.satisfyDependency(versionStorage);
        databaseDependencies.satisfyDependency(tokenHolders);
        databaseDependencies.satisfyDependency(databaseFacade);
        databaseDependencies.satisfyDependency(kernelTransactionFactory);
        databaseDependencies.satisfyDependency(storeCopyCheckPointMutex);
        databaseDependencies.satisfyDependency(transactionStats);
        databaseDependencies.satisfyDependency(indexStats);
        databaseDependencies.satisfyDependency(databaseLockManager);
        databaseDependencies.satisfyDependency(databaseAvailability);
        databaseDependencies.satisfyDependency(idGeneratorFactory);
        databaseDependencies.satisfyDependency(idController);
        databaseDependencies.satisfyDependency(lockService);
        databaseDependencies.satisfyDependency(cursorContextFactory);
        databaseDependencies.satisfyDependency(tracers);
        databaseDependencies.satisfyDependency(tracers.getDatabaseTracer());
        databaseDependencies.satisfyDependency(tracers.getPageCacheTracer());
        databaseDependencies.satisfyDependency(storageEngineFactory);
        databaseDependencies.satisfyDependencies(mode);

        recoveryCleanupWorkCollector = life.add(new GroupingRecoveryCleanupWorkCollector(
                scheduler, INDEX_CLEANUP, INDEX_CLEANUP_WORK, databaseLayout.getDatabaseName()));
        databaseDependencies.satisfyDependency(recoveryCleanupWorkCollector);

        // Memory tracking
        otherDatabasePool = otherMemoryPool.newDatabasePool(namedDatabaseId.name(), 0, null);
        life.add(onShutdown(() -> otherDatabasePool.close()));
        otherDatabaseMemoryTracker = otherDatabasePool.getPoolMemoryTracker();
        databaseDependencies.satisfyDependency(new DatabaseMemoryTrackers(otherDatabaseMemoryTracker));

        life.add(onShutdown(versionStorage::close));
        life.add(new PageCacheLifecycle(databasePageCache));
        life.add(initializeExtensions(databaseDependencies));
        life.add(initializeIndexProviderMap(databaseDependencies));

        DatabaseLayoutWatcher watcherService = watcherServiceFactory.apply(databaseLayout);
        life.add(watcherService);
        databaseDependencies.satisfyDependency(watcherService);

        // The CatalogManager has to update the dependency on TransactionIdStore when the system database is started
        // Note: CatalogManager does not exist in community edition if we use the new query router stack
        if (this.isSystem() && databaseDependencies.containsDependency(AbstractCatalogManager.class)) {
            var catalogManager = databaseDependencies.resolveDependency(AbstractCatalogManager.class);
            life.add(catalogManager);
        }
    }

    /**
     * Start the database and make it ready for transaction processing.
     * A database will automatically recover itself, if necessary, when started.
     * If the store files are obsolete (older than oldest supported version), then start will throw an exception.
     */
    @Override
    protected void specificStart() throws IOException {

        databaseMonitors.addMonitorListener(new LoggingLogFileMonitor(internalLog));
        databaseMonitors.addMonitorListener(
                new LoggingLogTailScannerMonitor(internalLogProvider.getLog(DetachedLogTailScanner.class)));
        databaseMonitors.addMonitorListener(new ReverseTransactionCursorLoggingMonitor(
                internalLogProvider.getLog(ReversedSingleFileCommandBatchCursor.class)));
        databaseMonitors.addMonitorListener(indexStats);

        // Upgrade the store before we begin
        upgradeStore(databaseConfig, databasePageCache, otherDatabaseMemoryTracker);

        // Check the tail of transaction logs and validate version
        LogTailMetadata tailMetadata = getLogTail();
        long lastClosedTxId = tailMetadata.getLastCommittedTransaction().id();
        initialiseContextFactory(() -> new TransactionIdSnapshot(lastClosedTxId), () -> lastClosedTxId);

        storageExists = storageEngineFactory.storageExists(fs, databaseLayout);
        validateStoreAndTxLogs(tailMetadata, cursorContextFactory, storageExists);

        if (Recovery.performRecovery(context(
                        fs,
                        globalPageCache,
                        tracers,
                        databaseConfig,
                        databaseLayout,
                        otherDatabaseMemoryTracker,
                        ioController,
                        internalLogProvider,
                        tailMetadata)
                .recoveryPredicate(RecoveryPredicate.ALL)
                .monitors(databaseMonitors)
                .extensionFactories(extensionFactories)
                .startupChecker(new RecoveryStartupChecker(startupController, namedDatabaseId))
                .clock(clock))) {
            // recovery replayed logs and wrote some checkpoints as result we need to rescan log tail to get the
            // latest info
            tailMetadata = getLogTail();
            long recoveredTxId = tailMetadata.getLastCommittedTransaction().id();
            initialiseContextFactory(() -> new TransactionIdSnapshot(recoveredTxId), () -> recoveredTxId);
        }

        metadataCache = databaseDependencies.satisfyDependency(new MetadataCache(tailMetadata));

        // Build all modules and their services
        DatabaseSchemaState databaseSchemaState = new DatabaseSchemaState(internalLogProvider);

        idController.initialize(
                fs,
                databaseLayout.file(PAGED_ID_BUFFER_FILE_NAME),
                databaseConfig,
                () -> kernelModule.kernelTransactions().get(),
                s -> kernelModule.kernelTransactions().eligibleForFreeing(s),
                otherDatabaseMemoryTracker,
                readOnlyDatabaseChecker);

        storageEngine = storageEngineFactory.instantiate(
                fs,
                clock,
                databaseLayout,
                databaseConfig,
                databasePageCache,
                tokenHolders,
                databaseSchemaState,
                constraintSemantics,
                indexProviderMap,
                lockService,
                idGeneratorFactory,
                databaseHealth,
                internalLogProvider,
                userLogProvider,
                recoveryCleanupWorkCollector,
                tailMetadata,
                metadataCache,
                otherDatabaseMemoryTracker,
                cursorContextFactory,
                tracers.getPageCacheTracer(),
                versionStorage);

        var metadataProvider = databaseDependencies.satisfyDependency(storageEngine.metadataProvider());

        initialiseContextFactory(
                getTransactionIdSnapshotFactory(databaseConfig, metadataProvider),
                getOldestTransactionIdFactory(databaseConfig, () -> kernelModule));
        elementIdMapper = new DefaultElementIdMapperV1(namedDatabaseId);

        // Recreate the logFiles after storage engine to get access to dependencies
        var logFiles = getLogFiles();

        life.add(storageEngine);
        life.add(storageEngine.schemaAndTokensLifecycle());
        life.add(logFiles);

        // Token indexes
        FullScanStoreView fullScanStoreView =
                new FullScanStoreView(lockService, storageEngine, databaseConfig, scheduler);
        var indexStoreViewLocks = storageEngine.indexingBehaviour().requireCoordinationLocks()
                ? lockService
                : LockService.NO_LOCK_SERVICE;
        IndexStoreViewFactory indexStoreViewFactory = new IndexStoreViewFactory(
                databaseConfig,
                storageEngine,
                databaseLockManager,
                fullScanStoreView,
                indexStoreViewLocks,
                internalLogProvider);

        // Schema indexes
        IndexStatisticsStore indexStatisticsStore = new IndexStatisticsStore(
                databasePageCache,
                fs,
                databaseLayout,
                recoveryCleanupWorkCollector,
                false,
                cursorContextFactory,
                tracers.getPageCacheTracer(),
                storageEngine.getOpenOptions());
        life.add(indexStatisticsStore);

        IndexingService indexingService = buildIndexingService(
                storageEngine,
                databaseSchemaState,
                indexStoreViewFactory,
                indexStatisticsStore,
                otherDatabaseMemoryTracker,
                metadataCache);

        databaseDependencies.satisfyDependency(storageEngine.countsAccessor());

        CheckPointerImpl.ForceOperation forceOperation =
                new DefaultForceOperation(indexingService, storageEngine, databasePageCache);
        DatabaseTransactionLogModule transactionLogModule = buildTransactionLogs(
                logFiles,
                databaseConfig,
                internalLogProvider,
                scheduler,
                forceOperation,
                metadataProvider,
                databaseMonitors,
                databaseDependencies,
                cursorContextFactory,
                storageEngineFactory.commandReaderFactory());
        commitmentFactory = new TransactionCommitmentFactory(metadataProvider);

        databaseTransactionEventListeners =
                new DatabaseTransactionEventListeners(databaseFacade, transactionEventListeners, namedDatabaseId);
        life.add(databaseTransactionEventListeners);
        final DatabaseKernelModule kernelModule = buildKernel(
                logFiles,
                transactionLogModule,
                indexingService,
                databaseSchemaState,
                storageEngine,
                metadataProvider,
                metadataCache,
                databaseAvailabilityGuard,
                clock,
                indexStatisticsStore,
                leaseService,
                cursorContextFactory);

        kernelModule.satisfyDependencies(databaseDependencies);

        // Do these assignments last so that we can ensure no cyclical dependencies exist
        this.kernelModule = kernelModule;

        databaseDependencies.satisfyDependency(commitmentFactory);
        databaseDependencies.satisfyDependency(databaseSchemaState);
        databaseDependencies.satisfyDependency(storageEngine);
        databaseDependencies.satisfyDependency(indexingService);
        databaseDependencies.satisfyDependency(indexStoreViewFactory);
        databaseDependencies.satisfyDependency(indexStatisticsStore);
        databaseDependencies.satisfyDependency(indexProviderMap);
        databaseDependencies.satisfyDependency(forceOperation);
        databaseDependencies.satisfyDependency(storageEngine.storeEntityCounters());
        databaseDependencies.satisfyDependency(elementIdMapper);

        var providerSpi = QueryEngineProvider.spi(
                internalLogProvider, databaseMonitors, scheduler, life, getKernel(), databaseConfig);
        this.executionEngine = QueryEngineProvider.initialize(
                databaseDependencies, databaseFacade, engineProvider, isSystem(), providerSpi);

        this.checkpointerLifecycle = new CheckpointerLifecycle(transactionLogModule.checkPointer(), databaseHealth);

        life.add(idController);
        life.add(onStart(this::registerUpgradeListener));
        life.add(databaseHealth);
        life.add(databaseAvailabilityGuard);
        life.add(databaseAvailability);
        life.setLast(checkpointerLifecycle);
        life.add(onStop(() -> this.executionEngine.clearQueryCaches()));

        databaseDependencies.resolveDependency(DbmsDiagnosticsManager.class).dumpDatabaseDiagnostics(this);
    }

    @Override
    protected void specificStop() {
        databaseConfig.removeListener(GraphDatabaseSettings.track_query_cpu_time, cpuChangeListener);
    }

    @Override
    protected void specificShutdown() {
        // no specific actions
    }

    private void initialiseContextFactory(
            TransactionIdSnapshotFactory transactionIdSnapshotFactory,
            OldestTransactionIdFactory oldestTransactionIdFactory) {
        cursorContextFactory.init(transactionIdSnapshotFactory, oldestTransactionIdFactory);
    }

    @Override
    protected void postStartupInit() throws Exception {
        if (!storageExists) {
            if (databaseConfig.get(GraphDatabaseInternalSettings.skip_default_indexes_on_creation)) {
                return;
            }
            try (var tx = kernelModule
                    .kernelAPI()
                    .beginTransaction(KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED)) {
                createLookupIndex(tx, EntityType.NODE);
                createLookupIndex(tx, EntityType.RELATIONSHIP);
                tx.commit();
            }
            checkpointAfterStartupInit();
        }
    }

    private void checkpointAfterStartupInit() throws IOException {
        var checkPointer = databaseDependencies.resolveDependency(CheckPointerImpl.class);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("Database init completed."));
    }

    private void createLookupIndex(KernelTransaction tx, EntityType entityType) throws KernelException {
        var descriptor = SchemaDescriptors.forAnyEntityTokens(entityType);

        IndexPrototype prototype = IndexPrototype.forSchema(descriptor)
                .withIndexType(LOOKUP)
                .withIndexProvider(indexProviderMap.getTokenIndexProvider().getProviderDescriptor());
        prototype = prototype.withName(SchemaNameUtil.generateName(prototype, new String[] {}, new String[] {}));

        tx.schemaWrite().indexCreate(prototype);
    }

    private LogTailMetadata getLogTail() throws IOException {
        return getLogFiles().getTailMetadata();
    }

    private LogFiles getLogFiles() throws IOException {
        return LogFilesBuilder.builder(
                        databaseLayout,
                        fs,
                        new DbmsRuntimeFallbackKernelVersionProvider(
                                databaseDependencies, databaseLayout.getDatabaseName(), databaseConfig))
                .withConfig(databaseConfig)
                .withDependencies(databaseDependencies)
                .withLogProvider(internalLogProvider)
                .withDatabaseTracers(tracers)
                .withMemoryTracker(otherDatabaseMemoryTracker)
                .withMonitors(databaseMonitors)
                .withClock(clock)
                .withStorageEngineFactory(storageEngineFactory)
                .build();
    }

    private void registerUpgradeListener() {
        DatabaseUpgradeTransactionHandler handler = new DatabaseUpgradeTransactionHandler(
                globalDependencies.resolveDependency(DbmsRuntimeVersionProvider.class),
                metadataCache,
                databaseTransactionEventListeners,
                UpgradeLocker.DEFAULT,
                internalLogProvider,
                databaseConfig,
                kernelModule.kernelAPI());

        handler.registerUpgradeListener((fromKernelVersion, toKernelVersion, tx) ->
                tx.upgrade().upgradeKernel(new Upgrade.KernelUpgrade(fromKernelVersion, toKernelVersion)));
    }

    private void validateStoreAndTxLogs(
            LogTailMetadata logTail, CursorContextFactory contextFactory, boolean storageExists) throws IOException {
        if (storageExists) {
            checkStoreId(logTail, contextFactory);
        } else {
            validateLogsAndStoreAbsence(logTail);
        }
    }

    private void validateLogsAndStoreAbsence(LogTailMetadata logTail) {
        if (!logTail.logsMissing()) {
            throw new RuntimeException(format(
                    "Fail to start '%s' since transaction logs were found, while database " + "files are missing.",
                    namedDatabaseId));
        }
    }

    private void checkStoreId(LogTailMetadata tailMetadata, CursorContextFactory contextFactory) throws IOException {
        try (var cursorContext = contextFactory.create(STORE_ID_VALIDATOR_TAG)) {
            validateStoreId(
                    tailMetadata,
                    storageEngineFactory.retrieveStoreId(fs, databaseLayout, databasePageCache, cursorContext));
        }
    }

    private LifeSupport initializeExtensions(Dependencies dependencies) {
        LifeSupport extensionsLife = new LifeSupport();

        extensionsLife.add(new DatabaseExtensions(
                new DatabaseExtensionContext(databaseLayout, dbmsInfo, dependencies),
                extensionFactories,
                dependencies,
                fail()));

        extensionsLife.init();
        return extensionsLife;
    }

    private Lifecycle initializeIndexProviderMap(Dependencies dependencies) {
        var indexProvidersLife = new LifeSupport();

        var indexProviderMap = StaticIndexProviderMapFactory.create(
                indexProvidersLife,
                databaseConfig,
                databasePageCache,
                fs,
                databaseLogService,
                databaseMonitors,
                readOnlyDatabaseChecker,
                mode,
                recoveryCleanupWorkCollector,
                databaseLayout,
                tokenHolders,
                scheduler,
                cursorContextFactory,
                tracers.getPageCacheTracer(),
                dependencies);
        this.indexProviderMap = indexProvidersLife.add(indexProviderMap);
        dependencies.satisfyDependency(this.indexProviderMap);
        // fulltextadapter for FulltextProcedures
        dependencies.satisfyDependency(
                new DefaultFulltextAdapter((FulltextIndexProvider) this.indexProviderMap.getFulltextProvider()));
        indexProvidersLife.init();
        return indexProvidersLife;
    }

    /**
     * A database can be upgraded <em>after</em> it has been {@link #init() initialized},
     * and <em>before</em> it is {@link #start() started}.
     */
    private void upgradeStore(
            DatabaseConfig databaseConfig, DatabasePageCache databasePageCache, MemoryTracker memoryTracker)
            throws IOException {
        IndexProviderMap indexProviderMap = databaseDependencies.resolveDependency(IndexProviderMap.class);
        var logTailSupplier = Suppliers.lazySingleton(() -> {
            try {
                return new LogTailExtractor(fs, databaseConfig, storageEngineFactory, tracers)
                        .getTailMetadata(
                                databaseLayout,
                                memoryTracker,
                                new DbmsRuntimeFallbackKernelVersionProvider(
                                        databaseDependencies, databaseLayout.getDatabaseName(), databaseConfig));
            } catch (Exception e) {
                throw new UnableToMigrateException("Fail to load log tail during upgrade.", e);
            }
        });
        var storeMigrator = new StoreMigrator(
                fs,
                databaseConfig,
                databaseLogService,
                databasePageCache,
                tracers,
                scheduler,
                databaseLayout,
                storageEngineFactory,
                storageEngineFactory,
                indexProviderMap,
                memoryTracker,
                logTailSupplier);
        storeMigrator.upgradeIfNeeded();
    }

    /**
     * Builds an {@link IndexingService} and adds it to this database's {@link LifeSupport}.
     */
    private IndexingService buildIndexingService(
            StorageEngine storageEngine,
            DatabaseSchemaState databaseSchemaState,
            IndexStoreViewFactory indexStoreViewFactory,
            IndexStatisticsStore indexStatisticsStore,
            MemoryTracker memoryTracker,
            KernelVersionProvider kernelVersionProvider) {
        return life.add(buildIndexingService(
                storageEngine,
                databaseSchemaState,
                indexStoreViewFactory,
                indexStatisticsStore,
                indexStats,
                databaseConfig,
                scheduler,
                indexProviderMap,
                tokenHolders,
                internalLogProvider,
                databaseMonitors.newMonitor(IndexMonitor.class),
                cursorContextFactory,
                memoryTracker,
                namedDatabaseId.name(),
                readOnlyDatabaseChecker,
                clock,
                kernelVersionProvider,
                fs,
                new KernelTransactionVisibilityProvider()));
    }

    /**
     * Convenience method for building am {@link IndexingService}. Doesn't add it to a {@link LifeSupport}.
     */
    public static IndexingService buildIndexingService(
            StorageEngine storageEngine,
            DatabaseSchemaState databaseSchemaState,
            IndexStoreViewFactory indexStoreViewFactory,
            IndexStatisticsStore indexStatisticsStore,
            DatabaseIndexStats indexCounters,
            Config config,
            JobScheduler jobScheduler,
            IndexProviderMap indexProviderMap,
            TokenNameLookup tokenNameLookup,
            InternalLogProvider internalLogProvider,
            IndexMonitor indexMonitor,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            String databaseName,
            DatabaseReadOnlyChecker readOnlyChecker,
            Clock clock,
            KernelVersionProvider kernelVersionProvider,
            FileSystemAbstraction fs,
            TransactionVisibilityProvider transactionVisibilityProvider) {
        IndexingService indexingService = IndexingServiceFactory.createIndexingService(
                storageEngine,
                config,
                jobScheduler,
                indexProviderMap,
                indexStoreViewFactory,
                tokenNameLookup,
                initialSchemaRulesLoader(storageEngine),
                internalLogProvider,
                indexMonitor,
                databaseSchemaState,
                indexStatisticsStore,
                indexCounters,
                contextFactory,
                memoryTracker,
                databaseName,
                readOnlyChecker,
                clock,
                kernelVersionProvider,
                fs,
                transactionVisibilityProvider);
        storageEngine.addIndexUpdateListener(indexingService);
        return indexingService;
    }

    @Override
    public boolean isSystem() {
        return namedDatabaseId.isSystemDatabase();
    }

    private DatabaseTransactionLogModule buildTransactionLogs(
            LogFiles logFiles,
            Config config,
            InternalLogProvider logProvider,
            JobScheduler scheduler,
            CheckPointerImpl.ForceOperation forceOperation,
            MetadataProvider metadataProvider,
            Monitors monitors,
            Dependencies databaseDependencies,
            CursorContextFactory cursorContextFactory,
            CommandReaderFactory commandReaderFactory) {
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache();
        databaseDependencies.satisfyDependencies(transactionMetadataCache);

        Lock pruneLock = new ReentrantLock();
        final LogPruning logPruning =
                new LogPruningImpl(fs, logFiles, logProvider, new LogPruneStrategyFactory(), clock, config, pruneLock);

        var transactionAppender = createTransactionAppender(
                logFiles,
                metadataProvider,
                metadataProvider,
                config,
                databaseHealth,
                scheduler,
                logProvider,
                transactionMetadataCache);
        life.add(transactionAppender);

        final LogicalTransactionStore logicalTransactionStore = new PhysicalLogicalTransactionStore(
                logFiles, transactionMetadataCache, commandReaderFactory, monitors, true, config);

        CheckPointThreshold threshold = CheckPointThreshold.createThreshold(config, clock, logPruning, logProvider);

        var checkpointAppender = logFiles.getCheckpointFile().getCheckpointAppender();
        final CheckPointerImpl checkPointer = new CheckPointerImpl(
                metadataProvider,
                threshold,
                forceOperation,
                logPruning,
                checkpointAppender,
                databaseHealth,
                logProvider,
                tracers,
                storeCopyCheckPointMutex,
                cursorContextFactory,
                clock,
                ioController,
                metadataCache);

        long recurringPeriod = threshold.checkFrequencyMillis();
        CheckPointScheduler checkPointScheduler = new CheckPointScheduler(
                checkPointer, scheduler, recurringPeriod, databaseHealth, namedDatabaseId.name());

        life.add(checkPointer);
        life.add(checkPointScheduler);

        TransactionLogServiceImpl transactionLogService = new TransactionLogServiceImpl(
                metadataProvider,
                logFiles,
                logicalTransactionStore,
                pruneLock,
                databaseAvailabilityGuard,
                logProvider,
                checkPointer,
                commandReaderFactory,
                databaseDependencies.resolveDependency(BinarySupportedKernelVersions.class));
        databaseDependencies.satisfyDependencies(
                checkPointer, logFiles, logicalTransactionStore, transactionAppender, transactionLogService);

        return new DatabaseTransactionLogModule(
                checkPointer, transactionAppender, transactionMetadataCache, logicalTransactionStore);
    }

    private DatabaseKernelModule buildKernel(
            LogFiles logFiles,
            DatabaseTransactionLogModule logsModule,
            IndexingService indexingService,
            DatabaseSchemaState databaseSchemaState,
            StorageEngine storageEngine,
            TransactionIdStore transactionIdStore,
            KernelVersionProvider kernelVersionProvider,
            AvailabilityGuard databaseAvailabilityGuard,
            SystemNanoClock clock,
            IndexStatisticsStore indexStatisticsStore,
            LeaseService leaseService,
            CursorContextFactory cursorContextFactory) {
        AtomicReference<CpuClock> cpuClockRef = setupCpuClockAtomicReference();

        TransactionCommitProcess transactionCommitProcess = commitProcessFactory.create(
                logsModule.transactionAppender(),
                storageEngine,
                readOnlyDatabaseChecker,
                databaseConfig.get(GraphDatabaseInternalSettings.out_of_disk_space_protection),
                commandCommitListeners);
        var transactionValidatorFactory = storageEngine.createTransactionValidatorFactory(databaseConfig);

        /*
         * This is used by explicit indexes and constraint indexes whenever a transaction is to be spawned
         * from within an existing transaction. It smells, and we should look over alternatives when time permits.
         */
        Supplier<Kernel> kernelProvider = () -> kernelModule.kernelAPI();

        ConstraintIndexCreator constraintIndexCreator =
                new ConstraintIndexCreator(kernelProvider, indexingService, internalLogProvider);

        TransactionExecutionMonitor transactionExecutionMonitor =
                getMonitors().newMonitor(TransactionExecutionMonitor.class);
        var transactionIdGenerator = new IdStoreTransactionIdGenerator(transactionIdStore);
        databaseDependencies.satisfyDependency(transactionIdGenerator);

        if (!databaseDependencies.containsDependency(ApplyEnrichmentStrategy.class)) {
            // ensure by default that no enrichment occurs
            databaseDependencies.satisfyDependency(ApplyEnrichmentStrategy.NO_ENRICHMENT);
        }

        KernelTransactions kernelTransactions = life.add(new KernelTransactions(
                databaseConfig,
                databaseLockManager,
                constraintIndexCreator,
                transactionCommitProcess,
                databaseTransactionEventListeners,
                transactionStats,
                databaseAvailabilityGuard,
                storageEngine,
                globalProcedures,
                globalDependencies.resolveDependency(DbmsRuntimeVersionProvider.class),
                transactionIdStore,
                kernelVersionProvider,
                serverIdentity,
                clock,
                cpuClockRef,
                accessCapabilityFactory,
                cursorContextFactory,
                collectionsFactorySupplier,
                constraintSemantics,
                databaseSchemaState,
                tokenHolders,
                elementIdMapper,
                getNamedDatabaseId(),
                indexingService,
                indexStatisticsStore,
                databaseDependencies,
                tracers,
                leaseService,
                transactionsMemoryPool,
                readOnlyDatabaseChecker,
                transactionExecutionMonitor,
                externalIdReuseConditionProvider.get(transactionIdStore, clock),
                commitmentFactory,
                transactionIdSequence,
                transactionIdGenerator,
                databaseHealth,
                logsModule.getLogicalTransactionStore(),
                transactionValidatorFactory,
                internalLogProvider,
                spdKernelTransactionDecorator));

        var transactionMonitor = buildTransactionMonitor(kernelTransactions, databaseConfig);

        KernelImpl kernel = new KernelImpl(
                kernelTransactions,
                databaseHealth,
                transactionStats,
                globalProcedures,
                databaseConfig,
                storageEngine,
                transactionExecutionMonitor);

        life.add(kernel);

        final StoreFileListing fileListing =
                new StoreFileListing(databaseLayout, logFiles, indexingService, storageEngine);
        databaseDependencies.satisfyDependency(fileListing);

        return new DatabaseKernelModule(
                transactionCommitProcess,
                kernel,
                kernelTransactions,
                fileListing,
                transactionMonitor,
                transactionIdGenerator);
    }

    private KernelTransactionMonitor buildTransactionMonitor(KernelTransactions kernelTransactions, Config config) {
        var kernelTransactionMonitor =
                new KernelTransactionMonitor(kernelTransactions, config, clock, databaseLogService);
        databaseDependencies.satisfyDependency(kernelTransactionMonitor);
        TransactionMonitorScheduler transactionMonitorScheduler = new TransactionMonitorScheduler(
                kernelTransactionMonitor,
                scheduler,
                config.get(GraphDatabaseSettings.transaction_monitor_check_interval)
                        .toMillis(),
                namedDatabaseId.name());
        life.add(transactionMonitorScheduler);
        return kernelTransactionMonitor;
    }

    @Override
    protected void safeCleanup() throws Exception {
        executeAll(
                () -> safeLifeShutdown(life),
                () -> safeStorageEngineClose(storageEngine),
                () -> safePoolRelease(otherDatabasePool));
    }

    @Override
    public void prepareToDrop() {
        prepareStop(alwaysTrue());
        checkpointerLifecycle.setCheckpointOnShutdown(false);
    }

    @Override
    protected void deleteDatabaseFilesOnDrop() {
        deleteDatabaseFiles(List.of(
                databaseLayout.databaseDirectory(),
                databaseLayout.getTransactionLogsDirectory(),
                databaseLayout.getScriptDirectory()));
    }

    private void deleteDatabaseFiles(List<Path> files) {
        try {
            for (Path fileToDelete : files) {
                FileSystemUtils.deleteFile(fs, fileToDelete);
            }
        } catch (IOException e) {
            internalLogProvider
                    .getLog(Database.class)
                    .error(format("Failed to delete '%s' files.", namedDatabaseId), e);
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected TransactionRegistry transactionRegistry() {
        return kernelModule.kernelTransactions();
    }

    @Override
    public Config getConfig() {
        return databaseConfig;
    }

    @Override
    public DatabaseLogService getLogService() {
        return databaseLogService;
    }

    @Override
    public DatabaseLogProvider getInternalLogProvider() {
        return internalLogProvider;
    }

    @Override
    public StoreId getStoreId() {
        return storageEngine.retrieveStoreId();
    }

    @Override
    public DatabaseLayout getDatabaseLayout() {
        return databaseLayout;
    }

    @Override
    public QueryExecutionEngine getExecutionEngine() {
        return executionEngine;
    }

    @Override
    public Kernel getKernel() {
        return kernelModule.kernelAPI();
    }

    @Override
    public ResourceIterator<StoreFileMetadata> listStoreFiles(boolean includeLogs) throws IOException {
        StoreFileListing.Builder fileListingBuilder = getStoreFileListing().builder();
        fileListingBuilder.excludeIdFiles();
        if (!includeLogs) {
            fileListingBuilder.excludeLogFiles();
        }
        return fileListingBuilder.build();
    }

    @Override
    public StoreFileListing getStoreFileListing() {
        return kernelModule.fileListing();
    }

    @Override
    public JobScheduler getScheduler() {
        return scheduler;
    }

    @Override
    public StoreCopyCheckPointMutex getStoreCopyCheckPointMutex() {
        return storeCopyCheckPointMutex;
    }

    @Override
    public TokenHolders getTokenHolders() {
        return tokenHolders;
    }

    @Override
    public DatabaseAvailabilityGuard getDatabaseAvailabilityGuard() {
        return databaseAvailabilityGuard;
    }

    @Override
    public GraphDatabaseAPI getDatabaseAPI() {
        return databaseFacade;
    }

    @Override
    public DatabaseTracers getTracers() {
        return tracers;
    }

    @Override
    public MemoryTracker getOtherDatabaseMemoryTracker() {
        return otherDatabaseMemoryTracker;
    }

    @Override
    public StorageEngineFactory getStorageEngineFactory() {
        return storageEngineFactory;
    }

    @Override
    public IOController getIoController() {
        return ioController;
    }

    @Override
    public CursorContextFactory getCursorContextFactory() {
        return cursorContextFactory;
    }

    @Override
    public ElementIdMapper getElementIdMapper() {
        return elementIdMapper;
    }

    public long estimateAvailableReservedSpace() throws IOException {
        return storageEngine.estimateAvailableReservedSpace();
    }

    private void prepareStop(Predicate<PagedFile> deleteFilePredicate) {
        databasePageCache.listExistingMappings().stream()
                .filter(deleteFilePredicate)
                .forEach(file -> file.setDeleteOnClose(true));
    }

    private long getAwaitActiveTransactionDeadlineMillis() {
        return databaseConfig
                .get(GraphDatabaseSettings.shutdown_transaction_end_timeout)
                .toMillis();
    }

    public static Iterable<IndexDescriptor> initialSchemaRulesLoader(StorageEngine storageEngine) {
        return () -> {
            try (StorageReader reader = storageEngine.newReader()) {
                return asList(reader.indexesGetAll()).iterator();
            }
        };
    }

    private static void safeStorageEngineClose(StorageEngine storageEngine) {
        if (storageEngine != null) {
            storageEngine.shutdown();
        }
    }

    private static void safePoolRelease(ScopedMemoryPool pool) {
        if (pool != null) {
            pool.close();
        }
    }

    private static void safeLifeShutdown(LifeSupport life) {
        if (life != null) {
            life.shutdown();
        }
    }

    private static LockService createLockService(DatabaseConfig databaseConfig) {
        return isNotMultiVersioned(databaseConfig) ? new ReentrantLockService() : LockService.NO_LOCK_SERVICE;
    }

    private static TransactionIdSnapshotFactory getTransactionIdSnapshotFactory(
            DatabaseConfig databaseConfig, MetadataProvider metadataProvider) {
        return isNotMultiVersioned(databaseConfig)
                ? (() -> new TransactionIdSnapshot(metadataProvider.getLastClosedTransactionId()))
                : metadataProvider::getClosedTransactionSnapshot;
    }

    private static OldestTransactionIdFactory getOldestTransactionIdFactory(
            DatabaseConfig databaseConfig, Supplier<DatabaseKernelModule> kernelModule) {
        return isNotMultiVersioned(databaseConfig)
                ? OldestTransactionIdFactory.EMPTY_OLDEST_ID_FACTORY
                : (() -> kernelModule.get().transactionMonitor().oldestVisibleClosedTransactionId());
    }

    private static boolean isNotMultiVersioned(DatabaseConfig databaseConfig) {
        return !"multiversion".equals(databaseConfig.get(db_format));
    }

    private class KernelTransactionVisibilityProvider implements TransactionVisibilityProvider {
        @Override
        public long oldestVisibleClosedTransactionId() {
            return kernelModule.transactionMonitor().oldestVisibleClosedTransactionId();
        }

        @Override
        public long oldestObservableHorizon() {
            return kernelModule.transactionMonitor().oldestObservableHorizon();
        }

        @Override
        public long youngestObservableHorizon() {
            return kernelModule.transactionMonitor().youngestObservableHorizon();
        }
    }
}
