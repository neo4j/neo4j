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
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.function.ThrowingAction.executeAll;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.id.BufferingIdGeneratorFactory.PAGED_ID_BUFFER_FILE_NAME;
import static org.neo4j.internal.schema.IndexType.LOOKUP;
import static org.neo4j.kernel.extension.ExtensionFailureStrategies.fail;
import static org.neo4j.kernel.impl.transaction.log.TransactionAppenderFactory.createTransactionAppender;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.pickLogFormatOnUpgrade;
import static org.neo4j.kernel.recovery.IncompleteTransactionAction.APPLY;
import static org.neo4j.kernel.recovery.IncompleteTransactionAction.ROLLBACK;
import static org.neo4j.kernel.recovery.Recovery.context;
import static org.neo4j.kernel.recovery.Recovery.validateStoreId;
import static org.neo4j.scheduler.Group.INDEX_CLEANUP;
import static org.neo4j.scheduler.Group.INDEX_CLEANUP_WORK;
import static org.neo4j.scheduler.Group.STORAGE_MAINTENANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.io.Closeable;
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
import org.neo4j.common.Subject;
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
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.Upgrade;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.OldestVisibilityHorizonFactory;
import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.io.pagecache.context.TransactionIdSnapshotFactory;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.io.pagecache.prefetch.PagePrefetcher;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.DatabaseCreationOptions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.database.transaction.TransactionLogServiceImpl;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.MultiVersionRollbackAvailabilityService;
import org.neo4j.kernel.diagnostics.providers.DbmsDiagnosticsManager;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.api.ChunkedTransactionTracker;
import org.neo4j.kernel.impl.api.CommandCommitListeners;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.ExternalIdReuseConditionProvider;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.KernelTransactionsFactory;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionIdSequence;
import org.neo4j.kernel.impl.api.TransactionVisibilityProvider;
import org.neo4j.kernel.impl.api.TransactionalProcessFactory;
import org.neo4j.kernel.impl.api.TransactionsFactory;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitor;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitorScheduler;
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
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
import org.neo4j.kernel.impl.storemigration.StoreVersionStateChecker;
import org.neo4j.kernel.impl.storemigration.UnableToMigrateException;
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.LoggingLogFileMonitor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.RecoveryOutcome;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointScheduler;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckpointerLifecycle;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.RangeLogVersionVisitor;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.DetachedLogTailScanner;
import org.neo4j.kernel.impl.transaction.log.pruning.CheckpointOnlyLogPruning;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruningImpl;
import org.neo4j.kernel.impl.transaction.log.reverse.ReverseTransactionCursorLoggingMonitor;
import org.neo4j.kernel.impl.transaction.log.reverse.ReversedSingleFileCommandBatchCursor;
import org.neo4j.kernel.impl.transaction.state.StaticIndexProviderMapFactory;
import org.neo4j.kernel.impl.transaction.state.storeview.FullScanStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.IndexStoreViewFactory;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
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
import org.neo4j.kernel.recovery.RecoveryResult;
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
import org.neo4j.storageengine.OperationMode;
import org.neo4j.storageengine.VectorStoreCreator;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.DeprecatedFormatWarning;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.ReadableStorageEngine;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.enrichment.ApplyEnrichmentStrategy;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.DefaultElementIdMapperV1;
import org.neo4j.values.ElementIdMapper;

public class Database extends AbstractDatabase {
    private static final String STORE_ID_VALIDATOR_TAG = "storeIdValidator";
    private static final String ID_CACHE_CLUSTER_CLEANUP_TAG = "idCacheClusterCleanup";

    private final ServerIdentity serverIdentity;
    private final PageCache globalPageCache;

    private final TokenHolders tokenHolders;
    private final GlobalTransactionEventListeners transactionEventListeners;
    private final CursorContextFactorySupplier cursorContextFactorySupplier;
    private IdGeneratorFactory idGeneratorFactory;
    private final IdContextFactory idContextFactory;
    private final IdGeneratorSettings idGeneratorSettings;
    private LockService lockService;
    private final FileSystemAbstraction fs;
    private final DatabaseTransactionStats transactionStats;
    private final DatabaseIndexStats indexStats;
    private final TransactionalProcessFactory commitProcessFactory;
    private final ConstraintSemantics constraintSemantics;
    private final GlobalProcedures globalProcedures;
    private final IOControllerService ioControllerService;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final DatabaseTracers tracers;
    private final AccessCapabilityFactory accessCapabilityFactory;
    private final LeaseService leaseService;
    private final ExternalIdReuseConditionProvider externalIdReuseConditionProvider;
    private final StorageEngineFactorySupplier storageEngineFactorySupplier;
    private final KernelTransactionsFactory kernelTransactionsFactory;
    private final PagePrefetcher pagePrefetcher;
    private final DatabaseCreationOptions databaseCreationOptions;
    private final LogPruneStrategyFactory logPruneStrategyFactory;

    private TransactionIdSequence transactionIdSequence;
    private IndexProviderMap indexProviderMap;
    private final DatabaseReadOnlyChecker readOnlyDatabaseChecker;
    private IdController idController;
    private final DbmsInfo dbmsInfo;
    private final HostedOnMode mode;
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
    private CursorContextFactory cursorContextFactory;
    private final VersionStorageFactory versionStorageFactory;
    private final CommandCommitListeners commandCommitListeners;
    private final VectorStoreCreator vectorStoreCreator;
    private MemoryTracker otherDatabaseMemoryTracker;
    private RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private DatabaseTransactionEventListeners databaseTransactionEventListeners;
    private IOController ioController;
    private ElementIdMapper elementIdMapper;
    private boolean storageExists;
    private TransactionCommitmentFactory commitmentFactory;
    private VersionStorage versionStorage;
    private LeaseMonitor leaseMonitor;
    private final ChunkedTransactionTracker chunkedTransactionTracker;
    private MultiVersionDatabaseRollbackService multiVersionDatabaseRollbackService;
    private volatile RecoveryPredicateSupplier recoveryPredicate = RecoveryPredicateSupplier.ALL;
    private final boolean raftTriggersUpgrade;

    public Database(DatabaseCreationContext context) {
        super(
                context.getGlobalDependencies(),
                context.getNamedDatabaseId(),
                context.getDatabaseConfig(),
                context.getDatabaseEventListeners(),
                context.getDatabaseMonitorsFactory(),
                context.getDatabaseLogService(),
                context.getScheduler(),
                context.getDatabaseAvailabilityGuardFactory(),
                context.getDatabaseHealthFactory(),
                context.getClock(),
                context.getExceptionHandlerService());
        this.serverIdentity = context.getServerIdentity();
        this.databaseLayout = context.getDatabaseLayout();
        this.idContextFactory = context.idContextFactory();
        this.idGeneratorSettings = context.idGeneratorSettings();
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
        this.accessCapabilityFactory = context.getAccessCapabilityFactory();
        this.dbmsInfo = context.getDbmsInfo();
        this.mode = context.getMode();
        this.cursorContextFactorySupplier = context.getCursorContextFactorySupplier();
        this.versionStorageFactory = context.getVersionStorageFactory();
        this.extensionFactories = context.getExtensionFactories();
        this.watcherServiceFactory = context.getWatcherServiceFactory();
        this.engineProvider = context.getEngineProvider();
        this.commitProcessFactory = context.getCommitProcessFactory();
        this.globalPageCache = context.getPageCache();
        this.storageEngineFactorySupplier = context.getStorageEngineFactorySupplier();
        TransactionsFactory transactionsFactory = context.getTransactionsFactory();
        this.databaseFacade = new GraphDatabaseFacade(
                this, databaseConfig, dbmsInfo, mode, transactionsFactory.mode(), databaseAvailabilityGuard);
        this.kernelTransactionFactory = new FacadeKernelTransactionFactory(databaseConfig, databaseFacade);
        this.tracers = context.getTracers();
        this.fileLockerService = context.getFileLockerService();
        this.leaseService = context.getLeaseService();
        this.startupController = context.getStartupController();
        this.readOnlyDatabaseChecker = context.getDbmsReadOnlyChecker().forDatabase(namedDatabaseId);
        this.externalIdReuseConditionProvider = context.externalIdReuseConditionProvider();
        this.commandCommitListeners = context.getCommandCommitListeners();
        this.kernelTransactionsFactory = transactionsFactory.kernelTransactionsFactory();
        this.pagePrefetcher = context.getPagePrefetcher();
        this.vectorStoreCreator = context.getVectorStoreCreator();
        this.databaseCreationOptions = context.getDatabaseCreationOptions();
        this.logPruneStrategyFactory = context.logPruneStrategyFactory();
        this.chunkedTransactionTracker = new ChunkedTransactionTracker();
        this.raftTriggersUpgrade = context.raftTriggersUpgrade();
    }

    /**
     * Initialize the database, and bring it to a state where its version can be examined, and it can be
     * upgraded if necessary.
     */
    @Override
    protected void specificInit() throws IOException {
        this.storageEngineFactory = storageEngineFactorySupplier.create();
        boolean multiVersioned = storageEngineFactory.multiVersioned();
        this.cursorContextFactory = cursorContextFactorySupplier.create(multiVersioned);
        var storageLockManager = storageEngineFactory.createLockManager(databaseConfig, this.clock, transactionStats);
        this.databaseLockManager =
                multiVersioned ? new MultiVersionLockManager(storageLockManager) : storageLockManager;
        this.lockService = createLockService(storageEngineFactory, getNamedDatabaseId());
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
                databaseConfig,
                multiVersioned);

        databasePageCache = new DatabasePageCache(globalPageCache, ioController, versionStorage, databaseConfig);
        DatabaseIdContext databaseIdContext = idContextFactory.createIdContext(
                namedDatabaseId, cursorContextFactory, databaseConfig, idGeneratorSettings, multiVersioned);
        this.idController = databaseIdContext.getIdController();
        this.idGeneratorFactory = databaseIdContext.getIdGeneratorFactory();
        this.leaseMonitor = getMonitors().newMonitor(LeaseMonitor.class);
        life.add(onShutdown(() -> databaseLockManager.close()));
        life.add(new LockerLifecycleAdapter(fileLockerService.createDatabaseLocker(fs, databaseLayout)));
        life.add(databaseConfig);

        databaseDependencies.satisfyDependencies(chunkedTransactionTracker);
        databaseDependencies.satisfyDependency(databaseCreationOptions);
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
        databaseDependencies.satisfyDependency(idGeneratorFactory);
        databaseDependencies.satisfyDependency(idController);
        databaseDependencies.satisfyDependency(lockService);
        databaseDependencies.satisfyDependency(cursorContextFactory);
        databaseDependencies.satisfyDependency(tracers);
        databaseDependencies.satisfyDependency(tracers.getDatabaseTracer());
        databaseDependencies.satisfyDependency(tracers.getPageCacheTracer());
        databaseDependencies.satisfyDependency(storageEngineFactory);
        databaseDependencies.satisfyDependencyIfAbsent(mode);
        databaseDependencies.satisfyDependencies(commandCommitListeners);

        recoveryCleanupWorkCollector = life.add(new GroupingRecoveryCleanupWorkCollector(
                scheduler, INDEX_CLEANUP, INDEX_CLEANUP_WORK, databaseLayout.getDatabaseName()));
        databaseDependencies.satisfyDependency(recoveryCleanupWorkCollector);

        // Memory tracking
        otherDatabasePool = otherMemoryPool.newDatabasePool(namedDatabaseId.name(), 0, null);
        life.add(onShutdown(() -> otherDatabasePool.close()));
        otherDatabaseMemoryTracker = otherDatabasePool.getPoolMemoryTracker();
        databaseDependencies.satisfyDependency(new DatabaseMemoryTrackers(otherDatabaseMemoryTracker));

        life.add(new PageCacheLifecycle(databasePageCache));
        life.add(versionStorage);
        life.add(initializeExtensions(databaseDependencies));

        DatabaseLayoutWatcher watcherService = watcherServiceFactory.apply(databaseLayout);
        life.add(watcherService);
        databaseDependencies.satisfyDependency(watcherService);

        // The CatalogManager has to update the dependency on TransactionIdStore when the system database is started
        // Note: CatalogManager does not exist in community edition if we use the new query router stack
        if (this.isSystem()) {
            var optionalCatalogManager = databaseDependencies.resolveOptionalDependency(AbstractCatalogManager.class);
            optionalCatalogManager.ifPresent(c -> life.add(c));
        }
    }

    /**
     * Start the database and make it ready for transaction processing.
     * A database will automatically recover itself, if necessary, when started.
     * If the store files are obsolete (older than the oldest supported version), then start will throw an exception.
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
        checkVersionSupportedAndNoBlockingInterruptedMigration(
                databaseConfig, databasePageCache, otherDatabaseMemoryTracker);

        // Check the tail of transaction logs and validate version
        LogFiles logFiles = getLogFiles(RecoveryOutcome.EMPTY_OUTCOME);
        LogTailMetadata tailMetadata = logFiles.getTailMetadata();
        long lastClosedTxId = tailMetadata.getLastCommittedTransaction().id();
        initialiseContextFactory(() -> new TransactionIdSnapshot(lastClosedTxId), () -> lastClosedTxId);

        storageExists = storageEngineFactory.storageExists(fs, databaseLayout);
        validateStoreAndTxLogs(tailMetadata, cursorContextFactory, storageExists);

        RecoveryResult recoveryResult = Recovery.performRecovery(context(
                        fs,
                        globalPageCache,
                        tracers,
                        databaseConfig,
                        databaseLayout,
                        otherDatabaseMemoryTracker,
                        ioController,
                        internalLogProvider,
                        tailMetadata)
                .recoveryPredicate(recoveryPredicate.get())
                .monitors(databaseMonitors)
                .extensionFactories(extensionFactories)
                .rollbackRegistry(chunkedTransactionTracker)
                .incompleteTransactionAction(HostedOnMode.SINGLE == mode ? ROLLBACK : APPLY)
                .startupChecker(new RecoveryStartupChecker(startupController, namedDatabaseId))
                .clock(clock));
        if (recoveryResult.recoveryPerformed()) {
            // recovery replayed logs and wrote some checkpoints as result we need to rescan log tail to get the
            // latest info
            logFiles = getLogFiles(recoveryResult.outcome());
            tailMetadata = logFiles.getTailMetadata();

            RecoveryOutcome recoveryOutcome = recoveryResult.outcome();
            long recoveredTxId;
            long[] notClosedTransactionIds;
            long highestEverObserved;
            if (recoveryOutcome.isEmpty()) {
                notClosedTransactionIds = EMPTY_LONG_ARRAY;
                recoveredTxId = tailMetadata.getLastCommittedTransaction().id();
                highestEverObserved = recoveredTxId;
            } else {
                recoveredTxId = recoveryOutcome.lastClosedGapFree().number();
                notClosedTransactionIds = recoveryOutcome.notClosedTransactionIds();
                highestEverObserved =
                        recoveryOutcome.lastCommittingTransactionId().id();
            }
            initialiseContextFactory(
                    () -> new TransactionIdSnapshot(recoveredTxId, highestEverObserved, notClosedTransactionIds),
                    () -> recoveredTxId);
        }

        LogMetadataProvider logMetadataProvider =
                databaseDependencies.satisfyDependency(logFiles.logMetadataProvider());
        internalLog.info("Current KernelVersion=" + logMetadataProvider.kernelVersion() + ", LogFormat= "
                + logMetadataProvider.getCurrentLogFormat());

        // Creating the IndexProviderMap resolves the KernelVersionProvider from the dependencies
        // so it has to happen here at the earliest.
        life.add(initializeIndexProviderMap(databaseDependencies));

        // Build all modules and their services
        DatabaseSchemaState databaseSchemaState = new DatabaseSchemaState(internalLogProvider);

        idController.initialize(
                fs,
                databaseLayout.file(PAGED_ID_BUFFER_FILE_NAME),
                databaseConfig,
                () -> kernelModule.kernelTransactions().get(),
                new IdControllerVisibilityBoundary(),
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
                scheduler,
                internalLogProvider,
                userLogProvider,
                recoveryCleanupWorkCollector,
                logMetadataProvider,
                otherDatabaseMemoryTracker,
                cursorContextFactory,
                tracers.getPageCacheTracer(),
                versionStorage,
                pagePrefetcher,
                databaseDependencies,
                exceptionHandlerService,
                OperationMode.DEFAULT,
                vectorStoreCreator,
                databaseCreationOptions,
                mode == HostedOnMode.RAFT || mode == HostedOnMode.REPLICA);
        // Satisfy the StoreIdProvider needed by logFiles. Logfiles doesn't need it until started by lifecycle.
        databaseDependencies.satisfyDependency(storageEngine.metadataProvider());

        initialiseContextFactory(
                getTransactionIdSnapshotFactory(storageEngineFactory, logMetadataProvider, getNamedDatabaseId()),
                getOldestVisibilityHorizonFactory(storageEngineFactory, () -> kernelModule, getNamedDatabaseId()));
        elementIdMapper = new DefaultElementIdMapperV1(namedDatabaseId);

        life.add(storageEngine);
        life.add(storageEngine.schemaAndTokensLifecycle(
                databaseConfig.get(GraphDatabaseInternalSettings.ignore_corrupt_schema)));
        life.add(logFiles);
        LogFiles logfilesForLambda = logFiles;
        life.add(onStart(() -> {
            long lowestLogVersion =
                    logfilesForLambda.getLogFile().getLogRangeInfo().lowestVersion();
            if (lowestLogVersion != RangeLogVersionVisitor.UNKNOWN) {
                var header = logfilesForLambda.getLogFile().extractHeader(lowestLogVersion);
                if (header != null) {
                    logMetadataProvider.setLowestAvailableCommittedTransactionId(header.getLastAppendIndex() + 1);
                }
            }
        }));

        // Token indexes
        var indexStoreViewLocks = storageEngine.indexingBehaviour().requireCoordinationLocks()
                ? lockService
                : LockService.NO_LOCK_SERVICE;
        FullScanStoreView fullScanStoreView =
                new FullScanStoreView(indexStoreViewLocks, storageEngine, databaseConfig, scheduler);
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
                logMetadataProvider);

        databaseDependencies.satisfyDependency(storageEngine.countsAccessor());

        CheckPointerImpl.ForceOperation forceOperation =
                new DefaultForceOperation(indexingService, storageEngine, databasePageCache);
        boolean isMergeLog =
                databaseConfig.get(GraphDatabaseInternalSettings.merged_log) && !namedDatabaseId.isSystemDatabase();
        DatabaseTransactionLogModule transactionLogModule = buildTransactionLogs(
                logFiles,
                databaseConfig,
                internalLogProvider,
                scheduler,
                forceOperation,
                logMetadataProvider,
                databaseMonitors,
                databaseDependencies,
                cursorContextFactory,
                storageEngineFactory.commandReaderFactory(),
                otherDatabaseMemoryTracker,
                isMergeLog);
        commitmentFactory = new TransactionCommitmentFactory(logMetadataProvider);

        databaseTransactionEventListeners =
                new DatabaseTransactionEventListeners(databaseFacade, transactionEventListeners, namedDatabaseId);
        life.add(databaseTransactionEventListeners);
        life.add(idController);
        final DatabaseKernelModule kernelModule = buildKernel(
                logFiles,
                transactionLogModule,
                indexingService,
                databaseSchemaState,
                storageEngine,
                logMetadataProvider,
                databaseAvailabilityGuard,
                clock,
                indexStatisticsStore,
                leaseService,
                cursorContextFactory);

        life.add(kernelModule.kernelAPI());
        kernelModule.satisfyDependencies(databaseDependencies);

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
                databaseDependencies,
                databaseFacade,
                engineProvider,
                isSystem(),
                providerSpi,
                storageEngineFactory.multiVersioned());

        this.checkpointerLifecycle = new CheckpointerLifecycle(
                transactionLogModule.checkPointer(), databaseHealth, logPruneStrategyFactory.skipOnShutdown());
        this.multiVersionDatabaseRollbackService = new MultiVersionDatabaseRollbackService(
                kernelModule.kernelTransactions(),
                internalLog,
                tracers,
                databaseAvailabilityGuard,
                leaseService,
                chunkedTransactionTracker,
                readOnlyDatabaseChecker,
                databaseHealth,
                kernelModule.getTransactionCommitProcess(),
                transactionIdSequence,
                clock);
        databaseDependencies.satisfyDependency(multiVersionDatabaseRollbackService);

        var rollBackAvailabilityService =
                new MultiVersionRollbackAvailabilityService(databaseAvailabilityGuard, chunkedTransactionTracker);
        databaseDependencies.satisfyDependency(rollBackAvailabilityService);
        life.add(rollBackAvailabilityService);
        life.add(onStop(() -> {
            this.executionEngine.clearQueryCaches();
            this.executionEngine.close();
        }));
        life.add(onStart(() -> registerUpgradeListener(logMetadataProvider)));
        life.add(databaseHealth);

        life.setLast(new DatabaseLifeShutdownCoordinator(
                databaseAvailabilityGuard,
                kernelModule.kernelTransactions(),
                checkpointerLifecycle,
                multiVersionDatabaseRollbackService));

        databaseDependencies.resolveDependency(DbmsDiagnosticsManager.class).dumpDatabaseDiagnostics(this);

        String format = storageEngine.metadataProvider().getStoreId().getFormatName();
        if (storageEngineFactory.isDeprecated(format)) {
            internalLog.warn(DeprecatedFormatWarning.getFormatWarning(databaseLayout.getDatabaseName(), format));
        }
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
            OldestVisibilityHorizonFactory oldestVisibilityHorizonFactory) {
        cursorContextFactory.init(transactionIdSnapshotFactory, oldestVisibilityHorizonFactory);
    }

    @Override
    protected void postStartupInit() throws Exception {
        if (!storageExists) {
            createTokenIndexes();
            return;
        }
        if (checkIfTokenIndexesMissing()) {
            var txIdStore = databaseDependencies.resolveDependency(TransactionIdStore.class);
            long lastCommittedTxId = txIdStore.getLastCommittedTransactionId();

            if (lastCommittedTxId > BASE_TX_ID || mode != HostedOnMode.SINGLE) {
                internalLog.warn("No token lookup indexes found. Token lookup indexes improve the performance of "
                        + "Cypher queries and the population of other indexes. Not having these indexes may lead to "
                        + "severe performance degradation.");
                return;
            }
            internalLog.info("Previous database creation looks incomplete. Creating the missing token lookup indexes to"
                    + " complete database creation.");
            createTokenIndexes();
        }
    }

    public void setRecoveryPredicate(RecoveryPredicateSupplier recoveryPredicate) {
        this.recoveryPredicate = recoveryPredicate;
    }

    private void createTokenIndexes() throws KernelException, IOException {
        try (var tx = kernelModule
                .kernelAPI()
                .beginTransaction(KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED)) {
            createLookupIndex(tx, EntityType.NODE);
            createLookupIndex(tx, EntityType.RELATIONSHIP);
            tx.commit();
        }
        checkpointAfterStartupInit();
    }

    private boolean checkIfTokenIndexesMissing() {
        return Iterables.count(
                        getDependencyResolver()
                                .resolveDependency(IndexingService.class)
                                .getIndexProxies(),
                        proxy -> proxy.getDescriptor().getIndexType() == LOOKUP)
                == 0;
    }

    private void checkpointAfterStartupInit() throws IOException {
        var checkPointer = databaseDependencies.resolveDependency(CheckPointerImpl.class);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("Database init completed."));
    }

    private void createLookupIndex(KernelTransaction tx, EntityType entityType) throws KernelException {
        var descriptor = SchemaDescriptors.forAnyEntityTokens(entityType);
        IndexPrototype prototype = IndexPrototype.forSchema(descriptor).withIndexType(LOOKUP);
        tx.schemaWrite().indexCreate(prototype);
    }

    private LogFiles getLogFiles(RecoveryOutcome recoveryOutcome) throws IOException {
        DbmsRuntimeFallbackKernelVersionProvider kernelVersionProvider = new DbmsRuntimeFallbackKernelVersionProvider(
                databaseDependencies, databaseLayout.getDatabaseName(), databaseConfig);
        return LogFilesBuilder.writeableBuilder(databaseLayout, fs, kernelVersionProvider, kernelVersionProvider)
                .withConfig(databaseConfig)
                .withDependencies(databaseDependencies)
                .withLogProvider(internalLogProvider)
                .withDatabaseTracers(tracers)
                .withMemoryTracker(otherDatabaseMemoryTracker)
                .withMonitors(databaseMonitors)
                .withClock(clock)
                .withStorageEngineFactory(storageEngineFactory)
                .withRecoveryOutcome(recoveryOutcome)
                .withTailReadingMaxPosition(recoveryPredicate.get().maxPosition())
                .build();
    }

    private void registerUpgradeListener(LogMetadataProvider logMetadataProvider) {
        DatabaseUpgradeTransactionHandler handler = new DatabaseUpgradeTransactionHandler(
                globalDependencies.resolveDependency(DbmsRuntimeVersionProvider.class),
                logMetadataProvider,
                logMetadataProvider,
                databaseTransactionEventListeners,
                UpgradeLocker.DEFAULT,
                internalLogProvider,
                databaseConfig,
                kernelModule.kernelAPI(),
                kernelModule.kernelTransactions(),
                isMultiVersioned(storageEngineFactory, namedDatabaseId),
                raftTriggersUpgrade);

        handler.registerUpgradeListener((fromKernelVersion, toKernelVersion, tx, currentLogFormat) -> {
            tx.upgrade()
                    .upgradeKernel(new Upgrade.KernelUpgrade(
                            fromKernelVersion,
                            toKernelVersion,
                            pickLogFormatOnUpgrade(
                                    fromKernelVersion, toKernelVersion, databaseConfig, currentLogFormat)));
        });
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
                    "Fail to start '%s' since transaction logs were found, while database files are missing.",
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
                dependencies.resolveDependency(KernelVersionProvider.class),
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
        indexProvidersLife.init();
        return indexProvidersLife;
    }

    private void checkVersionSupportedAndNoBlockingInterruptedMigration(
            DatabaseConfig databaseConfig, DatabasePageCache databasePageCache, MemoryTracker memoryTracker)
            throws IOException {

        var logTailSupplier = Suppliers.lazySingleton(() -> {
            try {
                return new LogTailExtractor(fs, databaseConfig, storageEngineFactory, tracers)
                        .getTailMetadata(
                                databaseLayout,
                                memoryTracker,
                                new DbmsRuntimeFallbackKernelVersionProvider(
                                        databaseDependencies, databaseLayout.getDatabaseName(), databaseConfig));
            } catch (Exception e) {
                throw new UnableToMigrateException("Fail to load log tail during upgrade check.", e);
            }
        });

        StoreVersionStateChecker.checkVersionSupportedAndNoBlockingInterruptedMigration(
                fs,
                databaseConfig,
                databaseLogService,
                databasePageCache,
                tracers,
                databaseLayout,
                storageEngineFactory,
                memoryTracker,
                logTailSupplier);
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
                elementIdMapper,
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
            ElementIdMapper elementIdMapper,
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
                elementIdMapper,
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
            DatabaseConfig databaseConfig,
            InternalLogProvider logProvider,
            JobScheduler scheduler,
            CheckPointerImpl.ForceOperation forceOperation,
            LogMetadataProvider logMetadataProvider,
            Monitors monitors,
            Dependencies databaseDependencies,
            CursorContextFactory cursorContextFactory,
            CommandReaderFactory commandReaderFactory,
            MemoryTracker memoryTracker,
            boolean isMergedLog) {
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache();
        databaseDependencies.satisfyDependencies(transactionMetadataCache);

        BinarySupportedKernelVersions binarySupportedKernelVersions =
                databaseDependencies.resolveDependency(BinarySupportedKernelVersions.class);
        Lock pruneLock = new ReentrantLock();
        final LogPruning logPruning = isMergedLog && (mode == HostedOnMode.RAFT || mode == HostedOnMode.REPLICA)
                ? new CheckpointOnlyLogPruning(fs, logFiles, logProvider, databaseConfig, pruneLock)
                : new LogPruningImpl(
                        fs,
                        logFiles,
                        logProvider,
                        logPruneStrategyFactory,
                        clock,
                        databaseConfig,
                        pruneLock,
                        logMetadataProvider,
                        commandReaderFactory,
                        binarySupportedKernelVersions,
                        memoryTracker);

        var transactionAppender = createTransactionAppender(
                logFiles,
                logMetadataProvider,
                logMetadataProvider,
                databaseConfig,
                databaseHealth,
                scheduler,
                logProvider,
                transactionMetadataCache,
                namedDatabaseId.name(),
                storageEngineFactory.multiVersioned(),
                isMergedLog && (mode == HostedOnMode.RAFT || mode == HostedOnMode.REPLICA));
        life.add(transactionAppender);

        final LogicalTransactionStore logicalTransactionStore = new PhysicalLogicalTransactionStore(
                logFiles,
                transactionMetadataCache,
                commandReaderFactory,
                monitors,
                true,
                databaseConfig,
                memoryTracker);

        CheckPointThreshold threshold =
                CheckPointThreshold.createThreshold(databaseConfig, clock, logPruning, logProvider);

        final CheckPointerImpl checkPointer = new CheckPointerImpl(
                logMetadataProvider,
                threshold,
                forceOperation,
                logPruning,
                logFiles.getCheckpointFile(),
                databaseHealth,
                logProvider,
                tracers,
                storeCopyCheckPointMutex,
                cursorContextFactory,
                clock,
                ioController,
                memoryTracker,
                databaseConfig);

        long recurringPeriod = threshold.checkFrequencyMillis();
        CheckPointScheduler checkPointScheduler = new CheckPointScheduler(
                checkPointer, scheduler, recurringPeriod, databaseHealth, namedDatabaseId.name());

        life.add(checkPointer);
        life.add(checkPointScheduler);

        TransactionLogServiceImpl transactionLogService = new TransactionLogServiceImpl(
                logMetadataProvider,
                logFiles,
                logicalTransactionStore,
                pruneLock,
                databaseAvailabilityGuard,
                logProvider,
                checkPointer,
                commandReaderFactory,
                binarySupportedKernelVersions);
        databaseDependencies.satisfyDependencies(
                checkPointer,
                logFiles,
                logicalTransactionStore,
                transactionAppender,
                transactionLogService,
                logPruning);

        return new DatabaseTransactionLogModule(checkPointer, transactionAppender, logicalTransactionStore);
    }

    private DatabaseKernelModule buildKernel(
            LogFiles logFiles,
            DatabaseTransactionLogModule logsModule,
            IndexingService indexingService,
            DatabaseSchemaState databaseSchemaState,
            StorageEngine storageEngine,
            LogMetadataProvider logMetadataProvider,
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
                commandCommitListeners,
                !isSystem() && databaseConfig.get(GraphDatabaseInternalSettings.prefetch_on_commit),
                internalLogProvider);
        var rollbackProcess =
                commitProcessFactory.createRollbackProcess(storageEngine, logsModule.getLogicalTransactionStore());
        databaseDependencies.satisfyDependency(rollbackProcess);
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
        var transactionIdGenerator = databaseConfig.get(GraphDatabaseInternalSettings.merged_log) && !isSystem()
                ? TransactionIdGenerator.EXTERNAL_ID
                : new IdStoreTransactionIdGenerator(logMetadataProvider);
        databaseDependencies.satisfyDependency(transactionIdGenerator);

        if (!databaseDependencies.containsDependency(ApplyEnrichmentStrategy.class)) {
            // ensure by default that no enrichment occurs
            databaseDependencies.satisfyDependency(ApplyEnrichmentStrategy.NO_ENRICHMENT);
        }

        KernelTransactions kernelTransactions = kernelTransactionsFactory.create(
                databaseConfig,
                databaseLockManager,
                constraintIndexCreator,
                transactionCommitProcess,
                rollbackProcess,
                databaseTransactionEventListeners,
                transactionStats,
                databaseAvailabilityGuard,
                storageEngine,
                globalProcedures,
                globalDependencies.resolveDependency(DbmsRuntimeVersionProvider.class),
                logMetadataProvider,
                logMetadataProvider,
                serverIdentity,
                clock,
                cpuClockRef,
                accessCapabilityFactory,
                cursorContextFactory,
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
                externalIdReuseConditionProvider.get(logMetadataProvider, clock),
                commitmentFactory,
                transactionIdSequence,
                transactionIdGenerator,
                databaseHealth,
                transactionValidatorFactory,
                exceptionHandlerService,
                internalLogProvider,
                mode,
                databaseMonitors);

        var transactionMonitor =
                buildTransactionMonitor(kernelTransactions, logMetadataProvider, databaseConfig, indexingService);

        KernelImpl kernel = new KernelImpl(
                kernelTransactions,
                databaseHealth,
                transactionStats,
                globalProcedures,
                databaseConfig,
                storageEngine,
                transactionExecutionMonitor);

        final StoreFileListing fileListing =
                new StoreFileListing(databaseLayout, fs, logFiles, indexingService, storageEngine);
        databaseDependencies.satisfyDependency(fileListing);

        return new DatabaseKernelModule(
                transactionCommitProcess,
                kernel,
                kernelTransactions,
                fileListing,
                transactionMonitor,
                transactionIdGenerator);
    }

    private KernelTransactionMonitor buildTransactionMonitor(
            KernelTransactions kernelTransactions,
            TransactionIdStore transactionIdStore,
            Config config,
            IndexingService indexingService) {
        var kernelTransactionMonitor = new KernelTransactionMonitor(
                kernelTransactions,
                transactionIdStore,
                config,
                clock,
                databaseLogService,
                indexingService,
                databaseHealth,
                isMultiVersioned(storageEngineFactory, namedDatabaseId));
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
        return storageEngine.metadataProvider().getStoreId();
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
    public ResourceIterator<Path> listStoreFiles(boolean includeLogs) throws IOException {
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
    public StorageEngine getStorageEngine() {
        return storageEngine;
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
    public ChunkedTransactionTracker getChunkedTransactionTracker() {
        return chunkedTransactionTracker;
    }

    @Override
    public ElementIdMapper getElementIdMapper() {
        return elementIdMapper;
    }

    public long estimateAvailableReservedSpace() {
        return storageEngine.estimateAvailableReservedSpace();
    }

    /**
     * Called when a new lease is acquired in a cluster.
     * This method must not perform any actions that can block for a long time. Any long-running
     * actions must be run in other threads. Any transactions created as a result of a call to
     * this method that fail to replicate should not be retried immediately. Instead, they should
     * be retried on the next leaseholder after this method is called again on that next leaseholder.
     * @param leaseId The ID of the newly acquired lease.
     */
    public void newLeaseAcquired(int leaseId, boolean iAmLeaseOwner) {
        try (var cursorContext = cursorContextFactory.create(ID_CACHE_CLUSTER_CLEANUP_TAG)) {
            idGeneratorFactory.clearCache(iAmLeaseOwner, cursorContext);
        }

        if (iAmLeaseOwner) {
            leaseMonitor.newLeaseAcquired(leaseId);
            if (!storageEngineFactory.multiVersioned()) {
                return;
            }
            scheduler.schedule(
                    STORAGE_MAINTENANCE,
                    () -> multiVersionDatabaseRollbackService.postLeaseSwitchTransactionCleanup(leaseId));
        }
    }

    public CompleteCommandBatch createUpgradeCommandBatch(KernelVersion to, int leaseId) {
        LogMetadataProvider logMetadataProvider = databaseDependencies.resolveDependency(LogMetadataProvider.class);
        KernelVersion from = logMetadataProvider.kernelVersion();
        LogFormat logFormatTo =
                pickLogFormatOnUpgrade(from, to, databaseConfig, logMetadataProvider.getCurrentLogFormat());
        long now = clock.millis();
        return new CompleteCommandBatch(
                List.of(storageEngine.createUpgradeCommand(from, to, logFormatTo)),
                TransactionIdStore.UNKNOWN_CONSENSUS_INDEX,
                now,
                logMetadataProvider.getLastCommittedTransactionId(),
                now,
                leaseId,
                Leases.NO_LEASES, // Skipped for now since not using this upgrade for SPD yet
                from,
                Subject.AUTH_DISABLED);
    }

    public UpgradeLock lockForUpgrade(LeaseClient leaseClient) {
        LockManager.Client lockClient = databaseLockManager.newClient();
        lockClient.initialize(
                leaseClient, LockManager.Client.INVALID_TRANSACTION_ID, otherDatabaseMemoryTracker, databaseConfig);
        return new UpgradeLock(lockClient, UpgradeLocker.DEFAULT.acquireWriteLock(lockClient));
    }

    private void prepareStop(Predicate<PagedFile> deleteFilePredicate) {
        databasePageCache.listExistingMappings().stream()
                .filter(deleteFilePredicate)
                .forEach(file -> file.setDeleteOnClose(true));
    }

    public static Iterable<IndexDescriptor> initialSchemaRulesLoader(ReadableStorageEngine storageEngine) {
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

    private static LockService createLockService(
            StorageEngineFactory storageEngineFactory, NamedDatabaseId namedDatabaseId) {
        return isMultiVersioned(storageEngineFactory, namedDatabaseId)
                ? LockService.NO_LOCK_SERVICE
                : new ReentrantLockService();
    }

    private static TransactionIdSnapshotFactory getTransactionIdSnapshotFactory(
            StorageEngineFactory storageEngineFactory,
            LogMetadataProvider metadataProvider,
            NamedDatabaseId namedDatabaseId) {
        return isMultiVersioned(storageEngineFactory, namedDatabaseId)
                ? metadataProvider::getClosedTransactionSnapshot
                : (() -> new TransactionIdSnapshot(metadataProvider.getHighestGapFreeClosedTransactionId()));
    }

    private static OldestVisibilityHorizonFactory getOldestVisibilityHorizonFactory(
            StorageEngineFactory storageEngineFactory,
            Supplier<DatabaseKernelModule> kernelModule,
            NamedDatabaseId namedDatabaseId) {
        return isMultiVersioned(storageEngineFactory, namedDatabaseId)
                ? (() -> kernelModule.get().transactionMonitor().oldestVisibilityHorizon())
                : OldestVisibilityHorizonFactory.EMPTY_OLDEST_HORIZON_FACTORY;
    }

    private static boolean isMultiVersioned(
            StorageEngineFactory storageEngineFactory, NamedDatabaseId namedDatabaseId) {
        return !namedDatabaseId.isSystemDatabase() && storageEngineFactory.multiVersioned();
    }

    private class KernelTransactionVisibilityProvider implements TransactionVisibilityProvider {
        @Override
        public long oldestVisibilityHorizon() {
            return kernelModule.transactionMonitor().oldestVisibilityHorizon();
        }

        @Override
        public long oldestCleanupHorizon() {
            return kernelModule.transactionMonitor().oldestCleanupHorizon();
        }

        @Override
        public long youngestObservableHorizon() {
            return kernelModule.transactionMonitor().youngestObservableHorizon();
        }
    }

    public record UpgradeLock(LockManager.Client lockClient, org.neo4j.lock.Lock lock) implements Closeable {
        @Override
        public void close() {
            lockClient.close();
        }
    }

    private class IdControllerVisibilityBoundary implements IdController.VisibilityHorizonVisibilityBoundary {
        @Override
        public long oldestCleanupHorizon() {
            return kernelModule.transactionMonitor().oldestCleanupHorizon();
        }

        @Override
        public long oldestVisibilityHorizon() {
            return kernelModule.transactionMonitor().oldestVisibilityHorizon();
        }
    }
}
