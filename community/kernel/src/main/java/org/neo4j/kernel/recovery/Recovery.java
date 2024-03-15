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
package org.neo4j.kernel.recovery;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.helpers.collection.Iterables.stream;
import static org.neo4j.io.pagecache.context.OldestTransactionIdFactory.EMPTY_OLDEST_ID_FACTORY;
import static org.neo4j.io.pagecache.context.TransactionIdSnapshotFactory.EMPTY_SNAPSHOT_FACTORY;
import static org.neo4j.kernel.impl.api.TransactionVisibilityProvider.EMPTY_VISIBILITY_PROVIDER;
import static org.neo4j.kernel.impl.constraints.ConstraintSemantics.getConstraintSemantics;
import static org.neo4j.kernel.impl.locking.LockManager.NO_LOCKS_LOCK_MANAGER;
import static org.neo4j.kernel.recovery.RecoveryStartupChecker.EMPTY_CHECKER;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;
import static org.neo4j.scheduler.Group.INDEX_CLEANUP;
import static org.neo4j.scheduler.Group.INDEX_CLEANUP_WORK;
import static org.neo4j.storageengine.api.StorageEngineFactory.selectStorageEngine;
import static org.neo4j.time.Clocks.systemClock;
import static org.neo4j.token.api.TokenHolder.TYPE_LABEL;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;
import static org.neo4j.token.api.TokenHolder.TYPE_RELATIONSHIP_TYPE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.LocalConfig;
import org.neo4j.dbms.database.DatabasePageCache;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.index.internal.gbptree.GroupingRecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.id.DefaultIdController;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.database.DefaultForceOperation;
import org.neo4j.kernel.database.MetadataCache;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionFailureStrategies;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.context.TransactionVersionContextSupplier;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.index.DatabaseIndexStats;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.FileStoreProviderRegistry;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.RecoveryThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruningImpl;
import org.neo4j.kernel.impl.transaction.state.StaticIndexProviderMapFactory;
import org.neo4j.kernel.impl.transaction.state.storeview.FullScanStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.IndexStoreViewFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.kernel.recovery.facade.DatabaseRecoveryFacade;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LoggerPrintWriterAdaptor;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.monitoring.Monitors;
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
import org.neo4j.time.Stopwatch;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;

/**
 * Utility class to perform store recovery or check is recovery is required.
 * Recovery is required and can/will be performed on database that have at least one transaction in transaction log after last available checkpoint.
 * During recovery all recorded changes from transaction logs will be replayed and in the end checkpoint will be performed.
 * Please note that recovery will not gonna wait for all affected indexes populations to finish.
 */
public final class Recovery {
    private Recovery() {}

    /**
     * Provide recovery helper that can perform recovery of some database described by {@link DatabaseLayout}.
     *
     * @param fs                                database filesystem
     * @param pageCache                         page cache used to perform database recovery.
     * @param tracers                           underlying operation tracers
     * @param config                            custom configuration
     * @param emptyLogsFallbackKernelVersion    provides the kernel version if no logs exists
     * @param databaseId databaseId of database to recover
     * @return helper recovery checker
     */
    public static DatabaseRecoveryFacade recoveryFacade(
            FileSystemAbstraction fs,
            PageCache pageCache,
            Tracers tracers,
            Config config,
            MemoryTracker memoryTracker,
            InternalLogProvider logProvider,
            KernelVersionProvider emptyLogsFallbackKernelVersion,
            NamedDatabaseId databaseId) {
        return new DatabaseRecoveryFacade(
                fs,
                pageCache,
                new DatabaseTracers(tracers, databaseId),
                config,
                memoryTracker,
                logProvider,
                emptyLogsFallbackKernelVersion);
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
    public static boolean isRecoveryRequired(
            FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, MemoryTracker memoryTracker)
            throws Exception {
        requireNonNull(databaseLayout);
        requireNonNull(config);
        requireNonNull(fs);
        config = Config.newBuilder()
                .fromConfig(config)
                .set(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8))
                .build();
        try (JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
                PageCache pageCache = getPageCache(config, fs, jobScheduler)) {
            return isRecoveryRequired(
                    fs, pageCache, databaseLayout, config, Optional.empty(), memoryTracker, DatabaseTracers.EMPTY);
        }
    }

    public static boolean isRecoveryRequired(
            FileSystemAbstraction fs,
            PageCache pageCache,
            DatabaseLayout databaseLayout,
            Config config,
            Optional<LogTailMetadata> logTailMetadata,
            MemoryTracker memoryTracker,
            DatabaseTracers databaseTracers)
            throws IOException {
        return isRecoveryRequired(
                fs,
                pageCache,
                databaseLayout,
                StorageEngineFactory.selectStorageEngine(fs, databaseLayout, config),
                config,
                logTailMetadata,
                memoryTracker,
                databaseTracers);
    }

    public static boolean isRecoveryRequired(
            FileSystemAbstraction fs,
            PageCache pageCache,
            DatabaseLayout layout,
            StorageEngineFactory storageEngineFactory,
            Config config,
            Optional<LogTailMetadata> logTailMetadata,
            MemoryTracker memoryTracker,
            DatabaseTracers databaseTracers)
            throws IOException {
        RecoveryRequiredChecker requiredChecker =
                new RecoveryRequiredChecker(fs, pageCache, config, storageEngineFactory, databaseTracers);
        final var databaseLayout = storageEngineFactory.formatSpecificDatabaseLayout(layout);
        return logTailMetadata.isPresent()
                ? requiredChecker.isRecoveryRequiredAt(databaseLayout, logTailMetadata.get())
                : requiredChecker.isRecoveryRequiredAt(databaseLayout, memoryTracker);
    }

    public static Context context(
            FileSystemAbstraction fs,
            PageCache pageCache,
            DatabaseTracers tracers,
            Config config,
            DatabaseLayout databaseLayout,
            MemoryTracker memoryTracker,
            IOController ioController,
            InternalLogProvider logProvider,
            KernelVersionProvider emptyLogsFallbackKernelVersion) {
        return new Context(
                fs,
                pageCache,
                databaseLayout,
                config,
                memoryTracker,
                tracers,
                ioController,
                logProvider,
                emptyLogsFallbackKernelVersion);
    }

    public static Context context(
            FileSystemAbstraction fs,
            PageCache pageCache,
            DatabaseTracers tracers,
            Config config,
            DatabaseLayout databaseLayout,
            MemoryTracker memoryTracker,
            IOController ioController,
            InternalLogProvider logProvider,
            LogTailMetadata logTail) {
        return new Context(
                fs, pageCache, databaseLayout, config, memoryTracker, tracers, ioController, logProvider, logTail);
    }

    /**
     * Context used to perform recovery of database described by provided layout.
     * If recovery is not required - nothing will be done to the database or logs unless {@link Context#force()} is specified
     */
    public static class Context {
        private final MemoryTracker memoryTracker;
        private final DatabaseLayout databaseLayout;
        private final FileSystemAbstraction fs;
        private final PageCache pageCache;
        private final Config config;
        private final DatabaseTracers tracers;
        private final InternalLogProvider logProvider;
        private boolean forceRunRecovery;
        private Monitors globalMonitors = new Monitors();
        private Iterable<ExtensionFactory<?>> extensionFactories;
        private Optional<LogTailMetadata> providedLogTail = Optional.empty();
        private RecoveryStartupChecker startupChecker = EMPTY_CHECKER;
        private Clock clock = systemClock();
        private final IOController ioController;
        private RecoveryPredicate recoveryPredicate = RecoveryPredicate.ALL;
        private RecoveryMode mode = RecoveryMode.FULL;
        private long awaitIndexesOnlineMillis;
        private final KernelVersionProvider emptyLogsFallbackKernelVersion;

        private Context(
                FileSystemAbstraction fileSystemAbstraction,
                PageCache pageCache,
                DatabaseLayout databaseLayout,
                Config config,
                MemoryTracker memoryTracker,
                DatabaseTracers tracers,
                IOController ioController,
                InternalLogProvider logProvider,
                KernelVersionProvider emptyLogsFallbackKernelVersion) {
            requireNonNull(pageCache);
            requireNonNull(fileSystemAbstraction);
            requireNonNull(databaseLayout);
            requireNonNull(config);
            this.pageCache = pageCache;
            this.fs = fileSystemAbstraction;
            this.databaseLayout = databaseLayout;
            this.config = config;
            this.memoryTracker = memoryTracker;
            this.tracers = tracers;
            this.ioController = ioController;
            this.logProvider = requireNonNull(logProvider);
            this.emptyLogsFallbackKernelVersion = emptyLogsFallbackKernelVersion;
        }

        private Context(
                FileSystemAbstraction fileSystemAbstraction,
                PageCache pageCache,
                DatabaseLayout databaseLayout,
                Config config,
                MemoryTracker memoryTracker,
                DatabaseTracers tracers,
                IOController ioController,
                InternalLogProvider logProvider,
                LogTailMetadata logTail) {
            requireNonNull(pageCache);
            requireNonNull(fileSystemAbstraction);
            requireNonNull(databaseLayout);
            requireNonNull(config);
            this.pageCache = pageCache;
            this.fs = fileSystemAbstraction;
            this.databaseLayout = databaseLayout;
            this.config = config;
            this.memoryTracker = memoryTracker;
            this.tracers = tracers;
            this.ioController = ioController;
            this.logProvider = requireNonNull(logProvider);
            // No need for the kernelVersionProvider if we are guaranteed a log tail.
            this.emptyLogsFallbackKernelVersion = KernelVersionProvider.THROWING_PROVIDER;
            this.providedLogTail = Optional.of(logTail);
        }

        /**
         * @param monitors global server monitors
         */
        public Context monitors(Monitors monitors) {
            this.globalMonitors = monitors;
            return this;
        }

        /**
         * @param extensionFactories extension factories for extensions that should participate in recovery
         */
        public Context extensionFactories(Iterable<ExtensionFactory<?>> extensionFactories) {
            this.extensionFactories = extensionFactories;
            return this;
        }

        /**
         * Force recovery to run even if the usual checks indicates that it's not required.
         * In specific cases, like after store copy there's always a need for doing a recovery or at least to start the db, checkpoint and shut down,
         * even if the normal "is recovery required" checks says that recovery isn't required
         */
        public Context force() {
            this.forceRunRecovery = true;
            return this;
        }

        /**
         * @param startupChecker Checker for recovery startup.
         */
        public Context startupChecker(RecoveryStartupChecker startupChecker) {
            this.startupChecker = startupChecker;
            return this;
        }

        /**
         * @param clock The clock to use
         */
        public Context clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * @param recoveryPredicate Criteria to decide when to stop recovery
         * We always replay everything, but can do early termination based on predicate for point in time recovery
         */
        public Context recoveryPredicate(RecoveryPredicate recoveryPredicate) {
            this.recoveryPredicate = recoveryPredicate;
            return this;
        }

        public Context awaitIndexesOnline(long time, TimeUnit unit) {
            this.awaitIndexesOnlineMillis = unit.toMillis(time);
            return this;
        }

        public Context recoveryMode(RecoveryMode mode) {
            this.mode = mode;
            return this;
        }
    }

    /**
     * Perform recovery as specified by the provided context
     * @param context The context to use
     * @throws IOException on any unexpected I/O exception encountered during recovery.
     */
    public static boolean performRecovery(Context context) throws IOException {
        requireNonNull(context);
        StorageEngineFactory storageEngineFactory =
                selectStorageEngine(context.fs, context.databaseLayout, context.config);
        Iterable<ExtensionFactory<?>> extensionFactories =
                context.extensionFactories != null ? context.extensionFactories : loadExtensions();

        assert !(context.pageCache instanceof DatabasePageCache)
                : "Recovery should use global page cache to avoid using overloaded mapping.";
        var config = new LocalConfig(context.config);
        try {
            return performRecovery(
                    context.fs,
                    context.pageCache,
                    context.tracers,
                    config,
                    storageEngineFactory.formatSpecificDatabaseLayout(context.databaseLayout),
                    storageEngineFactory,
                    context.forceRunRecovery,
                    context.logProvider,
                    context.globalMonitors,
                    extensionFactories,
                    context.providedLogTail,
                    context.startupChecker,
                    context.memoryTracker,
                    context.clock,
                    context.ioController,
                    context.recoveryPredicate,
                    context.awaitIndexesOnlineMillis,
                    context.emptyLogsFallbackKernelVersion,
                    context.mode);
        } finally {
            config.removeAllLocalListeners();
        }
    }

    private static boolean performRecovery(
            FileSystemAbstraction fs,
            PageCache pageCache,
            DatabaseTracers tracers,
            Config config,
            DatabaseLayout databaseLayout,
            StorageEngineFactory storageEngineFactory,
            boolean forceRunRecovery,
            InternalLogProvider logProvider,
            Monitors globalMonitors,
            Iterable<ExtensionFactory<?>> extensionFactories,
            Optional<LogTailMetadata> providedLogTail,
            RecoveryStartupChecker startupChecker,
            MemoryTracker memoryTracker,
            Clock clock,
            IOController ioController,
            RecoveryPredicate recoveryPredicate,
            long awaitIndexesOnlineMillis,
            KernelVersionProvider emptyLogsFallbackKernelVersion,
            RecoveryMode mode)
            throws IOException {
        InternalLog recoveryLog = logProvider.getLog(Recovery.class);
        if (!forceRunRecovery
                && !isRecoveryRequired(
                        fs,
                        pageCache,
                        databaseLayout,
                        storageEngineFactory,
                        config,
                        providedLogTail,
                        memoryTracker,
                        tracers)) {
            return false;
        }
        checkAllFilesPresence(databaseLayout, fs, pageCache, storageEngineFactory);
        LifeSupport recoveryLife = new LifeSupport();
        var namedDatabaseId = createRecoveryDatabaseId(fs, pageCache, databaseLayout, storageEngineFactory);
        Monitors monitors = new Monitors(globalMonitors, logProvider);
        VersionStorage recoveryVersionStorage = VersionStorage.EMPTY_STORAGE;
        DatabasePageCache databasePageCache =
                new DatabasePageCache(pageCache, ioController, recoveryVersionStorage, config);
        SimpleLogService logService = new SimpleLogService(logProvider);
        DatabaseReadOnlyChecker readOnlyChecker = writable();

        DatabaseSchemaState schemaState = new DatabaseSchemaState(logProvider);
        JobScheduler scheduler = recoveryLife.add(JobSchedulerFactory.createInitialisedScheduler());
        DatabaseAvailabilityGuard guard = new RecoveryAvailabilityGuard(namedDatabaseId, clock, recoveryLog);
        recoveryLife.add(guard);

        TransactionVersionContextSupplier versionContextSupplier = new TransactionVersionContextSupplier();
        versionContextSupplier.init(EMPTY_SNAPSHOT_FACTORY, EMPTY_OLDEST_ID_FACTORY);
        CursorContextFactory cursorContextFactory =
                new CursorContextFactory(tracers.getPageCacheTracer(), versionContextSupplier);
        DatabaseHealth databaseHealth = new DatabaseHealth(HealthEventGenerator.NO_OP, recoveryLog);

        // The token registries during recovery can add tokens w/o making a defensive copy
        // of all internal token registry state, because there should be none doing lookups
        // In fact token lookup should not be necessary at all during recovery
        TokenHolders tokenHolders = new TokenHolders(
                new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TYPE_PROPERTY_KEY),
                new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TYPE_LABEL),
                new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TYPE_RELATIONSHIP_TYPE));

        RecoveryCleanupWorkCollector recoveryCleanupCollector =
                recoveryLife.add(new GroupingRecoveryCleanupWorkCollector(
                        scheduler, INDEX_CLEANUP, INDEX_CLEANUP_WORK, databaseLayout.getDatabaseName()));

        DatabaseExtensions extensions = recoveryLife.add(instantiateRecoveryExtensions(
                databaseLayout,
                fs,
                config,
                logService,
                databasePageCache,
                scheduler,
                DbmsInfo.TOOL,
                monitors,
                tokenHolders,
                recoveryCleanupCollector,
                readOnlyChecker,
                extensionFactories,
                guard,
                tracers,
                namedDatabaseId,
                cursorContextFactory));
        Dependencies indexDependencies = new Dependencies(extensions);
        indexDependencies.satisfyDependencies(recoveryVersionStorage);

        var indexProviderMap = recoveryLife.add(StaticIndexProviderMapFactory.create(
                recoveryLife,
                config,
                databasePageCache,
                fs,
                logService,
                monitors,
                readOnlyChecker,
                HostedOnMode.SINGLE,
                recoveryCleanupCollector,
                databaseLayout,
                tokenHolders,
                scheduler,
                cursorContextFactory,
                tracers.getPageCacheTracer(),
                indexDependencies));

        LogTailMetadata logTailMetadata = providedLogTail.orElseGet(() -> loadLogTail(
                fs,
                tracers,
                config,
                databaseLayout,
                storageEngineFactory,
                memoryTracker,
                emptyLogsFallbackKernelVersion));
        MetadataCache recoveryMetaDataCache = new MetadataCache(logTailMetadata);
        StorageEngine storageEngine = storageEngineFactory.instantiate(
                fs,
                clock,
                databaseLayout,
                config,
                databasePageCache,
                tokenHolders,
                schemaState,
                getConstraintSemantics(),
                indexProviderMap,
                NO_LOCK_SERVICE,
                new DefaultIdGeneratorFactory(
                        fs, recoveryCleanupCollector, tracers.getPageCacheTracer(), databaseLayout.getDatabaseName()),
                databaseHealth,
                logService.getInternalLogProvider(),
                logService.getUserLogProvider(),
                recoveryCleanupCollector,
                logTailMetadata,
                recoveryMetaDataCache,
                memoryTracker,
                cursorContextFactory,
                tracers.getPageCacheTracer(),
                recoveryVersionStorage);

        // Schema indexes
        FullScanStoreView fullScanStoreView = new FullScanStoreView(NO_LOCK_SERVICE, storageEngine, config, scheduler);
        IndexStoreViewFactory indexStoreViewFactory = new IndexStoreViewFactory(
                config, storageEngine, NO_LOCKS_LOCK_MANAGER, fullScanStoreView, NO_LOCK_SERVICE, logProvider);

        IndexStatisticsStore indexStatisticsStore = new IndexStatisticsStore(
                databasePageCache,
                fs,
                databaseLayout,
                recoveryCleanupCollector,
                false,
                cursorContextFactory,
                tracers.getPageCacheTracer(),
                storageEngine.getOpenOptions());
        IndexingService indexingService = Database.buildIndexingService(
                storageEngine,
                schemaState,
                indexStoreViewFactory,
                indexStatisticsStore,
                new DatabaseIndexStats(),
                config,
                scheduler,
                indexProviderMap,
                tokenHolders,
                logProvider,
                monitors.newMonitor(IndexMonitor.class),
                cursorContextFactory,
                memoryTracker,
                databaseLayout.getDatabaseName(),
                readOnlyChecker,
                clock,
                recoveryMetaDataCache,
                fs,
                EMPTY_VISIBILITY_PROVIDER);

        MetadataProvider metadataProvider = storageEngine.metadataProvider();

        var dependencies = dependenciesOf(
                databaseLayout,
                config,
                databasePageCache,
                fs,
                logProvider,
                tokenHolders,
                schemaState,
                getConstraintSemantics(),
                NO_LOCK_SERVICE,
                databaseHealth,
                new DefaultIdGeneratorFactory(
                        fs, recoveryCleanupCollector, tracers.getPageCacheTracer(), databaseLayout.getDatabaseName()),
                new DefaultIdController(),
                readOnlyChecker,
                cursorContextFactory,
                logService,
                metadataProvider);

        LogFiles logFiles = LogFilesBuilder.builder(databaseLayout, fs, recoveryMetaDataCache)
                .withStorageEngineFactory(storageEngineFactory)
                .withConfig(config)
                .withDatabaseTracers(tracers)
                .withExternalLogTailMetadata(logTailMetadata)
                .withDependencies(dependencies)
                .withMemoryTracker(memoryTracker)
                .build();

        boolean failOnCorruptedLogFiles = config.get(GraphDatabaseInternalSettings.fail_on_corrupted_log_files);
        validateStoreId(logTailMetadata, storageEngine.retrieveStoreId());

        TransactionMetadataCache metadataCache = new TransactionMetadataCache();
        PhysicalLogicalTransactionStore transactionStore = new PhysicalLogicalTransactionStore(
                logFiles,
                metadataCache,
                storageEngineFactory.commandReaderFactory(),
                monitors,
                failOnCorruptedLogFiles,
                config);

        LifeSupport schemaLife = new LifeSupport();
        schemaLife.add(storageEngine.schemaAndTokensLifecycle());
        schemaLife.add(indexingService);

        var doParallelRecovery = config.get(GraphDatabaseInternalSettings.do_parallel_recovery);
        TransactionLogsRecovery transactionLogsRecovery = transactionLogRecovery(
                fs,
                metadataProvider,
                monitors.newMonitor(RecoveryMonitor.class),
                monitors.newMonitor(RecoveryStartInformationProvider.Monitor.class),
                logFiles,
                storageEngine,
                logTailMetadata,
                transactionStore,
                metadataProvider,
                schemaLife,
                databaseLayout,
                failOnCorruptedLogFiles,
                recoveryLog,
                startupChecker,
                memoryTracker,
                clock,
                doParallelRecovery,
                recoveryPredicate,
                cursorContextFactory,
                mode,
                new BinarySupportedKernelVersions(config));

        CheckPointerImpl.ForceOperation forceOperation =
                new DefaultForceOperation(indexingService, storageEngine, databasePageCache);
        var checkpointAppender = logFiles.getCheckpointFile().getCheckpointAppender();
        LogPruning logPruning = new LogPruningImpl(
                fs, logFiles, logProvider, new LogPruneStrategyFactory(), clock, config, new ReentrantLock());
        CheckPointerImpl checkPointer = new CheckPointerImpl(
                metadataProvider,
                RecoveryThreshold.INSTANCE,
                forceOperation,
                logPruning,
                checkpointAppender,
                databaseHealth,
                logProvider,
                tracers,
                new StoreCopyCheckPointMutex(),
                cursorContextFactory,
                clock,
                ioController,
                recoveryMetaDataCache);
        recoveryLife.add(indexStatisticsStore);
        recoveryLife.add(storageEngine);
        recoveryLife.add(new MissingTransactionLogsCheck(config, logTailMetadata, recoveryLog));
        recoveryLife.add(logFiles);
        recoveryLife.add(transactionLogsRecovery);
        recoveryLife.add(checkPointer);
        try {
            recoveryLife.start();

            if (databaseHealth.hasNoPanic()) {
                if (logTailMetadata.hasUnreadableBytesInCheckpointLogs()) {
                    logFiles.getCheckpointFile().rotate();
                }
                if (awaitIndexesOnlineMillis > 0) {
                    awaitIndexesOnline(indexingService, awaitIndexesOnlineMillis);
                }
                // stop extensions now to prevent possible interference with checkpoint
                extensions.stop();
                String recoveryMessage =
                        logTailMetadata.logsMissing() ? "Recovery with missing logs completed." : "Recovery completed.";
                checkPointer.forceCheckPoint(new SimpleTriggerInfo(recoveryMessage));
            }
        } finally {
            recoveryLife.shutdown();
        }
        return true;
    }

    private static void awaitIndexesOnline(IndexingService indexingService, long awaitIndexesOnlineMillis) {
        var stopWatch = Stopwatch.start();
        try {
            for (IndexProxy indexProxy : indexingService.getIndexProxies()) {
                while (stopWatch.hasTimedOut(awaitIndexesOnlineMillis, MILLISECONDS)
                        && indexProxy.getState() == InternalIndexState.POPULATING) {
                    MILLISECONDS.sleep(10);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static LogTailMetadata loadLogTail(
            FileSystemAbstraction fs,
            DatabaseTracers tracers,
            Config config,
            DatabaseLayout databaseLayout,
            StorageEngineFactory storageEngineFactory,
            MemoryTracker memoryTracker,
            KernelVersionProvider emptyLogsFallbackKernelVersion) {
        try {
            return new LogTailExtractor(fs, config, storageEngineFactory, tracers, false)
                    .getTailMetadata(databaseLayout, memoryTracker, emptyLogsFallbackKernelVersion);
        } catch (IOException ioe) {
            throw new UncheckedIOException("Fail to load log tail.", ioe);
        }
    }

    private static NamedDatabaseId createRecoveryDatabaseId(
            FileSystemAbstraction fs,
            PageCache pageCache,
            DatabaseLayout databaseLayout,
            StorageEngineFactory storageEngineFactory) {
        UUID uuid = storageEngineFactory
                .databaseIdUuid(fs, databaseLayout, pageCache, CursorContext.NULL_CONTEXT)
                .orElse(new UUID(0, 0));
        return DatabaseIdFactory.from(databaseLayout.getDatabaseName(), uuid);
    }

    public static void validateStoreId(LogTailMetadata tailMetadata, StoreId storeId) {
        var optionalTxStoreId = tailMetadata.getStoreId();
        if (optionalTxStoreId.isPresent()) {
            var txStoreId = optionalTxStoreId.get();
            if (!storeId.isSameOrUpgradeSuccessor(txStoreId) && !txStoreId.isSameOrUpgradeSuccessor(storeId)) {
                throw new RuntimeException(
                        "Mismatching store id. Store StoreId: " + storeId + ". Transaction log StoreId: " + txStoreId);
            }
        }
    }

    private static void checkAllFilesPresence(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fs,
            PageCache pageCache,
            StorageEngineFactory storageEngineFactory) {
        StorageFilesState state = storageEngineFactory.checkStoreFileState(fs, databaseLayout, pageCache);
        if (state.recoveryState() == RecoveryState.UNRECOVERABLE) {
            throw new RuntimeException(format(
                    "Store files %s is(are) missing and recovery is not possible. Please restore from a consistent backup.",
                    state.missingFiles()));
        }
    }

    private static TransactionLogsRecovery transactionLogRecovery(
            FileSystemAbstraction fileSystemAbstraction,
            TransactionIdStore transactionIdStore,
            RecoveryMonitor recoveryMonitor,
            RecoveryStartInformationProvider.Monitor positionMonitor,
            LogFiles logFiles,
            StorageEngine storageEngine,
            KernelVersionProvider versionProvider,
            LogicalTransactionStore logicalTransactionStore,
            LogVersionRepository logVersionRepository,
            Lifecycle schemaLife,
            DatabaseLayout databaseLayout,
            boolean failOnCorruptedLogFiles,
            InternalLog log,
            RecoveryStartupChecker startupChecker,
            MemoryTracker memoryTracker,
            Clock clock,
            boolean doParallelRecovery,
            RecoveryPredicate recoveryPredicate,
            CursorContextFactory contextFactory,
            RecoveryMode mode,
            BinarySupportedKernelVersions binarySupportedKernelVersions) {
        RecoveryService recoveryService = new DefaultRecoveryService(
                storageEngine,
                transactionIdStore,
                logicalTransactionStore,
                logVersionRepository,
                logFiles,
                versionProvider,
                positionMonitor,
                log,
                clock,
                doParallelRecovery,
                binarySupportedKernelVersions,
                contextFactory);
        CorruptedLogsTruncator logsTruncator = new CorruptedLogsTruncator(
                databaseLayout.databaseDirectory(), logFiles, fileSystemAbstraction, memoryTracker);
        var loggerPrintWriterAdaptor = new LoggerPrintWriterAdaptor(log, Level.INFO);
        return new TransactionLogsRecovery(
                recoveryService,
                logsTruncator,
                schemaLife,
                recoveryMonitor,
                ProgressMonitorFactory.basicTextual(loggerPrintWriterAdaptor),
                failOnCorruptedLogFiles,
                startupChecker,
                recoveryPredicate,
                contextFactory,
                mode);
    }

    private static Iterable<ExtensionFactory<?>> loadExtensions() {
        return Iterables.cast(Services.loadAll(ExtensionFactory.class));
    }

    private static DatabaseExtensions instantiateRecoveryExtensions(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fileSystem,
            Config config,
            LogService logService,
            PageCache pageCache,
            JobScheduler jobScheduler,
            DbmsInfo info,
            Monitors monitors,
            TokenHolders tokenHolders,
            RecoveryCleanupWorkCollector recoveryCleanupCollector,
            DatabaseReadOnlyChecker readOnlyChecker,
            Iterable<ExtensionFactory<?>> extensionFactories,
            AvailabilityGuard availabilityGuard,
            DatabaseTracers tracers,
            NamedDatabaseId namedDatabaseId,
            CursorContextFactory contextFactory) {
        List<ExtensionFactory<?>> recoveryExtensions = stream(extensionFactories)
                .filter(extension -> extension.getClass().isAnnotationPresent(RecoveryExtension.class))
                .toList();

        NonListenableMonitors nonListenableMonitors =
                new NonListenableMonitors(monitors, logService.getInternalLogProvider());
        var deps = dependenciesOf(
                fileSystem,
                config,
                logService,
                pageCache,
                nonListenableMonitors,
                jobScheduler,
                tokenHolders,
                recoveryCleanupCollector,
                tracers,
                databaseLayout,
                readOnlyChecker,
                availabilityGuard,
                namedDatabaseId,
                FileStoreProviderRegistry.EMPTY,
                contextFactory);
        DatabaseExtensionContext extensionContext = new DatabaseExtensionContext(databaseLayout, info, deps);
        return new DatabaseExtensions(extensionContext, recoveryExtensions, deps, ExtensionFailureStrategies.fail());
    }

    private static PageCache getPageCache(Config config, FileSystemAbstraction fs, JobScheduler jobScheduler) {
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fs,
                config,
                PageCacheTracer.NULL,
                NullLog.getInstance(),
                jobScheduler,
                Clocks.nanoClock(),
                new MemoryPools());
        return pageCacheFactory.getOrCreatePageCache();
    }

    static void throwUnableToCleanRecover(Throwable t) {
        throw new RuntimeException(
                "Error reading transaction logs, recovery not possible. To force the database to start anyway, you can specify '"
                        + GraphDatabaseInternalSettings.fail_on_corrupted_log_files.name()
                        + "=false'. This will try to recover as much "
                        + "as possible and then truncate the corrupt part of the transaction log. Doing this means your database "
                        + "integrity might be compromised, please consider restoring from a consistent backup instead.",
                t);
    }

    private static class RecoveryAvailabilityGuard extends DatabaseAvailabilityGuard {
        RecoveryAvailabilityGuard(NamedDatabaseId namedDatabaseId, Clock clock, InternalLog log) {
            // we do not want ot pass real config to guard for recovery
            super(namedDatabaseId, clock, log, 0, new CompositeDatabaseAvailabilityGuard(clock, Config.defaults()));
            init();
            start();
        }

        @Override
        public void addListener(AvailabilityListener listener) {
            super.addListener(listener);
            listener.available();
        }
    }

    // We need to create monitors that do not allow listener registration here since at this point another version of
    // extensions already stared by owning
    // database life and if we will allow registration of listeners here we will end-up having same event captured by
    // multiple listeners resulting in
    // for example duplicated logging records in user facing logs
    private static class NonListenableMonitors extends Monitors {
        NonListenableMonitors(Monitors monitors, InternalLogProvider logProvider) {
            super(monitors, logProvider);
        }

        @Override
        public void addMonitorListener(Object monitorListener, String... tags) {}
    }

    private static class MissingTransactionLogsCheck extends LifecycleAdapter {
        private final Config config;
        private final LogTailMetadata logTail;
        private final InternalLog log;

        MissingTransactionLogsCheck(Config config, LogTailMetadata logTail, InternalLog log) {
            this.config = config;
            this.logTail = logTail;
            this.log = log;
        }

        @Override
        public void init() {
            checkForMissingLogFiles();
        }

        private void checkForMissingLogFiles() {
            if (logTail.logsMissing()) {
                if (config.get(GraphDatabaseSettings.fail_on_missing_files)) {
                    log.error("Transaction logs are missing and recovery is not possible.");
                    log.info(
                            "To force the database to start anyway, you can specify '%s=false'. "
                                    + "This will create new transaction log and will update database metadata accordingly. "
                                    + "Doing this means your database integrity might be compromised, "
                                    + "please consider restoring from a consistent backup instead.",
                            GraphDatabaseSettings.fail_on_missing_files.name());

                    throw new RuntimeException("Transaction logs are missing and recovery is not possible.");
                }
                log.warn("No transaction logs were detected, but recovery was forced by user.");
            }
        }
    }
}
