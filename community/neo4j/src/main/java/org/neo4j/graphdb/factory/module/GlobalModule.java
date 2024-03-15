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
package org.neo4j.graphdb.factory.module;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.data_collector_max_recent_query_count;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.duplication_user_messages;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation;
import static org.neo4j.configuration.GraphDatabaseSettings.filewatcher_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_tracking;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_global_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_max_off_heap_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_memory_allocation;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_off_heap_block_cache_size;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_off_heap_max_cacheable_block_size;
import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onShutdown;
import static org.neo4j.logging.log4j.LogConfig.createLoggerFromXmlConfig;

import java.nio.file.Path;
import java.util.function.Supplier;
import org.neo4j.capabilities.CapabilitiesService;
import org.neo4j.capabilities.DBMSCapabilities;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStats;
import org.neo4j.cypher.internal.util.InternalNotificationStats;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.internal.collector.RecentQueryBuffer;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemLifecycleAdapter;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.locker.Locker;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.diagnostics.providers.DbmsDiagnosticsManager;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionFailureStrategies;
import org.neo4j.kernel.extension.GlobalExtensions;
import org.neo4j.kernel.extension.context.GlobalExtensionContext;
import org.neo4j.kernel.impl.cache.VmPauseMonitorComponent;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.pagecache.PageCacheLifecycle;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.util.collection.CachingOffHeapBlockAllocator;
import org.neo4j.kernel.impl.util.collection.CapacityLimitingBlockAllocatorDecorator;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.impl.util.collection.OffHeapBlockAllocator;
import org.neo4j.kernel.impl.util.collection.OffHeapCollectionsFactory;
import org.neo4j.kernel.impl.util.watcher.DefaultFileSystemWatcherService;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.kernel.info.JvmChecker;
import org.neo4j.kernel.info.JvmMetadataRepository;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.internal.event.DefaultGlobalTransactionEventListeners;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.internal.locker.FileLockerService;
import org.neo4j.kernel.internal.locker.GlobalLockerService;
import org.neo4j.kernel.internal.locker.LockerLifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.kernel.monitoring.tracing.DefaultTracers;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.Neo4jLoggerContext;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryPools;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

/**
 * Global module for {@link DatabaseManagementServiceFactory}. This creates all global services and components from DBMS.
 */
public class GlobalModule {

    private final PageCache pageCache;
    private final Monitors globalMonitors;
    private final Dependencies globalDependencies;
    private final LogService logService;
    private final LifeSupport globalLife;
    private final Neo4jLayout neo4jLayout;
    private final DbmsInfo dbmsInfo;
    private final DbmsDiagnosticsManager dbmsDiagnosticsManager;
    private final Tracers tracers;
    private final Config globalConfig;
    private final FileSystemAbstraction fileSystem;
    private final GlobalExtensions globalExtensions;
    private final Iterable<ExtensionFactory<?>> extensionFactories;
    private final JobScheduler jobScheduler;
    private final SystemNanoClock globalClock;
    private final CollectionsFactorySupplier collectionsFactorySupplier;
    private final ConnectorPortRegister connectorPortRegister;
    private final CompositeDatabaseAvailabilityGuard globalAvailabilityGuard;
    private final FileSystemWatcherService fileSystemWatcher;
    private final DatabaseEventListeners databaseEventListeners;
    private final GlobalTransactionEventListeners transactionEventListeners;
    private final DependencyResolver externalDependencyResolver;
    private final FileLockerService fileLockerService;
    private final MemoryPools memoryPools;
    private final InternalNotificationStats cypherNotificationStats;
    private final InternalSyntaxUsageStats cypherSyntaxUsageStats;
    private final GlobalMemoryGroupTracker transactionsMemoryPool;
    private final GlobalMemoryGroupTracker otherMemoryPool;
    private final CapabilitiesService capabilitiesService;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;

    /**
     * @param globalConfig         configuration affecting global aspects of the system.
     * @param dbmsInfo             the type of dbms this module manages.
     * @param daemonMode           if we run in daemon mode or not. If {@code true}, we will avoid printing to stdout/stderr.
     * @param externalDependencies optional external dependencies provided by caller.
     */
    public GlobalModule(
            Config globalConfig, DbmsInfo dbmsInfo, boolean daemonMode, ExternalDependencies externalDependencies) {
        externalDependencyResolver =
                externalDependencies.dependencies() != null ? externalDependencies.dependencies() : new Dependencies();

        this.dbmsInfo = dbmsInfo;

        globalDependencies = new Dependencies();
        globalDependencies.satisfyDependency(dbmsInfo);

        globalClock = globalDependencies.satisfyDependency(createClock());
        globalLife = createLife();

        this.neo4jLayout = Neo4jLayout.of(globalConfig);

        this.globalConfig = globalDependencies.satisfyDependency(globalConfig);
        binarySupportedKernelVersions = new BinarySupportedKernelVersions(this.globalConfig);
        globalDependencies.satisfyDependency(binarySupportedKernelVersions);

        fileSystem = tryResolveOrCreate(FileSystemAbstraction.class, this::createFileSystemAbstraction);
        globalDependencies.satisfyDependency(fileSystem);
        globalLife.add(new FileSystemLifecycleAdapter(fileSystem));

        // If no logging was passed in from the outside then create logging and register
        // with this life
        logService = globalDependencies.satisfyDependency(
                createLogService(externalDependencies.userLogProvider(), daemonMode));
        globalConfig.setLogger(logService.getInternalLog(Config.class));

        // Component monitoring
        globalMonitors = externalDependencies.monitors() == null ? new Monitors() : externalDependencies.monitors();
        globalDependencies.satisfyDependency(globalMonitors);

        JobScheduler createdOrResolvedScheduler = tryResolveOrCreate(JobScheduler.class, this::createJobScheduler);
        jobScheduler = globalLife.add(globalDependencies.satisfyDependency(createdOrResolvedScheduler));

        fileLockerService = createFileLockerService();
        Locker storeLocker = fileLockerService.createStoreLocker(fileSystem, neo4jLayout);
        globalLife.add(globalDependencies.satisfyDependency(new LockerLifecycleAdapter(storeLocker)));

        new JvmChecker(logService.getInternalLog(JvmChecker.class), new JvmMetadataRepository())
                .checkJvmCompatibilityAndIssueWarning();

        memoryPools = new MemoryPools(globalConfig.get(memory_tracking));
        otherMemoryPool = memoryPools.pool(MemoryGroup.OTHER, 0, null);
        transactionsMemoryPool = memoryPools.pool(
                MemoryGroup.TRANSACTION,
                globalConfig.get(memory_transaction_global_max_size),
                memory_transaction_global_max_size.name());
        globalConfig.addListener(
                memory_transaction_global_max_size, (before, after) -> transactionsMemoryPool.setSize(after));
        globalDependencies.satisfyDependency(memoryPools);

        var recentQueryBuffer = new RecentQueryBuffer(
                globalConfig.get(data_collector_max_recent_query_count),
                memoryPools.pool(MemoryGroup.RECENT_QUERY_BUFFER, 0, null).getPoolMemoryTracker());
        globalDependencies.satisfyDependency(recentQueryBuffer);

        if (globalConfig.get(GraphDatabaseInternalSettings.vm_pause_monitor_enabled)) {
            globalLife.add(new VmPauseMonitorComponent(
                    globalConfig,
                    logService.getInternalLog(VmPauseMonitorComponent.class),
                    jobScheduler,
                    globalMonitors));
        }

        globalAvailabilityGuard = new CompositeDatabaseAvailabilityGuard(globalClock, globalConfig);
        globalDependencies.satisfyDependency(globalAvailabilityGuard);
        globalLife.setLast(globalAvailabilityGuard);

        tracers = tryResolveOrCreate(Tracers.class, this::createDefaultTracers);
        globalDependencies.satisfyDependency(tracers);
        globalDependencies.satisfyDependency(tracers.getPageCacheTracer());

        collectionsFactorySupplier = createCollectionsFactorySupplier(globalConfig, globalLife, logService);

        pageCache = tryResolveOrCreate(
                PageCache.class,
                () -> createPageCache(
                        fileSystem, globalConfig, logService, tracers, jobScheduler, globalClock, memoryPools));
        globalDependencies.satisfyDependency(pageCache);

        globalLife.add(new PageCacheLifecycle(pageCache));

        dbmsDiagnosticsManager = new DbmsDiagnosticsManager(globalDependencies, logService);
        globalDependencies.satisfyDependency(dbmsDiagnosticsManager);

        dbmsDiagnosticsManager.dumpSystemDiagnostics();

        fileSystemWatcher = createFileSystemWatcherService(fileSystem, logService, jobScheduler, globalConfig);
        globalLife.add(fileSystemWatcher);
        globalDependencies.satisfyDependency(fileSystemWatcher);

        extensionFactories = externalDependencies.extensions();
        globalExtensions = globalDependencies.satisfyDependency(new GlobalExtensions(
                new GlobalExtensionContext(neo4jLayout, dbmsInfo, globalDependencies),
                extensionFactories,
                globalDependencies,
                ExtensionFailureStrategies.fail()));

        databaseEventListeners = new DatabaseEventListeners(logService.getInternalLog(DatabaseEventListeners.class));
        Iterable<? extends DatabaseEventListener> externalListeners = externalDependencies.databaseEventListeners();
        for (DatabaseEventListener databaseListener : externalListeners) {
            databaseEventListeners.registerDatabaseEventListener(databaseListener);
        }
        globalDependencies.satisfyDependencies(databaseEventListeners);

        cypherNotificationStats = new InternalNotificationStats();
        globalDependencies.satisfyDependencies(cypherNotificationStats);

        cypherSyntaxUsageStats = InternalSyntaxUsageStats.newImpl();
        globalDependencies.satisfyDependencies(cypherSyntaxUsageStats);

        var outOfDiskSpaceListener =
                new OutOfDiskSpaceListener(globalConfig, logService.getInternalLog(OutOfDiskSpaceListener.class));
        databaseEventListeners.registerDatabaseEventListener(outOfDiskSpaceListener);

        transactionEventListeners = createGlobalTransactionEventListeners();
        globalDependencies.satisfyDependency(transactionEventListeners);

        connectorPortRegister = new ConnectorPortRegister();
        globalDependencies.satisfyDependency(connectorPortRegister);

        capabilitiesService = loadCapabilities();
        globalDependencies.satisfyDependency(capabilitiesService);
        globalDependencies.satisfyDependency(
                tryResolveOrCreate(NativeAccess.class, NativeAccessProvider::getNativeAccess));
    }

    private Tracers createDefaultTracers() {
        String desiredImplementationName = globalConfig.get(GraphDatabaseInternalSettings.tracer);
        return new DefaultTracers(
                desiredImplementationName,
                logService.getInternalLog(DefaultTracers.class),
                globalMonitors,
                jobScheduler,
                globalClock,
                globalConfig);
    }

    private <T> T tryResolveOrCreate(Class<T> clazz, Supplier<T> newInstanceMethod) {
        return externalDependencyResolver.containsDependency(clazz)
                ? externalDependencyResolver.resolveDependency(clazz)
                : newInstanceMethod.get();
    }

    protected FileLockerService createFileLockerService() {
        return new GlobalLockerService();
    }

    protected SystemNanoClock createClock() {
        return Clocks.nanoClock();
    }

    public LifeSupport createLife() {
        return new LifeSupport();
    }

    protected FileSystemAbstraction createFileSystemAbstraction() {
        return new DefaultFileSystemAbstraction();
    }

    private FileSystemWatcherService createFileSystemWatcherService(
            FileSystemAbstraction fileSystem, LogService logging, JobScheduler jobScheduler, Config config) {
        if (!config.get(filewatcher_enabled)) {
            InternalLog log = logging.getInternalLog(getClass());
            log.info("File watcher disabled by configuration.");
            return FileSystemWatcherService.EMPTY_WATCHER;
        }

        try {
            return new DefaultFileSystemWatcherService(
                    jobScheduler, fileSystem.fileWatcher(), logging.getInternalLogProvider());
        } catch (Exception e) {
            InternalLog log = logging.getInternalLog(getClass());
            log.warn(
                    "Can not create file watcher for current file system. File monitoring capabilities for store files will be disabled.",
                    e);
            return FileSystemWatcherService.EMPTY_WATCHER;
        }
    }

    protected LogService createLogService(InternalLogProvider userLogProvider, boolean daemonMode) {
        userLogProvider = userLogProvider == null ? NullLogProvider.getInstance() : userLogProvider;
        InternalLogProvider internalLogProvider = NullLogProvider.getInstance();

        if (globalConfig.get(GraphDatabaseSettings.debug_log_enabled)) {
            Path xmlConfig = globalConfig.get(GraphDatabaseSettings.server_logging_config_path);
            boolean allowDefaultXmlConfig =
                    !globalConfig.isExplicitlySet(GraphDatabaseSettings.server_logging_config_path);
            @SuppressWarnings("Convert2MethodRef")
            Neo4jLoggerContext loggerContext = createLoggerFromXmlConfig(
                    fileSystem,
                    xmlConfig,
                    allowDefaultXmlConfig,
                    daemonMode,
                    globalConfig::configStringLookup,
                    log -> dbmsDiagnosticsManager.dumpAll(
                            log), // dbmsDiagnosticsManager is null here, but will be assigned later
                    DiagnosticsManager.class.getCanonicalName());

            loggerContext.getLogger(getClass()).info("Logging config in use: " + loggerContext.getConfigSourceInfo());
            internalLogProvider = new Log4jLogProvider(loggerContext);
        }

        SimpleLogService logService =
                new SimpleLogService(userLogProvider, internalLogProvider, globalConfig.get(duplication_user_messages));

        return globalLife.add(logService);
    }

    protected GlobalTransactionEventListeners createGlobalTransactionEventListeners() {
        return new DefaultGlobalTransactionEventListeners();
    }

    private JobScheduler createJobScheduler() {
        JobScheduler jobScheduler =
                JobSchedulerFactory.createInitialisedScheduler(globalClock, logService.getInternalLogProvider());
        jobScheduler.setParallelism(
                Group.INDEX_SAMPLING, globalConfig.get(GraphDatabaseInternalSettings.index_sampling_parallelism));
        jobScheduler.setParallelism(
                Group.INDEX_POPULATION, globalConfig.get(GraphDatabaseInternalSettings.index_population_parallelism));
        jobScheduler.setParallelism(
                Group.PAGE_CACHE_PRE_FETCHER, globalConfig.get(GraphDatabaseSettings.pagecache_scan_prefetch));
        return jobScheduler;
    }

    protected PageCache createPageCache(
            FileSystemAbstraction fileSystem,
            Config config,
            LogService logging,
            Tracers tracers,
            JobScheduler jobScheduler,
            SystemNanoClock clock,
            MemoryPools memoryPools) {
        InternalLog pageCacheLog = logging.getInternalLog(PageCache.class);
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem, config, tracers.getPageCacheTracer(), pageCacheLog, jobScheduler, clock, memoryPools);
        PageCache pageCache = pageCacheFactory.getOrCreatePageCache();

        if (config.get(GraphDatabaseInternalSettings.dump_configuration)) {
            pageCacheFactory.dumpConfiguration();
        }
        return pageCache;
    }

    private static CollectionsFactorySupplier createCollectionsFactorySupplier(
            Config config, LifeSupport life, LogService logService) {
        final TransactionStateMemoryAllocation allocation = config.get(tx_state_memory_allocation);
        if (allocation == TransactionStateMemoryAllocation.OFF_HEAP) {
            if (!UnsafeUtil.unsafeByteBufferAccessAvailable()) {
                var log = logService.getInternalLog(GlobalModule.class);
                log.warn(tx_state_memory_allocation.name() + " is set to " + TransactionStateMemoryAllocation.OFF_HEAP
                        + " but unsafe access to java.nio.DirectByteBuffer is not available. Defaulting to "
                        + TransactionStateMemoryAllocation.ON_HEAP + ".");
                return CollectionsFactorySupplier.ON_HEAP;
            }

            return createOffHeapCollectionsFactory(config, life);
        }
        return CollectionsFactorySupplier.ON_HEAP;
    }

    private static CollectionsFactorySupplier createOffHeapCollectionsFactory(Config config, LifeSupport life) {
        final CachingOffHeapBlockAllocator allocator = new CachingOffHeapBlockAllocator(
                config.get(tx_state_off_heap_max_cacheable_block_size), config.get(tx_state_off_heap_block_cache_size));
        final OffHeapBlockAllocator sharedBlockAllocator;
        final long maxMemory = config.get(tx_state_max_off_heap_memory);
        if (maxMemory > 0) {
            sharedBlockAllocator = new CapacityLimitingBlockAllocatorDecorator(
                    allocator, maxMemory, tx_state_max_off_heap_memory.name());
        } else {
            sharedBlockAllocator = allocator;
        }
        life.add(onShutdown(sharedBlockAllocator::release));
        return () -> new OffHeapCollectionsFactory(sharedBlockAllocator);
    }

    private CapabilitiesService loadCapabilities() {
        var service = CapabilitiesService.newCapabilities(globalConfig, globalDependencies);
        service.set(DBMSCapabilities.dbms_instance_version, Version.getNeo4jVersion());
        service.set(DBMSCapabilities.dbms_instance_kernel_version, Version.getKernelVersion());
        service.set(DBMSCapabilities.dbms_instance_edition, dbmsInfo.edition.toString());
        return service;
    }

    public FileWatcher getFileWatcher() {
        return fileSystemWatcher.getFileWatcher();
    }

    public ConnectorPortRegister getConnectorPortRegister() {
        return connectorPortRegister;
    }

    CollectionsFactorySupplier getCollectionsFactorySupplier() {
        return collectionsFactorySupplier;
    }

    public SystemNanoClock getGlobalClock() {
        return globalClock;
    }

    public JobScheduler getJobScheduler() {
        return jobScheduler;
    }

    public GlobalExtensions getGlobalExtensions() {
        return globalExtensions;
    }

    Iterable<ExtensionFactory<?>> getExtensionFactories() {
        return extensionFactories;
    }

    public Config getGlobalConfig() {
        return globalConfig;
    }

    public FileSystemAbstraction getFileSystem() {
        return fileSystem;
    }

    public BinarySupportedKernelVersions getBinarySupportedKernelVersions() {
        return binarySupportedKernelVersions;
    }

    public Tracers getTracers() {
        return tracers;
    }

    public Neo4jLayout getNeo4jLayout() {
        return neo4jLayout;
    }

    public DbmsInfo getDbmsInfo() {
        return dbmsInfo;
    }

    public LifeSupport getGlobalLife() {
        return globalLife;
    }

    public PageCache getPageCache() {
        return pageCache;
    }

    public Monitors getGlobalMonitors() {
        return globalMonitors;
    }

    public Dependencies getGlobalDependencies() {
        return globalDependencies;
    }

    public LogService getLogService() {
        return logService;
    }

    public CompositeDatabaseAvailabilityGuard getGlobalAvailabilityGuard() {
        return globalAvailabilityGuard;
    }

    public DatabaseEventListeners getDatabaseEventListeners() {
        return databaseEventListeners;
    }

    public GlobalTransactionEventListeners getTransactionEventListeners() {
        return transactionEventListeners;
    }

    public DependencyResolver getExternalDependencyResolver() {
        return externalDependencyResolver;
    }

    FileLockerService getFileLockerService() {
        return fileLockerService;
    }

    public MemoryPools getMemoryPools() {
        return memoryPools;
    }

    public GlobalMemoryGroupTracker getTransactionsMemoryPool() {
        return transactionsMemoryPool;
    }

    public GlobalMemoryGroupTracker getOtherMemoryPool() {
        return otherMemoryPool;
    }

    public CapabilitiesService getCapabilitiesService() {
        return capabilitiesService;
    }
}
