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
package org.neo4j.consistency;

import static org.neo4j.configuration.GraphDatabaseSettings.memory_tracking;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.factory.DbmsInfo.TOOL;
import static org.neo4j.kernel.impl.index.schema.SchemaIndexExtensionLoader.instantiateExtensions;
import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onShutdown;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static org.neo4j.logging.log4j.LogConfig.createLoggerFromXmlConfig;
import static org.neo4j.logging.log4j.LogUtils.newLoggerBuilder;
import static org.neo4j.logging.log4j.LogUtils.newTemporaryXmlConfigBuilder;
import static org.neo4j.logging.log4j.LoggerTarget.ROOT_LOGGER;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.io.FilenameUtils;
import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.logging.LoggingReporterFactoryInvocationHandler;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.CommonDatabaseStores;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.state.StaticIndexProviderMapFactory;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.recovery.LogTailExtractor;
import org.neo4j.logging.DuplicatingLog;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.Clocks;

public class ConsistencyCheckService {
    private static final long DEFAULT_SMALL_MAX_OFF_HEAP_MEMORY = mebiBytes(80);
    private static final long MIN_OFF_HEAP_CACHING_MEMORY = mebiBytes(8);

    private final Date timestamp;
    private final DatabaseLayout layout;
    private final Config config;
    private final OutputStream progressOutput;
    private final InternalLogProvider logProvider;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final boolean verbose;
    private final Path reportPath;
    private final ConsistencyFlags consistencyFlags;
    private final PageCacheTracer pageCacheTracer;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker memoryTracker;
    private final long maxOffHeapMemory;
    private final int numberOfThreads;

    public ConsistencyCheckService(DatabaseLayout layout) {
        this(
                new Date(),
                layout,
                Config.defaults(),
                null,
                NullLogProvider.getInstance(),
                new DefaultFileSystemAbstraction(),
                null,
                false,
                null,
                ConsistencyFlags.ALL,
                PageCacheTracer.NULL,
                new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER),
                EmptyMemoryTracker.INSTANCE,
                DEFAULT_SMALL_MAX_OFF_HEAP_MEMORY,
                Runtime.getRuntime().availableProcessors());
    }

    private ConsistencyCheckService(
            Date timestamp,
            DatabaseLayout layout,
            Config config,
            OutputStream progressOutput,
            InternalLogProvider logProvider,
            FileSystemAbstraction fileSystem,
            PageCache pageCache,
            boolean verbose,
            Path reportPath,
            ConsistencyFlags consistencyFlags,
            PageCacheTracer pageCacheTracer,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            long maxOffHeapMemory,
            int numberOfThreads) {
        this.timestamp = timestamp;
        this.layout = layout;
        this.config = config;
        this.progressOutput = progressOutput;
        this.logProvider = logProvider;
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.verbose = verbose;
        this.reportPath = reportPath;
        this.consistencyFlags = consistencyFlags;
        this.pageCacheTracer = pageCacheTracer;
        this.contextFactory = contextFactory;
        this.memoryTracker = memoryTracker;
        this.maxOffHeapMemory = maxOffHeapMemory;
        this.numberOfThreads = numberOfThreads;
    }

    public ConsistencyCheckService with(CursorContextFactory contextFactory) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public ConsistencyCheckService with(Date timestamp) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public ConsistencyCheckService with(DatabaseLayout layout) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public ConsistencyCheckService with(Config config) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public ConsistencyCheckService with(OutputStream progressOutput) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public ConsistencyCheckService with(InternalLogProvider logProvider) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public ConsistencyCheckService with(FileSystemAbstraction fileSystem) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public ConsistencyCheckService with(PageCache pageCache) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public ConsistencyCheckService verbose(boolean verbose) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public ConsistencyCheckService with(Path reportPath) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public ConsistencyCheckService with(ConsistencyFlags consistencyFlags) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public ConsistencyCheckService with(PageCacheTracer pageCacheTracer) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public ConsistencyCheckService with(MemoryTracker memoryTracker) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public ConsistencyCheckService withMaxOffHeapMemory(long maxOffHeapMemory) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public ConsistencyCheckService withNumberOfThreads(int numberOfThreads) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportPath,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker,
                maxOffHeapMemory,
                numberOfThreads);
    }

    public Result runFullConsistencyCheck() throws ConsistencyCheckIncompleteException {
        try (var life = new Lifespan()) {
            var config = this.config;
            var pageCache = this.pageCache;
            long pageCacheMemory;
            long offHeapCachingMemory;
            var storageEngineFactory =
                    StorageEngineFactory.selectStorageEngine(fileSystem, layout).orElseThrow();
            var storeSize = storeSize(storageEngineFactory);
            if (pageCache == null) {
                // Now that there's no existing page cache we have the opportunity to allocate one with an optimally
                // sized given the max amount of memory we've been given.
                var distribution = ConsistencyCheckMemoryCalculation.calculate(
                        maxOffHeapMemory, storeSize, calculateOptimalOffHeapMemoryForChecker());
                pageCacheMemory = distribution.pageCacheMemory();
                offHeapCachingMemory = distribution.offHeapCachingMemory();

                config = Config.newBuilder()
                        .fromConfig(config)
                        .set(GraphDatabaseSettings.pagecache_memory, pageCacheMemory)
                        .build();
                pageCache = createPageCache(life, config);
                printMemoryConfiguration(false, storeSize, pageCacheMemory, offHeapCachingMemory);
            } else {
                pageCacheMemory = pageCache.maxCachedPages() * pageCache.pageSize();
                offHeapCachingMemory = Long.max(maxOffHeapMemory - pageCacheMemory, MIN_OFF_HEAP_CACHING_MEMORY);
                printMemoryConfiguration(true, storeSize, pageCacheMemory, offHeapCachingMemory);
            }

            config.set(GraphDatabaseSettings.pagecache_warmup_enabled, false);

            // assert recovered
            final var databaseLayout = storageEngineFactory.formatSpecificDatabaseLayout(layout);
            assertRecovered(databaseLayout, pageCache, config, fileSystem, memoryTracker);

            assertSupportedFormat(
                    databaseLayout, config, fileSystem, pageCache, storageEngineFactory, logProvider, contextFactory);

            // instantiate the inconsistencies report logging
            final var outLog = logProvider.getLog(getClass());
            final var reportFile = chooseReportFile(config);
            final var reportLogProvider = new Log4jLogProvider(createLoggerFromXmlConfig(
                    fileSystem,
                    newTemporaryXmlConfigBuilder(fileSystem)
                            .withLogger(newLoggerBuilder(ROOT_LOGGER, reportFile)
                                    .withLevel(Level.INFO)
                                    .createOnDemand()
                                    .withCategory(false)
                                    .build())
                            .create(),
                    false,
                    config::configStringLookup));
            life.add(onShutdown(reportLogProvider::close));
            final var reportFileLog = reportLogProvider.getLog(getClass());
            final var reportLog = new DuplicatingLog(outLog, reportFileLog);

            // instantiate kernel extensions and the StaticIndexProviderMapFactory thing
            final var jobScheduler = life.add(JobSchedulerFactory.createInitialisedScheduler());
            final var recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.ignore();
            final var monitors = new Monitors();
            final var tokenHolders = storageEngineFactory.loadReadOnlyTokens(
                    fileSystem,
                    databaseLayout,
                    config,
                    pageCache,
                    pageCacheTracer,
                    true,
                    contextFactory,
                    memoryTracker);
            final var extensions = life.add(instantiateExtensions(
                    databaseLayout,
                    fileSystem,
                    config,
                    new SimpleLogService(logProvider),
                    pageCache,
                    jobScheduler,
                    recoveryCleanupWorkCollector,
                    TOOL,
                    monitors,
                    tokenHolders,
                    pageCacheTracer,
                    readOnly()));

            Dependencies dependencies = new Dependencies(extensions);
            dependencies.satisfyDependency(VersionStorage.EMPTY_STORAGE);
            final var indexProviders = life.add(StaticIndexProviderMapFactory.create(
                    life,
                    config,
                    pageCache,
                    fileSystem,
                    new SimpleLogService(logProvider),
                    monitors,
                    readOnly(),
                    HostedOnMode.SINGLE,
                    recoveryCleanupWorkCollector,
                    databaseLayout,
                    tokenHolders,
                    jobScheduler,
                    contextFactory,
                    pageCacheTracer,
                    dependencies));

            // do the consistency check
            life.start();
            final var summary = new ConsistencySummaryStatistics();

            if (consistencyFlags.checkIndexes()
                    && consistencyFlags.checkStructure()
                    && fileSystem.fileExists(databaseLayout.pathForStore(CommonDatabaseStores.INDEX_STATISTICS))) {
                var openOptions =
                        storageEngineFactory.getStoreOpenOptions(fileSystem, pageCache, databaseLayout, contextFactory);
                final var statisticsStore = new IndexStatisticsStore(
                        pageCache,
                        fileSystem,
                        databaseLayout,
                        recoveryCleanupWorkCollector,
                        true,
                        contextFactory,
                        pageCacheTracer,
                        openOptions);
                life.add(statisticsStore);
                consistencyCheckOnStatisticsStore(reportLog, summary, statisticsStore);
            }

            final var logTailExtractor =
                    new LogTailExtractor(fileSystem, config, storageEngineFactory, DatabaseTracers.EMPTY);

            storageEngineFactory.consistencyCheck(
                    fileSystem,
                    databaseLayout,
                    config,
                    pageCache,
                    indexProviders,
                    reportLog,
                    outLog,
                    summary,
                    numberOfThreads,
                    offHeapCachingMemory,
                    progressOutput,
                    verbose,
                    consistencyFlags,
                    contextFactory,
                    pageCacheTracer,
                    logTailExtractor.getTailMetadata(databaseLayout, memoryTracker),
                    memoryTracker);

            if (!summary.isConsistent()) {
                reportLog.warn("Inconsistencies found: " + summary);
                outLog.warn("See '%s' for a detailed consistency report.", reportFile);
                return Result.failure(reportFile, summary);
            }
            return Result.success(reportFile, summary);

        } catch (IOException | RuntimeException e) {
            throw new ConsistencyCheckIncompleteException(e);
        }
    }

    private void printMemoryConfiguration(
            boolean providedPageCache, long storeSize, long pageCacheMemory, long offHeapCachingMemory) {
        if (progressOutput != null) {
            new PrintStream(progressOutput, true)
                    .printf(
                            "Running consistency check with max off-heap:%s%n  Store size:%s"
                                    + "%n  %s page cache:%s"
                                    + "%n  Off-heap memory for caching:%s%n",
                            bytesToString(maxOffHeapMemory),
                            bytesToString(storeSize),
                            providedPageCache ? "Provided" : "Allocated",
                            bytesToString(pageCacheMemory),
                            bytesToString(offHeapCachingMemory));
        }
    }

    private long storeSize(StorageEngineFactory storageEngineFactory) throws ConsistencyCheckIncompleteException {
        try {
            var size = 0L;
            for (var storageFile : storageEngineFactory.listStorageFiles(fileSystem, layout)) {
                try {
                    size += fileSystem.getFileSize(storageFile);
                } catch (IOException e) {
                    // Could not get size of this particular store file, missing?
                }
            }
            return size;
        } catch (IOException e) {
            throw new ConsistencyCheckIncompleteException(e);
        }
    }

    private PageCache createPageCache(Lifespan life, Config config) {
        final var jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        life.add(onShutdown(jobScheduler::close));
        final var pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem,
                config,
                pageCacheTracer,
                logProvider.getLog(PageCache.class),
                jobScheduler,
                Clocks.nanoClock(),
                new MemoryPools(config.get(memory_tracking)),
                pageCacheConfig -> pageCacheConfig.faultLockStriping(1 << 11));
        final var pageCache = pageCacheFactory.getOrCreatePageCache();
        life.add(onShutdown(pageCache::close));
        return pageCache;
    }

    /**
     * Starts a tiny page cache just to ask the store about rough number of entities in it. Based on that the storage can then decide
     * what the optimal amount of available off-heap memory should be when running the consistency check.
     */
    private long calculateOptimalOffHeapMemoryForChecker() {
        try (JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
                var tempPageCache = new ConfiguringPageCacheFactory(
                                fileSystem,
                                Config.defaults(GraphDatabaseSettings.pagecache_memory, mebiBytes(8)),
                                PageCacheTracer.NULL,
                                NullLog.getInstance(),
                                jobScheduler,
                                Clocks.nanoClock(),
                                new MemoryPools())
                        .getOrCreatePageCache()) {
            final var storageEngineFactory =
                    StorageEngineFactory.selectStorageEngine(fileSystem, layout).orElseThrow();
            return Long.max(
                    MIN_OFF_HEAP_CACHING_MEMORY,
                    storageEngineFactory.optimalAvailableConsistencyCheckerMemory(
                            fileSystem, layout, config, tempPageCache));
        }
    }

    private static void assertSupportedFormat(
            DatabaseLayout databaseLayout,
            Config config,
            FileSystemAbstraction fileSystem,
            PageCache pageCache,
            StorageEngineFactory storageEngineFactory,
            InternalLogProvider logProvider,
            CursorContextFactory contextFactory) {
        final var storeVersionCheck = storageEngineFactory.versionCheck(
                fileSystem, databaseLayout, config, pageCache, new SimpleLogService(logProvider), contextFactory);

        try (var cursorContext = contextFactory.create("consistencyCheck")) {
            if (!storeVersionCheck.isCurrentStoreVersionFullySupported(cursorContext)) {
                throw new IllegalStateException(
                        "The store must be upgraded or migrated to a supported version before it is possible to "
                                + "check consistency");
            }
        }
    }

    private void consistencyCheckOnStatisticsStore(
            InternalLog log, ConsistencySummaryStatistics summary, ConsistencyCheckable checkable) {
        LoggingReporterFactoryInvocationHandler handler = new LoggingReporterFactoryInvocationHandler(log, true);
        ReporterFactory proxyFactory = new ReporterFactory(handler);

        checkable.consistencyCheck(proxyFactory, NULL_CONTEXT_FACTORY, numberOfThreads);
        summary.update("INDEX_STATISTICS", handler.errors(), handler.warnings());
    }

    private static void assertRecovered(
            DatabaseLayout databaseLayout,
            PageCache pageCache,
            Config config,
            FileSystemAbstraction fileSystem,
            MemoryTracker memoryTracker)
            throws IOException {
        if (isRecoveryRequired(
                fileSystem,
                pageCache,
                databaseLayout,
                config,
                Optional.empty(),
                memoryTracker,
                DatabaseTracers.EMPTY)) {
            throw new IllegalStateException(
                    """
                    Active logical log detected, this might be a source of inconsistencies.
                    Please recover database.
                    To perform recovery please start database in single mode and perform clean shutdown.""");
        }
    }

    private Path chooseReportFile(Config config) {
        final var path = Objects.requireNonNullElse(reportPath, defaultReportDir(config))
                .toAbsolutePath()
                .normalize();
        return FilenameUtils.isExtension(path.toString(), "report")
                ? path
                : path.resolve(defaultLogFileName(timestamp));
    }

    private static Path defaultReportDir(Config config) {
        return config.get(GraphDatabaseSettings.logs_directory);
    }

    private static String defaultLogFileName(Date date) {
        return "inconsistencies-%s.report".formatted(new SimpleDateFormat("yyyy-MM-dd.HH.mm.ss").format(date));
    }

    public static class Result {
        private final boolean successful;
        private final Path reportFile;
        private final ConsistencySummaryStatistics summary;

        public static Result failure(Path reportFile, ConsistencySummaryStatistics summary) {
            return new Result(false, reportFile, summary);
        }

        public static Result success(Path reportFile, ConsistencySummaryStatistics summary) {
            return new Result(true, reportFile, summary);
        }

        private Result(boolean successful, Path reportFile, ConsistencySummaryStatistics summary) {
            this.successful = successful;
            this.reportFile = reportFile;
            this.summary = summary;
        }

        public boolean isSuccessful() {
            return successful;
        }

        public Path reportFile() {
            return reportFile;
        }

        public ConsistencySummaryStatistics summary() {
            return summary;
        }
    }
}
