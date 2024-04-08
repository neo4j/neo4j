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
package org.neo4j.internal.batchimport.store;

import static java.lang.Math.min;
import static java.nio.file.StandardOpenOption.READ;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_iops_limit;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.io.IOUtils.uncheckedConsumer;
import static org.neo4j.io.mem.MemoryAllocator.createAllocator;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_ARRAY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_STRING;
import static org.neo4j.kernel.impl.store.StoreType.RELATIONSHIP_GROUP;
import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.CHECKPOINT_FILE_PREFIX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.function.Predicate;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.pagecache.ConfigurableIOBufferFactory;
import org.neo4j.counts.CountsStore;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.store.io.IoTracer;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.counts.CountsStoreProvider;
import org.neo4j.internal.counts.DegreeStoreProvider;
import org.neo4j.internal.counts.GBPTreeRelationshipGroupDegreesStore;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.io.pagecache.ExternallyManagedPageCache;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.PageCacheOptionsSelector;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.RecordStorageCapability;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;

/**
 * Creator and accessor of {@link NeoStores} with some logic to provide very batch friendly services to the
 * {@link NeoStores} when instantiating it. Different services for specific purposes.
 */
public class BatchingNeoStores implements AutoCloseable, MemoryStatsVisitor.Visitable {
    static final long MAX_PAGE_CACHE_MEMORY = gibiBytes(1);
    private static final String BATCHING_STORE_CREATION_TAG = "batchingStoreCreation";
    private static final String BATCHING_STORE_SHUTDOWN_TAG = "batchingStoreShutdown";

    private static final String TEMP_STORE_NAME = "temp";
    // Empirical and slightly defensive threshold where relationship records seem to start requiring double record
    // units.
    // Basically decided by picking a maxId of pointer (as well as node ids) in the relationship record and randomizing
    // its data,
    // seeing which is a maxId where records starts to require a secondary unit.
    static final long DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD = 1L << 33;
    private static final StoreType[] TEMP_STORE_TYPES = {RELATIONSHIP_GROUP, PROPERTY, PROPERTY_ARRAY, PROPERTY_STRING};

    private final FileSystemAbstraction fileSystem;
    private final InternalLogProvider internalLogProvider;
    private final InternalLogProvider userLogProvider;
    private final RecordDatabaseLayout databaseLayout;
    private final RecordDatabaseLayout temporaryDatabaseLayout;
    private final Config neo4jConfig;
    private final Configuration importConfiguration;
    private final PageCache pageCache;
    private final IoTracer ioTracer;
    private final RecordFormats recordFormats;
    private final AdditionalInitialIds initialIds;
    private final boolean externalPageCache;
    private final IdGeneratorFactory idGeneratorFactory;
    private final IdGeneratorFactory tempIdGeneratorFactory;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker memoryTracker;
    private final String databaseName;
    private final ImmutableSet<OpenOption> openOptions;
    private final PageCacheTracer pageCacheTracer;

    // Some stores are considered temporary during the import and will be reordered/restructured
    // into the main store. These temporary stores will live here
    private NeoStores neoStores;
    private NeoStores temporaryNeoStores;
    private TokenHolders tokenHolders;
    private PageCacheFlusher flusher;
    private final LogTailLogVersionsMetadata logTailMetadata;
    private boolean doubleRelationshipRecordUnits;

    private boolean successful;

    private BatchingNeoStores(
            FileSystemAbstraction fileSystem,
            PageCache pageCache,
            RecordDatabaseLayout databaseLayout,
            Config neo4jConfig,
            Configuration importConfiguration,
            LogService logService,
            AdditionalInitialIds initialIds,
            LogTailLogVersionsMetadata logTailMetadata,
            boolean externalPageCache,
            IoTracer ioTracer,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            PageCacheTracer pageCacheTracer) {
        this.fileSystem = fileSystem;
        this.recordFormats = RecordFormatSelector.selectForStoreOrConfigForNewDbs(
                neo4jConfig,
                databaseLayout,
                fileSystem,
                pageCache,
                logService.getInternalLogProvider(),
                contextFactory);
        this.importConfiguration = importConfiguration;
        this.initialIds = initialIds;
        this.internalLogProvider = logService.getInternalLogProvider();
        this.userLogProvider = logService.getUserLogProvider();
        this.databaseLayout = databaseLayout;
        this.temporaryDatabaseLayout = RecordDatabaseLayout.ofFlat(databaseLayout.file(TEMP_STORE_NAME));
        this.neo4jConfig = neo4jConfig;
        this.pageCache = pageCache;
        this.ioTracer = ioTracer;
        this.externalPageCache = externalPageCache;
        this.databaseName = databaseLayout.getDatabaseName();
        this.idGeneratorFactory = new DefaultIdGeneratorFactory(fileSystem, immediate(), pageCacheTracer, databaseName);
        this.tempIdGeneratorFactory =
                new DefaultIdGeneratorFactory(fileSystem, immediate(), pageCacheTracer, databaseName);
        this.contextFactory = contextFactory;
        this.memoryTracker = memoryTracker;
        this.logTailMetadata = logTailMetadata;
        this.pageCacheTracer = pageCacheTracer;
        this.openOptions = PageCacheOptionsSelector.select(recordFormats);
    }

    /**
     * Called when expecting a clean {@code storeDir} folder and where a new store will be created.
     * This happens on an initial attempt to import.
     *
     * @throws IllegalStateException if {@code storeDir} already contains a database.
     */
    public void createNew() throws IOException {
        assertDatabaseIsNonExistent();

        deleteIndexes();
        deleteCountsStore();

        instantiateStores();
    }

    private void deleteIndexes() throws IOException {
        // There might have been a previous import which was killed before it even started, where the token indexes
        // could
        // be in a semi-initialized state. Better to be on the safe side and deleted them. We get here after determining
        // that
        // the db is either completely empty or non-existent anyway, so deleting this file is OK.
        Path indexDirectory = IndexDirectoryStructure.baseSchemaIndexFolder(databaseLayout.databaseDirectory());
        fileSystem.deleteRecursively(indexDirectory);
    }

    private void deleteCountsStore() throws IOException {
        if (fileSystem.fileExists(databaseLayout.countStore())) {
            fileSystem.deleteFile(databaseLayout.countStore());
        }
    }

    public void assertDatabaseIsNonExistent() throws DirectoryNotEmptyException {
        if (hasExistingDatabaseContents()) {
            throw new DirectoryNotEmptyException(
                    databaseLayout.databaseDirectory() + " already contains data, cannot do import here");
        }

        if (hasExistingTransactionContents()) {
            throw new DirectoryNotEmptyException(
                    databaseLayout.getTransactionLogsDirectory() + " already contains data, cannot do import here");
        }
    }

    private boolean hasExistingTransactionContents() {
        TransactionLogFilesHelper logFilesHelper =
                new TransactionLogFilesHelper(fileSystem, databaseLayout.getTransactionLogsDirectory());
        TransactionLogFilesHelper checkpointFilesHelper = new TransactionLogFilesHelper(
                fileSystem, databaseLayout.getTransactionLogsDirectory(), CHECKPOINT_FILE_PREFIX);
        try {
            return logFilesHelper.getMatchedFiles().length > 0 || checkpointFilesHelper.getMatchedFiles().length > 0;
        } catch (IOException e) {
            // Could not check txlogs (does not exist?) Do nothing
            return false;
        }
    }

    private boolean hasExistingDatabaseContents() {
        Path metaDataFile = databaseLayout.metadataStore();
        try (PagedFile pagedFile =
                pageCache.map(metaDataFile, pageCache.pageSize(), databaseName, immutable.of(READ))) {
            // OK so the db probably exists
        } catch (IOException e) {
            // It's OK
            return false;
        }

        try (NeoStores stores = newStoreFactory(databaseLayout, idGeneratorFactory, contextFactory, immutable.empty())
                .openNeoStores(StoreType.NODE, StoreType.RELATIONSHIP)) {
            return stores.getNodeStore().getIdGenerator().getHighId() > 0
                    || stores.getRelationshipStore().getIdGenerator().getHighId() > 0;
        }
    }

    /**
     * Called when expecting a previous attempt/state of a database to open, where some store files should be kept,
     * but others deleted. All temporary stores will be deleted in this call.
     *
     * @param mainStoresToKeep {@link Predicate} controlling which files to keep, i.e. {@code true} means keep, {@code false} means delete.
     * @param tempStoresToKeep {@link Predicate} controlling which files to keep, i.e. {@code true} means keep, {@code false} means delete.
     */
    public void pruneAndOpenExistingStore(Predicate<StoreType> mainStoresToKeep, Predicate<StoreType> tempStoresToKeep)
            throws IOException {
        deleteStoreFiles(temporaryDatabaseLayout, tempStoresToKeep);
        deleteStoreFiles(databaseLayout, mainStoresToKeep);
        instantiateStores();
    }

    private void deleteStoreFiles(DatabaseLayout databaseLayout, Predicate<StoreType> storesToKeep) {
        for (StoreType type : StoreType.STORE_TYPES) {
            if (!storesToKeep.test(type)) {
                DatabaseFile databaseFile = type.getDatabaseFile();
                databaseLayout.allFiles(databaseFile).forEach(uncheckedConsumer(fileSystem::deleteFile));
            }
        }
    }

    private void instantiateStores() throws IOException {
        neoStores = newStoreFactory(databaseLayout, idGeneratorFactory, contextFactory, immutable.empty())
                .openAllNeoStores();
        DynamicAllocatorProvider allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);
        tokenHolders = StoreTokens.directTokenHolders(neoStores, allocatorProvider, contextFactory, memoryTracker);

        temporaryNeoStores = instantiateTempStores();

        try (var cursorContext = contextFactory.create(BATCHING_STORE_CREATION_TAG)) {
            neoStores.start(cursorContext);
            temporaryNeoStores.start(cursorContext);
            MetaDataStore metaDataStore = neoStores.getMetaDataStore();
            metaDataStore.setLastCommittedAndClosedTransactionId(
                    initialIds.lastCommittedTransactionId(),
                    logTailMetadata.getLastCommittedTransaction().kernelVersion(),
                    initialIds.lastCommittedTransactionChecksum(),
                    BASE_TX_COMMIT_TIMESTAMP,
                    UNKNOWN_CONSENSUS_INDEX,
                    initialIds.lastCommittedTransactionLogByteOffset(),
                    initialIds.lastCommittedTransactionLogVersion());
            metaDataStore.setCheckpointLogVersion(initialIds.checkpointLogVersion());
        }
    }

    private NeoStores instantiateTempStores() {
        return newStoreFactory(temporaryDatabaseLayout, tempIdGeneratorFactory, contextFactory, immutable.empty())
                .openNeoStores(TEMP_STORE_TYPES);
    }

    public static BatchingNeoStores batchingNeoStores(
            FileSystemAbstraction fileSystem,
            RecordDatabaseLayout databaseLayout,
            Configuration config,
            LogService logService,
            AdditionalInitialIds initialIds,
            LogTailLogVersionsMetadata logTailMetadata,
            Config dbConfig,
            JobScheduler jobScheduler,
            PageCacheTracer pageCacheTracer,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        Config neo4jConfig = getNeo4jConfig(dbConfig);
        ExternallyManagedPageCache providedPageCache = config.providedPageCache();
        PageCache pageCache = providedPageCache != null
                ? providedPageCache
                : createPageCache(fileSystem, neo4jConfig, pageCacheTracer, jobScheduler, memoryTracker);

        return new BatchingNeoStores(
                fileSystem,
                pageCache,
                databaseLayout,
                neo4jConfig,
                config,
                logService,
                initialIds,
                logTailMetadata,
                false,
                pageCacheTracer::bytesWritten,
                contextFactory,
                memoryTracker,
                pageCacheTracer);
    }

    public static BatchingNeoStores batchingNeoStoresWithExternalPageCache(
            FileSystemAbstraction fileSystem,
            PageCache pageCache,
            PageCacheTracer tracer,
            CursorContextFactory contextFactory,
            RecordDatabaseLayout databaseLayout,
            Configuration config,
            LogService logService,
            AdditionalInitialIds initialIds,
            LogTailLogVersionsMetadata logTailMetadata,
            Config dbConfig,
            MemoryTracker memoryTracker) {
        return new BatchingNeoStores(
                fileSystem,
                pageCache,
                databaseLayout,
                getNeo4jConfig(dbConfig),
                config,
                logService,
                initialIds,
                logTailMetadata,
                true,
                tracer::bytesWritten,
                contextFactory,
                memoryTracker,
                tracer);
    }

    private static Config getNeo4jConfig(Config dbConfig) {
        dbConfig.set(pagecache_memory, pageCacheMemory(dbConfig));
        dbConfig.set(check_point_iops_limit, -1);
        return dbConfig;
    }

    /**
     * @return amount of memory to reserve for the page cache. This should just be "enough" for it to be able
     * to sequentially read and write a couple of stores at a time. If configured too high then there will
     * be less memory available for other caches which are critical during the import. Optimal size is
     * estimated to be 100-200 MiB. The importer will figure out an optimal page size from this value,
     * with slightly bigger page size than "normal" random access use cases.
     */
    private static long pageCacheMemory(Config dbConfig) {
        var fromSetting = dbConfig.get(pagecache_memory);
        if (fromSetting != null) {
            return Long.min(MAX_PAGE_CACHE_MEMORY, fromSetting);
        }

        // Get the upper bound of what we can get from the default config calculation
        // We even want to limit amount of memory a bit more since we don't need very much during import
        long maxFreeMemory = OsBeanUtil.getFreePhysicalMemory();
        if (0 < maxFreeMemory && maxFreeMemory < Long.MAX_VALUE) {
            // We got a reading of amount of free memory from the OS, use this to potentially reduce the page cache
            // size if the amount of free memory is very small.
            return min(MAX_PAGE_CACHE_MEMORY, maxFreeMemory);
        }
        // We couldn't get a proper reading from the OS, just allocate the default page cache size,
        // which is quite small and optimal in terms of performance.
        return MAX_PAGE_CACHE_MEMORY;
    }

    private static PageCache createPageCache(
            FileSystemAbstraction fileSystem,
            Config config,
            PageCacheTracer tracer,
            JobScheduler jobScheduler,
            MemoryTracker memoryTracker) {
        SingleFilePageSwapperFactory swapperFactory =
                new SingleFilePageSwapperFactory(fileSystem, tracer, EmptyMemoryTracker.INSTANCE);
        MemoryAllocator memoryAllocator = createAllocator(config.get(pagecache_memory), memoryTracker);
        MuninnPageCache.Configuration configuration = MuninnPageCache.config(memoryAllocator)
                .pageCacheTracer(tracer)
                .memoryTracker(memoryTracker)
                .bufferFactory(new ConfigurableIOBufferFactory(config, memoryTracker))
                .faultLockStriping(1 << 11)
                .reservedPageBytes(PageCache.RESERVED_BYTES)
                .disableEvictionThread();
        return new MuninnPageCache(swapperFactory, jobScheduler, configuration);
    }

    private StoreFactory newStoreFactory(
            RecordDatabaseLayout databaseLayout,
            IdGeneratorFactory idGeneratorFactory,
            CursorContextFactory contextFactory,
            ImmutableSet<OpenOption> openOptions) {
        return new StoreFactory(
                databaseLayout,
                neo4jConfig,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                fileSystem,
                recordFormats,
                internalLogProvider,
                contextFactory,
                false,
                logTailMetadata,
                openOptions);
    }

    /**
     * @return temporary relationship group store which will be deleted in {@link #close()}.
     */
    public RecordStore<RelationshipGroupRecord> getTemporaryRelationshipGroupStore() {
        return temporaryNeoStores.getRelationshipGroupStore();
    }

    /**
     * @return temporary property store which will be deleted in {@link #close()}.
     */
    public PropertyStore getTemporaryPropertyStore() {
        return temporaryNeoStores.getPropertyStore();
    }

    public IoTracer getIoTracer() {
        return ioTracer;
    }

    public NodeStore getNodeStore() {
        return neoStores.getNodeStore();
    }

    public PropertyStore getPropertyStore() {
        return neoStores.getPropertyStore();
    }

    public RelationshipStore getRelationshipStore() {
        return neoStores.getRelationshipStore();
    }

    public RelationshipGroupStore getRelationshipGroupStore() {
        return neoStores.getRelationshipGroupStore();
    }

    public void buildCountsStore(
            CountsBuilder builder, CursorContextFactory contextFactory, MemoryTracker memoryTracker) {
        try {
            deleteCountsStore();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (var countsStore = openCountsStore(
                        pageCache,
                        fileSystem,
                        databaseLayout,
                        userLogProvider,
                        RecoveryCleanupWorkCollector.immediate(),
                        builder,
                        neo4jConfig,
                        contextFactory,
                        pageCacheTracer);
                var cursorContext = contextFactory.create("buildCountsStore")) {
            countsStore.start(cursorContext, memoryTracker);
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                countsStore.checkpoint(flushEvent, cursorContext);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // Also build an empty relationship group degrees store since the importer will not make any group degrees
        // external.
        // This will prevent an unnecessary rebuild on the first startup.
        try (var groupDegreesStore = DegreeStoreProvider.getInstance()
                        .openDegreesStore(
                                pageCache,
                                fileSystem,
                                databaseLayout,
                                userLogProvider,
                                immediate(),
                                neo4jConfig,
                                contextFactory,
                                pageCacheTracer,
                                new GBPTreeRelationshipGroupDegreesStore.EmptyDegreesRebuilder(
                                        neoStores.getMetaDataStore().getLastCommittedTransactionId()),
                                openOptions,
                                false,
                                VersionStorage.EMPTY_STORAGE);
                var cursorContext = contextFactory.create("buildRelationshipDegreesStore")) {
            groupDegreesStore.start(cursorContext, memoryTracker);
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                groupDegreesStore.checkpoint(flushEvent, cursorContext);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private CountsStore openCountsStore(
            PageCache pageCache,
            FileSystemAbstraction fs,
            RecordDatabaseLayout layout,
            InternalLogProvider userLogProvider,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            CountsBuilder builder,
            Config config,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer) {
        return CountsStoreProvider.getInstance()
                .openCountsStore(
                        pageCache,
                        fs,
                        layout,
                        userLogProvider,
                        recoveryCleanupWorkCollector,
                        config,
                        contextFactory,
                        pageCacheTracer,
                        getOpenOptions(),
                        builder,
                        false,
                        VersionStorage.EMPTY_STORAGE);
    }

    @Override
    public void close() throws IOException {
        // Here as a safety mechanism when e.g. panicking.
        markHighIds();
        if (flusher != null) {
            stopFlushingPageCache();
        }

        try (var cursorContext = contextFactory.create(BATCHING_STORE_SHUTDOWN_TAG)) {
            flushAndForce(cursorContext);
        }

        // Close the neo store
        closeAll(neoStores, temporaryNeoStores);
        if (!externalPageCache) {
            pageCache.close();
        }

        if (successful) {
            cleanup();
        }
    }

    public void markHighIds() {
        if (neoStores != null) {
            idGeneratorFactory.visit(IdGenerator::markHighestWrittenAtHighId);
        }
    }

    private void cleanup() throws IOException {
        Path tempDbDirectory = temporaryDatabaseLayout.databaseDirectory();
        if (!tempDbDirectory.getParent().equals(databaseLayout.databaseDirectory())) {
            throw new IllegalStateException(
                    "Temporary store is dislocated. It should be located under current database directory but instead located in: "
                            + tempDbDirectory.getParent());
        }
        fileSystem.deleteRecursively(tempDbDirectory);
    }

    public long getLastCommittedTransactionId() {
        return neoStores.getMetaDataStore().getLastCommittedTransactionId();
    }

    public NeoStores getNeoStores() {
        return neoStores;
    }

    public NeoStores getTemporaryNeoStores() {
        return temporaryNeoStores;
    }

    public TokenHolders getTokenHolders() {
        return tokenHolders;
    }

    public void startFlushingPageCache() {
        if (importConfiguration.sequentialBackgroundFlushing()) {
            if (flusher != null) {
                throw new IllegalStateException("Flusher already started");
            }
            flusher = new PageCacheFlusher(pageCache);
            flusher.start();
        }
    }

    public void stopFlushingPageCache() {
        if (importConfiguration.sequentialBackgroundFlushing()) {
            if (flusher == null) {
                throw new IllegalStateException("Flusher not started");
            }
            flusher.halt();
            flusher = null;
        }
    }

    @Override
    public void acceptMemoryStatsVisitor(MemoryStatsVisitor visitor) {
        visitor.offHeapUsage(pageCache.maxCachedPages() * pageCache.pageSize());
    }

    public PageCache getPageCache() {
        return pageCache;
    }

    public void flushAndForce(CursorContext cursorContext) throws IOException {
        if (neoStores != null) {
            neoStores.flush(DatabaseFlushEvent.NULL, cursorContext);
        }
        if (temporaryNeoStores != null) {
            temporaryNeoStores.flush(DatabaseFlushEvent.NULL, cursorContext);
        }
    }

    public FileSystemAbstraction fileSystem() {
        return fileSystem;
    }

    public DatabaseLayout databaseLayout() {
        return databaseLayout;
    }

    public RecordFormats recordFormats() {
        return recordFormats;
    }

    public void success() {
        successful = true;
    }

    public boolean determineDoubleRelationshipRecordUnits(Input.Estimates inputEstimates) {
        doubleRelationshipRecordUnits = recordFormats.hasCapability(RecordStorageCapability.SECONDARY_RECORD_UNITS)
                && inputEstimates.numberOfRelationships() > DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD;
        return doubleRelationshipRecordUnits;
    }

    public boolean usesDoubleRelationshipRecordUnits() {
        return doubleRelationshipRecordUnits;
    }

    public ImmutableSet<OpenOption> getOpenOptions() {
        return openOptions;
    }
}
