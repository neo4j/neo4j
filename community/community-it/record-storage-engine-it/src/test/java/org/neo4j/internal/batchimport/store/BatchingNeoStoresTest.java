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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.batchimport.DefaultAdditionalIds.EMPTY;
import static org.neo4j.internal.batchimport.store.BatchingNeoStores.DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD;
import static org.neo4j.internal.batchimport.store.BatchingNeoStores.batchingNeoStores;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.Commitment.NO_COMMITMENT;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.counts.CountsUpdater;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.LockVerificationFactory;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.ExternallyManagedPageCache;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.database.MetadataCache;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.format.ForcedSecondaryUnitRecordFormats;
import org.neo4j.kernel.impl.store.format.PageCacheOptionsSelector;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.log.CompleteTransaction;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.lock.LockService;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandBatchToApply;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.Clocks;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.storable.Values;

@PageCacheExtension
@Neo4jLayoutExtension
class BatchingNeoStoresTest {
    private static final RelationshipType RELTYPE = RelationshipType.withName("TEST");
    private static final CursorContextFactory CONTEXT_FACTORY = NULL_CONTEXT_FACTORY;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    @Test
    void shouldNotOpenStoreWithNodesOrRelationshipsInIt() throws Throwable {
        // GIVEN
        someDataInTheDatabase();

        // WHEN
        DirectoryNotEmptyException exception = assertThrows(DirectoryNotEmptyException.class, () -> {
            try (JobScheduler jobScheduler = new ThreadPoolJobScheduler()) {
                try (BatchingNeoStores store = batchingNeoStores(
                        fileSystem,
                        databaseLayout,
                        Configuration.DEFAULT,
                        NullLogService.getInstance(),
                        EMPTY,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                        Config.defaults(),
                        jobScheduler,
                        PageCacheTracer.NULL,
                        CONTEXT_FACTORY,
                        INSTANCE)) {
                    store.createNew();
                }
            }
        });

        // THEN
        assertThat(exception.getMessage())
                .contains(databaseLayout.databaseDirectory().toString())
                .contains("already contains");
    }

    @Test
    void shouldNotOpenStoreWithTransactionLogContentsInIt() throws Throwable {
        // GIVEN
        someDataInTheDatabase();
        fileSystem.deleteRecursively(databaseLayout.databaseDirectory());

        // WHEN
        DirectoryNotEmptyException exception = assertThrows(DirectoryNotEmptyException.class, () -> {
            try (JobScheduler jobScheduler = new ThreadPoolJobScheduler()) {
                try (BatchingNeoStores store = batchingNeoStores(
                        fileSystem,
                        databaseLayout,
                        Configuration.DEFAULT,
                        NullLogService.getInstance(),
                        EMPTY,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                        Config.defaults(),
                        jobScheduler,
                        PageCacheTracer.NULL,
                        CONTEXT_FACTORY,
                        INSTANCE)) {
                    store.createNew();
                }
            }
        });

        // THEN
        assertThat(exception.getMessage())
                .contains(databaseLayout.getTransactionLogsDirectory().toString())
                .contains("already contains");
    }

    @Test
    void shouldRespectDbConfig() throws Exception {
        // GIVEN
        int size = 10;
        Config config = Config.newBuilder()
                .set(GraphDatabaseInternalSettings.array_block_size, size)
                .set(GraphDatabaseInternalSettings.string_block_size, size)
                .build();

        // WHEN
        int headerSize = defaultFormat().dynamic().getRecordHeaderSize();
        try (JobScheduler jobScheduler = new ThreadPoolJobScheduler();
                BatchingNeoStores store = batchingNeoStores(
                        fileSystem,
                        databaseLayout,
                        Configuration.DEFAULT,
                        NullLogService.getInstance(),
                        EMPTY,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                        config,
                        jobScheduler,
                        PageCacheTracer.NULL,
                        CONTEXT_FACTORY,
                        INSTANCE)) {
            store.createNew();

            // THEN
            assertEquals(
                    size + headerSize, store.getPropertyStore().getArrayStore().getRecordSize());
            assertEquals(
                    size + headerSize, store.getPropertyStore().getStringStore().getRecordSize());
        }
    }

    @Test
    void shouldPruneAndOpenExistingDatabase() throws Exception {
        // given
        for (StoreType typeToTest : relevantRecordStores()) {
            // given all the stores with some records in them
            testDirectory.cleanup();
            try (BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache(
                    fileSystem,
                    pageCache,
                    PageCacheTracer.NULL,
                    CONTEXT_FACTORY,
                    databaseLayout,
                    Configuration.DEFAULT,
                    NullLogService.getInstance(),
                    EMPTY,
                    LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                    Config.defaults(),
                    INSTANCE)) {
                stores.createNew();
                NeoStores neoStores = stores.getNeoStores();
                var allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);
                for (StoreType type : relevantRecordStores()) {
                    createRecordIn(neoStores.getRecordStore(type), allocatorProvider);
                }
            }

            // when opening and pruning all except the one we test
            try (BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache(
                    fileSystem,
                    pageCache,
                    PageCacheTracer.NULL,
                    CONTEXT_FACTORY,
                    databaseLayout,
                    Configuration.DEFAULT,
                    NullLogService.getInstance(),
                    EMPTY,
                    LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                    Config.defaults(),
                    INSTANCE)) {
                stores.pruneAndOpenExistingStore(type -> type == typeToTest, Predicates.alwaysFalse());

                // then only the one we kept should have data in it
                for (StoreType type : relevantRecordStores()) {
                    RecordStore<AbstractBaseRecord> store =
                            stores.getNeoStores().getRecordStore(type);
                    long highId = store.getIdGenerator().getHighId();
                    if (type == typeToTest) {
                        assertThat(highId).as(store.toString()).isGreaterThan(store.getNumberOfReservedLowIds());
                    } else {
                        assertEquals(store.getNumberOfReservedLowIds(), highId, store.toString());
                    }
                }
            }
        }
    }

    @Test
    void shouldDecideToAllocateDoubleRelationshipRecordUnitsOnLargeAmountOfRelationshipsOnSupportedFormat()
            throws Exception {
        // given
        Config config = configForForcedSecondaryUnitRecordFormats();
        try (BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache(
                fileSystem,
                pageCache,
                PageCacheTracer.NULL,
                CONTEXT_FACTORY,
                databaseLayout,
                Configuration.DEFAULT,
                NullLogService.getInstance(),
                EMPTY,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                config,
                INSTANCE)) {
            stores.createNew();
            Input.Estimates estimates =
                    Input.knownEstimates(0, DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD << 1, 0, 0, 0, 0, 0);

            // when
            boolean doubleUnits = stores.determineDoubleRelationshipRecordUnits(estimates);

            // then
            assertTrue(doubleUnits);
        }
    }

    @Test
    void shouldNotDecideToAllocateDoubleRelationshipRecordUnitsonLowAmountOfRelationshipsOnSupportedFormat()
            throws Exception {
        // given
        Config config = configForForcedSecondaryUnitRecordFormats();
        try (BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache(
                fileSystem,
                pageCache,
                PageCacheTracer.NULL,
                CONTEXT_FACTORY,
                databaseLayout,
                Configuration.DEFAULT,
                NullLogService.getInstance(),
                EMPTY,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                config,
                INSTANCE)) {
            stores.createNew();
            Input.Estimates estimates =
                    Input.knownEstimates(0, DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD >> 1, 0, 0, 0, 0, 0);

            // when
            boolean doubleUnits = stores.determineDoubleRelationshipRecordUnits(estimates);

            // then
            assertFalse(doubleUnits);
        }
    }

    @Test
    void shouldNotDecideToAllocateDoubleRelationshipRecordUnitsOnLargeAmountOfRelationshipsOnUnsupportedFormat()
            throws Exception {
        // given
        try (BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache(
                fileSystem,
                pageCache,
                PageCacheTracer.NULL,
                CONTEXT_FACTORY,
                databaseLayout,
                Configuration.DEFAULT,
                NullLogService.getInstance(),
                EMPTY,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                Config.defaults(),
                INSTANCE)) {
            stores.createNew();
            Input.Estimates estimates =
                    Input.knownEstimates(0, DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD << 1, 0, 0, 0, 0, 0);

            // when
            boolean doubleUnits = stores.determineDoubleRelationshipRecordUnits(estimates);

            // then
            assertFalse(doubleUnits);
        }
    }

    @Test
    void shouldRebuildCountsStoreEvenIfExistsInEmptyDb() throws IOException {
        // given
        var openOptions = PageCacheOptionsSelector.select(RecordFormatSelector.selectForStoreOrConfigForNewDbs(
                Config.defaults(),
                databaseLayout,
                fileSystem,
                pageCache,
                NullLogService.getInstance().getInternalLogProvider(),
                CONTEXT_FACTORY));
        try (GBPTreeCountsStore countsStore = new GBPTreeCountsStore(
                pageCache,
                databaseLayout.countStore(),
                fileSystem,
                RecoveryCleanupWorkCollector.immediate(),
                CountsBuilder.EMPTY,
                false,
                GBPTreeCountsStore.NO_MONITOR,
                DEFAULT_DATABASE_NAME,
                1_000,
                NullLogProvider.getInstance(),
                CONTEXT_FACTORY,
                PageCacheTracer.NULL,
                openOptions)) {
            countsStore.start(NULL_CONTEXT, INSTANCE);
            countsStore.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        }

        // when
        try (BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache(
                fileSystem,
                pageCache,
                PageCacheTracer.NULL,
                CONTEXT_FACTORY,
                databaseLayout,
                Configuration.DEFAULT,
                NullLogService.getInstance(),
                EMPTY,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                Config.defaults(),
                INSTANCE)) {
            stores.createNew();
            stores.buildCountsStore(
                    new CountsBuilder() {

                        @Override
                        public void initialize(
                                CountsUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
                            updater.incrementNodeCount(1, 10);
                            updater.incrementNodeCount(2, 20);
                            updater.incrementRelationshipCount(ANY_LABEL, 1, 2, 30);
                            updater.incrementRelationshipCount(1, 2, ANY_LABEL, 50);
                        }

                        @Override
                        public long lastCommittedTxId() {
                            return BASE_TX_ID + 1;
                        }
                    },
                    CONTEXT_FACTORY,
                    INSTANCE);
        }

        // then
        try (var countsStore = new GBPTreeCountsStore(
                pageCache,
                databaseLayout.countStore(),
                fileSystem,
                RecoveryCleanupWorkCollector.immediate(),
                CountsBuilder.EMPTY,
                false,
                GBPTreeCountsStore.NO_MONITOR,
                DEFAULT_DATABASE_NAME,
                1_000,
                NullLogProvider.getInstance(),
                CONTEXT_FACTORY,
                PageCacheTracer.NULL,
                openOptions)) {
            assertEquals(10, countsStore.nodeCount(1, NULL_CONTEXT));
            assertEquals(20, countsStore.nodeCount(2, NULL_CONTEXT));
            assertEquals(30, countsStore.relationshipCount(ANY_LABEL, 1, 2, NULL_CONTEXT));
            assertEquals(50, countsStore.relationshipCount(1, 2, ANY_LABEL, NULL_CONTEXT));
        }
    }

    @Test
    void shouldOverrideBigPageCacheMemorySettingContainingUnit() throws IOException {
        // GIVEN
        Config dbConfig = Config.defaults(pagecache_memory, ByteUnit.gibiBytes(2));

        // WHEN
        try (var stores = batchingNeoStores(
                fileSystem,
                databaseLayout,
                Configuration.DEFAULT,
                NullLogService.getInstance(),
                EMPTY,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                dbConfig,
                Mockito.mock(JobScheduler.class),
                PageCacheTracer.NULL,
                NULL_CONTEXT_FACTORY,
                INSTANCE)) {
            // THEN
            assertThat(stores.getPageCache().maxCachedPages()
                            * stores.getPageCache().pageSize())
                    .isCloseTo(BatchingNeoStores.MAX_PAGE_CACHE_MEMORY, Percentage.withPercentage(1));
        }
    }

    @Test
    void shouldOverrideSmallPageCacheMemorySettingContainingUnit() throws IOException {
        // GIVEN
        long overridden = ByteUnit.mebiBytes(10);
        Config dbConfig = Config.defaults(pagecache_memory, overridden);

        // WHEN
        try (var stores = batchingNeoStores(
                fileSystem,
                databaseLayout,
                Configuration.DEFAULT,
                NullLogService.getInstance(),
                EMPTY,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                dbConfig,
                Mockito.mock(JobScheduler.class),
                PageCacheTracer.NULL,
                NULL_CONTEXT_FACTORY,
                INSTANCE)) {
            // THEN
            assertThat(stores.getPageCache().maxCachedPages()
                            * stores.getPageCache().pageSize())
                    .isCloseTo(overridden, Percentage.withPercentage(1));
        }
    }

    @Test
    void shouldUseProvidedPageCache() throws IOException {
        // GIVEN
        var config = new Configuration.Overridden(Configuration.DEFAULT) {
            @Override
            public ExternallyManagedPageCache providedPageCache() {
                return new ExternallyManagedPageCache(pageCache);
            }
        };

        // WHEN
        try (var stores = batchingNeoStores(
                fileSystem,
                databaseLayout,
                config,
                NullLogService.getInstance(),
                EMPTY,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                Config.defaults(),
                Mockito.mock(JobScheduler.class),
                PageCacheTracer.NULL,
                NULL_CONTEXT_FACTORY,
                INSTANCE)) {
            stores.createNew();

            // THEN
            assertThat(pageCache.listExistingMappings().size()).isGreaterThan(0);
        }
    }

    private static StoreType[] relevantRecordStores() {
        return Stream.of(StoreType.STORE_TYPES)
                .filter(type -> type != StoreType.META_DATA)
                .toArray(StoreType[]::new);
    }

    private static <RECORD extends AbstractBaseRecord> void createRecordIn(
            RecordStore<RECORD> store, DynamicAllocatorProvider allocatorProvider) {
        RECORD record = store.newRecord();
        record.setId(store.getIdGenerator().nextId(NULL_CONTEXT));
        record.setInUse(true);
        if (record instanceof PropertyRecord) {
            // Special hack for property store, since it's not enough to simply set a record as in use there
            PropertyBlock block = new PropertyBlock();
            PropertyStore.encodeValue(
                    block,
                    0,
                    Values.of(10),
                    allocatorProvider.allocator(StoreType.PROPERTY_STRING),
                    allocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                    NULL_CONTEXT,
                    INSTANCE);
            ((PropertyRecord) record).addPropertyBlock(block);
        }
        try (var storeCursor = store.openPageCursorForWriting(0, NULL_CONTEXT)) {
            store.updateRecord(record, storeCursor, NULL_CONTEXT, StoreCursors.NULL);
        }
    }

    private void someDataInTheDatabase() throws Exception {
        NullLog nullLog = NullLog.getInstance();
        try (JobScheduler scheduler = JobSchedulerFactory.createInitialisedScheduler();
                PageCache pageCache = new ConfiguringPageCacheFactory(
                                fileSystem,
                                Config.defaults(),
                                PageCacheTracer.NULL,
                                nullLog,
                                scheduler,
                                Clocks.nanoClock(),
                                new MemoryPools())
                        .getOrCreatePageCache();
                Lifespan life = new Lifespan()) {
            // TODO this little dance with TokenHolders is really annoying and must be solved with a better abstraction
            DeferredInitializedTokenCreator propertyKeyTokenCreator = new DeferredInitializedTokenCreator() {
                @Override
                void create(String name, boolean internal, int id) {
                    txState.propertyKeyDoCreateForName(name, internal, id);
                }
            };
            DeferredInitializedTokenCreator labelTokenCreator = new DeferredInitializedTokenCreator() {
                @Override
                void create(String name, boolean internal, int id) {
                    txState.labelDoCreateForName(name, internal, id);
                }
            };
            DeferredInitializedTokenCreator relationshipTypeTokenCreator = new DeferredInitializedTokenCreator() {
                @Override
                void create(String name, boolean internal, int id) {
                    txState.relationshipTypeDoCreateForName(name, internal, id);
                }
            };
            TokenHolders tokenHolders = new TokenHolders(
                    new CreatingTokenHolder(propertyKeyTokenCreator, TokenHolder.TYPE_PROPERTY_KEY),
                    new CreatingTokenHolder(labelTokenCreator, TokenHolder.TYPE_LABEL),
                    new CreatingTokenHolder(relationshipTypeTokenCreator, TokenHolder.TYPE_RELATIONSHIP_TYPE));
            IndexConfigCompleter indexConfigCompleter = (index, indexingBehaviour) -> index;
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector = immediate();
            LogTailMetadata emptyLogTail = new EmptyLogTailMetadata(Config.defaults());
            MetadataCache versionRepository = new MetadataCache(emptyLogTail);
            RecordStorageEngine storageEngine = life.add(new RecordStorageEngine(
                    databaseLayout,
                    Config.defaults(),
                    pageCache,
                    fileSystem,
                    NullLogProvider.getInstance(),
                    NullLogProvider.getInstance(),
                    tokenHolders,
                    new DatabaseSchemaState(NullLogProvider.getInstance()),
                    new StandardConstraintSemantics(),
                    indexConfigCompleter,
                    LockService.NO_LOCK_SERVICE,
                    new DatabaseHealth(HealthEventGenerator.NO_OP, nullLog),
                    new DefaultIdGeneratorFactory(fileSystem, immediate(), PageCacheTracer.NULL, DEFAULT_DATABASE_NAME),
                    recoveryCleanupWorkCollector,
                    INSTANCE,
                    emptyLogTail,
                    versionRepository,
                    LockVerificationFactory.NONE,
                    CONTEXT_FACTORY,
                    PageCacheTracer.NULL,
                    VersionStorage.EMPTY_STORAGE));
            // Create the relationship type token
            TxState txState = new TxState();
            var transactionIdGenerator = new IdStoreTransactionIdGenerator(storageEngine.metadataProvider());
            NeoStores neoStores = storageEngine.testAccessNeoStores();
            try (CommandCreationContext commandCreationContext = storageEngine.newCommandCreationContext(false);
                    var storeCursors = storageEngine.createStorageCursors(NULL_CONTEXT)) {
                commandCreationContext.initialize(
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        NULL_CONTEXT,
                        storeCursors,
                        CommandCreationContext.NO_STARTTIME_OF_OLDEST_TRANSACTION,
                        ResourceLocker.IGNORE,
                        () -> LockTracer.NONE);
                propertyKeyTokenCreator.initialize(neoStores.getPropertyKeyTokenStore(), txState);
                labelTokenCreator.initialize(neoStores.getLabelTokenStore(), txState);
                relationshipTypeTokenCreator.initialize(neoStores.getRelationshipTypeTokenStore(), txState);
                int relTypeId = tokenHolders.relationshipTypeTokens().getOrCreateId(RELTYPE.name());
                apply(txState, commandCreationContext, storageEngine, storeCursors, transactionIdGenerator);

                // Finally, we're initialized and ready to create two nodes and a relationship
                txState = new TxState();
                long node1 = commandCreationContext.reserveNode();
                long node2 = commandCreationContext.reserveNode();
                txState.nodeDoCreate(node1);
                txState.nodeDoCreate(node2);
                txState.relationshipDoCreate(
                        commandCreationContext.reserveRelationship(node1, node2, relTypeId, true, true),
                        relTypeId,
                        node1,
                        node2);
                apply(txState, commandCreationContext, storageEngine, storeCursors, transactionIdGenerator);
                neoStores.flush(DatabaseFlushEvent.NULL, NULL_CONTEXT);
            }

            TransactionLogInitializer.getLogFilesInitializer()
                    .initializeLogFiles(
                            databaseLayout, neoStores.getMetaDataStore(), versionRepository, fileSystem, "testing");
        }
    }

    private static void apply(
            TxState txState,
            CommandCreationContext commandCreationContext,
            RecordStorageEngine storageEngine,
            StoreCursors storeCursors,
            TransactionIdGenerator transactionIdGenerator)
            throws Exception {
        try (RecordStorageReader storageReader = storageEngine.newReader()) {
            List<StorageCommand> commands = storageEngine.createCommands(
                    txState,
                    storageReader,
                    commandCreationContext,
                    LockTracer.NONE,
                    v -> v,
                    NULL_CONTEXT,
                    storeCursors,
                    INSTANCE);
            CommandBatchToApply apply = new TransactionToApply(
                    new CompleteTransaction(
                            commands,
                            UNKNOWN_CONSENSUS_INDEX,
                            0,
                            0,
                            0,
                            0,
                            LatestVersions.LATEST_KERNEL_VERSION,
                            ANONYMOUS),
                    NULL_CONTEXT,
                    storeCursors,
                    NO_COMMITMENT,
                    transactionIdGenerator);
            storageEngine.apply(apply, TransactionApplicationMode.INTERNAL);
        }
    }

    private static Config configForForcedSecondaryUnitRecordFormats() {
        return Config.newBuilder()
                .set(GraphDatabaseSettings.db_format, ForcedSecondaryUnitRecordFormats.DEFAULT_RECORD_FORMATS.name())
                .set(GraphDatabaseInternalSettings.include_versions_under_development, false)
                .build();
    }

    private abstract static class DeferredInitializedTokenCreator implements TokenCreator {
        TokenStore<?> store;
        TransactionState txState;

        void initialize(TokenStore<?> store, TransactionState txState) {
            this.store = store;
            this.txState = txState;
        }

        @Override
        public int createToken(String name, boolean internal) {
            int id = (int) store.getIdGenerator().nextId(NULL_CONTEXT);
            create(name, internal, id);
            return id;
        }

        abstract void create(String name, boolean internal, int id);
    }
}
