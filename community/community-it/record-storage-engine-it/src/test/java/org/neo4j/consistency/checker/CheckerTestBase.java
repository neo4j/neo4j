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
package org.neo4j.consistency.checker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.consistency.checker.ParallelExecution.NOOP_EXCEPTION_HANDLER;
import static org.neo4j.consistency.checker.RecordStorageConsistencyChecker.DEFAULT_SLOT_SIZES;
import static org.neo4j.consistency.checking.ByteArrayBitsManipulator.MAX_BYTES;
import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.GROUP_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.Values.intArray;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.checking.LookupAccessorsFromRunningDb;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheSlots;
import org.neo4j.consistency.checking.cache.DefaultCacheAccess;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.report.InconsistencyMessageLogger;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.consistency.statistics.Counts;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.RecordStorageIndexingBehaviour;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.recordstorage.SchemaStorage;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@EphemeralTestDirectoryExtension
class CheckerTestBase {
    static final int NUMBER_OF_THREADS = 4;
    static final long NULL = NULL_REFERENCE.longValue();
    static final int IDS_PER_CHUNK = 100;

    @Inject
    TestDirectory directory;

    MutableIntObjectMap<MutableIntSet> noMandatoryProperties = IntObjectMaps.mutable.empty();
    MutableIntObjectMap<MutableIntObjectMap<PropertyTypeSet>> noAllowedTypes = IntObjectMaps.mutable.empty();
    GraphDatabaseAPI db;
    NeoStores neoStores;
    NodeStore nodeStore;
    PropertyStore propertyStore;
    RelationshipGroupStore relationshipGroupStore;
    RelationshipStore relationshipStore;
    SchemaStore schemaStore;
    ConsistencyReporter reporter;
    ConsistencyReporter.Monitor monitor;
    SchemaStorage schemaStorage;

    IdGenerator relGroupIdGenerator;
    IdGenerator nodeStoreIdGenerator;
    IdGenerator relStoreIdGenerator;
    IdGenerator schemaIdGenerator;

    private DatabaseManagementService dbms;
    private CheckerContext context;
    private CountsState countsState;
    private CacheAccess cacheAccess;
    private TokenHolders tokenHolders;
    private PageCache pageCache;
    protected CachedStoreCursors storeCursors;
    protected DynamicAllocatorProvider allocatorProvider;

    @BeforeEach
    void setUpDb() throws Exception {
        dbms = new TestDatabaseManagementServiceBuilder(directory.homePath())
                .setConfig(db_format, RecordFormatSelector.defaultFormat().name())
                .setFileSystem(directory.getFileSystem())
                .build();
        db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);

        // Create our tokens
        Kernel kernel = db.getDependencyResolver().resolveDependency(Kernel.class);
        try (KernelTransaction tx =
                kernel.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
            initialData(tx);
            tx.commit();
        }

        DependencyResolver dependencies = db.getDependencyResolver();
        neoStores = dependencies.resolveDependency(RecordStorageEngine.class).testAccessNeoStores();
        allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);
        nodeStore = neoStores.getNodeStore();
        relationshipGroupStore = neoStores.getRelationshipGroupStore();
        propertyStore = neoStores.getPropertyStore();
        relationshipStore = neoStores.getRelationshipStore();
        schemaStore = neoStores.getSchemaStore();
        tokenHolders = dependencies.resolveDependency(TokenHolders.class);
        schemaStorage = new SchemaStorage(schemaStore, tokenHolders);
        cacheAccess = new DefaultCacheAccess(
                NumberArrayFactories.HEAP.newDynamicByteArray(10_000, new byte[MAX_BYTES], INSTANCE),
                Counts.NONE,
                NUMBER_OF_THREADS);
        cacheAccess.setCacheSlotSizes(DEFAULT_SLOT_SIZES);
        pageCache = dependencies.resolveDependency(PageCache.class);
        storeCursors = new CachedStoreCursors(neoStores, CursorContext.NULL_CONTEXT);
        relGroupIdGenerator = relationshipGroupStore.getIdGenerator();
        nodeStoreIdGenerator = nodeStore.getIdGenerator();
        relStoreIdGenerator = relationshipStore.getIdGenerator();
        schemaIdGenerator = schemaStore.getIdGenerator();
    }

    @AfterEach
    void tearDownDb() {
        closeAllUnchecked(storeCursors, countsState);
        dbms.shutdown();
    }

    IndexUpdater labelIndexWriter() {
        IndexingService indexingService = db.getDependencyResolver().resolveDependency(IndexingService.class);
        final IndexDescriptor[] indexDescriptors =
                schemaStorage.indexGetForSchema(() -> ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR, storeCursors);
        // The Node Label Index should exist and be unique.
        assertThat(indexDescriptors.length).isEqualTo(1);
        IndexDescriptor nli = indexDescriptors[0];
        IndexProxy indexProxy;
        try {
            indexProxy = indexingService.getIndexProxy(nli);
        } catch (IndexNotFoundKernelException e) {
            throw new RuntimeException(e);
        }
        return indexProxy.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false);
    }

    void initialData(KernelTransaction tx) throws KernelException {}

    CheckerContext context() throws Exception {
        return context(NUMBER_OF_THREADS, ConsistencyFlags.ALL);
    }

    CheckerContext context(ConsistencyFlags consistencyFlags) throws Exception {
        return context(NUMBER_OF_THREADS, consistencyFlags);
    }

    CheckerContext context(int numberOfThreads) throws Exception {
        return context(numberOfThreads, ConsistencyFlags.ALL);
    }

    CheckerContext context(int numberOfThreads, ConsistencyFlags consistencyFlags) throws Exception {
        ConsistencySummaryStatistics inconsistenciesSummary = new ConsistencySummaryStatistics();
        return context(numberOfThreads, consistencyFlags, inconsistenciesSummary);
    }

    CheckerContext context(
            int numberOfThreads, ConsistencyFlags consistencyFlags, ConsistencySummaryStatistics inconsistenciesSummary)
            throws Exception {
        if (context != null) {
            return context;
        }

        // We do this as late as possible because of how it eagerly caches which indexes exist so if the test creates an
        // index
        // this lazy instantiation allows the context to pick it up
        Config config = Config.defaults(neo4j_home, directory.homePath());
        DependencyResolver dependencies = db.getDependencyResolver();
        IndexProviderMap indexProviders = dependencies.resolveDependency(IndexProviderMap.class);
        IndexingService indexingService = dependencies.resolveDependency(IndexingService.class);
        CursorContextFactory contextFactory = new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);
        IndexAccessors indexAccessors = new IndexAccessors(
                indexProviders,
                cursorContext ->
                        asResourceIterator(allIndexes(neoStores, dependencies.resolveDependency(TokenHolders.class))
                                .iterator()),
                new IndexSamplingConfig(config),
                new LookupAccessorsFromRunningDb(indexingService),
                tokenHolders,
                contextFactory,
                neoStores.getOpenOptions(),
                new RecordStorageIndexingBehaviour(
                        neoStores.getNodeStore().getRecordsPerPage(),
                        neoStores.getRelationshipStore().getRecordsPerPage()));
        InconsistencyReport report =
                new InconsistencyReport(new InconsistencyMessageLogger(NullLog.getInstance()), inconsistenciesSummary);
        monitor = mock(ConsistencyReporter.Monitor.class);
        reporter = new ConsistencyReporter(report, monitor);
        countsState = new CountsState(neoStores, cacheAccess, INSTANCE);
        EntityBasedMemoryLimiter limiter = EntityBasedMemoryLimiter.defaultMemoryLimiter(mebiBytes(80))
                .create(
                        nodeStore.getIdGenerator().getHighId(),
                        relationshipStore.getIdGenerator().getHighId());
        ProgressMonitorFactory.MultiPartBuilder progress = ProgressMonitorFactory.NONE.multipleParts("Test");
        ParallelExecution execution = new ParallelExecution(numberOfThreads, NOOP_EXCEPTION_HANDLER, IDS_PER_CHUNK);
        context = new CheckerContext(
                neoStores,
                indexAccessors,
                execution,
                reporter,
                cacheAccess,
                tokenHolders,
                new RecordLoading(neoStores),
                countsState,
                limiter,
                progress,
                pageCache,
                INSTANCE,
                NullLog.getInstance(),
                false,
                consistencyFlags,
                contextFactory);
        context.initialize();
        return context;
    }

    private Iterable<IndexDescriptor> allIndexes(NeoStores neoStores, TokenHolders tokenHolders) {
        try (var storeCursors = new CachedStoreCursors(neoStores, CursorContext.NULL_CONTEXT)) {
            return Iterators.asCollection(SchemaRuleAccess.getSchemaRuleAccess(neoStores.getSchemaStore(), tokenHolders)
                    .indexesGetAllIgnoreMalformed(storeCursors));
        }
    }

    static Value stringValueOfLength(int length) {
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = (char) ('a' + (i % 10));
        }
        return stringValue(String.valueOf(chars));
    }

    static Value intArrayValueOfLength(int length) {
        int[] array = new int[length];
        for (int i = 0; i < length; i++) {
            array[i] = Integer.MAX_VALUE - i;
        }
        return intArray(array);
    }

    static Value stringArrayValueOfLength(int stringLength, int arrayLength) {
        String[] array = new String[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            char c = (char) ('a' + i % 20);
            array[i] = String.valueOf(c).repeat(stringLength);
        }
        return Values.stringArray(array);
    }

    /**
     * Magic for extracting a ConsistencyReport method name compile-time safe.
     */
    <T extends ConsistencyReport> void expect(Class<T> cls, Consumer<T> methodCall) {
        methodCall.accept(mock(cls, invocation -> {
            expect(cls, invocation.getMethod().getName());
            return null;
        }));
    }

    private void expect(Class<? extends ConsistencyReport> reportClass, String reportMethod) {
        verify(monitor, atLeastOnce()).reported(eq(reportClass), eq(reportMethod), anyString(), anyBoolean());
    }

    long index(SchemaDescriptor descriptor) throws KernelException {
        long indexId;
        try (KernelTransaction tx = ktx()) {
            IndexDescriptor index = tx.schemaWrite()
                    .indexCreate(IndexPrototype.forSchema(descriptor).withName("the index"));
            tx.commit();
            indexId = index.getId();
        }
        awaitIndexesOnline();
        return indexId;
    }

    IndexDescriptor uniqueIndex(SchemaDescriptor descriptor) throws KernelException {
        String constraintName = "me";
        try (KernelTransaction tx = ktx()) {
            tx.schemaWrite()
                    .uniquePropertyConstraintCreate(
                            IndexPrototype.uniqueForSchema(descriptor).withName(constraintName));
            tx.commit();
        }
        awaitIndexesOnline();
        try (KernelTransaction tx = ktx()) {
            var constraint =
                    tx.schemaRead().constraintGetForName(constraintName).asUniquenessConstraint();
            long indexId = constraint.ownedIndexId();
            Iterator<IndexDescriptor> iterator = tx.schemaRead().indexesGetAllNonLocking();
            while (iterator.hasNext()) {
                IndexDescriptor indexDescriptor = iterator.next();
                if (indexDescriptor.getId() == indexId) {
                    return indexDescriptor;
                }
            }
        }
        throw new RuntimeException("Index not found: " + descriptor);
    }

    private void awaitIndexesOnline() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.MINUTES);
            tx.commit();
        }
    }

    KernelTransaction ktx() throws TransactionFailureException {
        Kernel kernel = db.getDependencyResolver().resolveDependency(Kernel.class);
        return kernel.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED);
    }

    /**
     * Convenience for a transaction that auto-commits.
     */
    AutoCloseable tx() throws TransactionFailureException {
        Kernel kernel = db.getDependencyResolver().resolveDependency(Kernel.class);
        KernelTransaction tx = kernel.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED);
        return () -> {
            tx.commit();
            tx.close();
        };
    }

    PropertyBlock propertyValue(int propertyKey, Value value) {
        PropertyBlock propertyBlock = new PropertyBlock();
        neoStores
                .getPropertyStore()
                .encodeValue(
                        propertyBlock,
                        propertyKey,
                        value,
                        allocatorProvider.allocator(StoreType.PROPERTY_STRING),
                        allocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                        CursorContext.NULL_CONTEXT,
                        INSTANCE);
        return propertyBlock;
    }

    int[] nodeLabels(NodeRecord node) {
        return NodeLabelsField.get(node, neoStores.getNodeStore(), storeCursors);
    }

    NodeRecord loadNode(long id) {
        NodeStore nodeStore = neoStores.getNodeStore();
        var cursor = storeCursors.readCursor(NODE_CURSOR);
        return nodeStore.getRecordByCursor(id, nodeStore.newRecord(), RecordLoad.NORMAL, cursor);
    }

    long node(long id, long nextProp, long nextRel, int... labels) {
        NodeRecord node = new NodeRecord(id).initialize(true, nextProp, false, NULL, 0);
        InlineNodeLabels.putSorted(
                node,
                labels,
                nodeStore,
                null /*<-- intentionally prevent dynamic labels here*/,
                CursorContext.NULL_CONTEXT,
                storeCursors,
                INSTANCE);
        try (var storeCursor = storeCursors.writeCursor(NODE_CURSOR)) {
            nodeStore.updateRecord(node, storeCursor, CursorContext.NULL_CONTEXT, storeCursors);
        }
        return id;
    }

    long relationship(
            long id,
            long startNode,
            long endNode,
            int type,
            long startPrev,
            long startNext,
            long endPrev,
            long endNext,
            boolean firstInStart,
            boolean firstInEnd) {
        return relationship(
                id, startNode, endNode, type, NULL, startPrev, startNext, endPrev, endNext, firstInStart, firstInEnd);
    }

    long relationship(
            long id,
            long startNode,
            long endNode,
            int type,
            long nextProp,
            long startPrev,
            long startNext,
            long endPrev,
            long endNext,
            boolean firstInStart,
            boolean firstInEnd) {
        RelationshipRecord relationship = new RelationshipRecord(id)
                .initialize(
                        true,
                        nextProp,
                        startNode,
                        endNode,
                        type,
                        startPrev,
                        startNext,
                        endPrev,
                        endNext,
                        firstInStart,
                        firstInEnd);
        try (var storeCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            relationshipStore.updateRecord(relationship, storeCursor, CursorContext.NULL_CONTEXT, storeCursors);
        }
        return id;
    }

    void markAsDeletedId(CommonAbstractStore store, long id) {
        try (var marker = store.getIdGenerator().transactionalMarker(NULL_CONTEXT)) {
            marker.markDeleted(id);
        }
    }

    void markAsUsedId(CommonAbstractStore store, long id) {
        try (var marker = store.getIdGenerator().transactionalMarker(NULL_CONTEXT)) {
            marker.markUsed(id);
        }
    }

    long relationshipGroup(long id, long next, long owningNode, int type, long firstOut, long firstIn, long firstLoop) {
        RelationshipGroupRecord group =
                new RelationshipGroupRecord(id).initialize(true, type, firstOut, firstIn, firstLoop, owningNode, next);
        try (var storeCursor = storeCursors.writeCursor(GROUP_CURSOR)) {
            relationshipGroupStore.updateRecord(group, storeCursor, CursorContext.NULL_CONTEXT, storeCursors);
        }
        return id;
    }

    long nodePlusCached(long id, long nextProp, long nextRel, int... labels) {
        long node = node(id, nextProp, NULL, labels);
        CacheAccess.Client client = cacheAccess.client();
        client.putToCacheSingle(id, CacheSlots.NodeLink.SLOT_IN_USE, 1);
        client.putToCacheSingle(id, CacheSlots.NodeLink.SLOT_RELATIONSHIP_ID, nextRel);
        return node;
    }

    protected void property(long id, long prev, long next, PropertyBlock... properties) {
        PropertyRecord prop = new PropertyRecord(id).initialize(true, prev, next);
        for (PropertyBlock property : properties) {
            prop.addPropertyBlock(property);
        }
        try (var storeCursor = storeCursors.writeCursor(PROPERTY_CURSOR)) {
            propertyStore.updateRecord(prop, storeCursor, CursorContext.NULL_CONTEXT, storeCursors);
        }
    }
}
