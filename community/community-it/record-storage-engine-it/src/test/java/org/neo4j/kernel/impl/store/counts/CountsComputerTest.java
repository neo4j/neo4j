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
package org.neo4j.kernel.impl.store.counts;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.IntStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.counts.CountsUpdater;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.helpers.progress.Indicator;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;

@ExtendWith(SoftAssertionsExtension.class)
@PageCacheExtension
@Neo4jLayoutExtension
class CountsComputerTest {
    private static final NullLogProvider LOG_PROVIDER = NullLogProvider.getInstance();
    private static final Config CONFIG = Config.defaults();
    private static final PageCacheTracer PAGE_CACHE_TRACER = PageCacheTracer.NULL;
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PAGE_CACHE_TRACER, EMPTY_CONTEXT_SUPPLIER);

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    private TestDatabaseManagementServiceBuilder dbBuilder;

    @InjectSoftAssertions
    private SoftAssertions softly;

    @BeforeEach
    void setup() {
        dbBuilder = new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setConfig(GraphDatabaseSettings.db_format, FormatFamily.ALIGNED.name())
                .setFileSystem(new UncloseableDelegatingFileSystemAbstraction(fileSystem));
    }

    @Test
    void tracePageCacheAccessOnInitialization() throws IOException {
        DatabaseManagementService managementService = dbBuilder.build();
        try {
            GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            try (var tx = db.beginTx()) {
                // Just make it non-empty
                tx.createNode();
                tx.commit();
            }

            var countsStore = db.getDependencyResolver().resolveDependency(GBPTreeCountsStore.class);
            var pageCacheTracer = new DefaultPageCacheTracer();
            var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
            var cursorContext = contextFactory.create("tracePageCacheAccessOnInitialization");

            countsStore.start(cursorContext, INSTANCE);

            PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
            softly.assertThat(cursorTracer.pins()).as("Pins").isEqualTo(4);
            softly.assertThat(cursorTracer.unpins()).as("Unpins").isEqualTo(4);
            softly.assertThat(cursorTracer.hits()).as("hits").isEqualTo(3);
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void skipPopulationWhenNodeAndRelationshipStoresAreEmpty() throws IOException {
        DatabaseManagementService managementService = dbBuilder.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        long lastCommittedTransactionId = getLastTxId(db);
        managementService.shutdown();

        InvocationTrackingProgressMonitorFactory factory = new InvocationTrackingProgressMonitorFactory();
        rebuildCounts(lastCommittedTransactionId, factory);

        try (var store = createCountsStore(matchingBuilder(lastCommittedTransactionId), getDBOpenOptions(db))) {
            store.start(NULL_CONTEXT, INSTANCE);
            softly.assertThat(store.txId()).as("Store Transaction id").isEqualTo(lastCommittedTransactionId);
            store.accept(new AssertEmptyCountStoreVisitor(), NULL_CONTEXT);
        }

        softly.assertThat(factory.isStartInvoked()).as("Start").isFalse();
        softly.assertThat(factory.isAddInvoked()).as("Add").isFalse();
    }

    @Test
    void shouldCreateAnEmptyCountsStoreFromAnEmptyDatabase() throws IOException {
        DatabaseManagementService managementService = dbBuilder.build();
        final GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        long lastCommittedTransactionId = getLastTxId(db);
        managementService.shutdown();

        rebuildCounts(lastCommittedTransactionId);

        try (var store = createCountsStore(matchingBuilder(lastCommittedTransactionId), getDBOpenOptions(db))) {
            store.start(NULL_CONTEXT, INSTANCE);
            softly.assertThat(store.txId()).as("Store Transaction id").isEqualTo(lastCommittedTransactionId);
            store.accept(new AssertEmptyCountStoreVisitor(), NULL_CONTEXT);
        }
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreNodesInTheDB() throws IOException, KernelException {
        DatabaseManagementService managementService = dbBuilder.build();
        final GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);

        Label[] labels;
        int[] labelIds;
        Node[] nodes;

        try (Transaction tx = db.beginTx()) {
            labels = createLabels(4);
            labelIds = getLabelIdsFrom(tx, labels);

            nodes = new Node[] {
                tx.createNode(labels[0]), tx.createNode(labels[1]), tx.createNode(labels[2]), tx.createNode()
            };
            tx.commit();
        }
        long lastCommittedTransactionId = getLastTxId(db);
        managementService.shutdown();

        rebuildCounts(lastCommittedTransactionId);

        try (var store = createCountsStore(getDBOpenOptions(db))) {
            softly.assertThat(store.txId()).as("Store Transaction id").isEqualTo(lastCommittedTransactionId);

            softly.assertThat(store.nodeCount(ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()")
                    .isEqualTo(nodes.length);
            softly.assertThat(store.nodeCount(labelIds[0], NULL_CONTEXT))
                    .as("count: (:%s)", labels[0])
                    .isEqualTo(1);
            softly.assertThat(store.nodeCount(labelIds[1], NULL_CONTEXT))
                    .as("count: (:%s)", labels[1])
                    .isEqualTo(1);
            softly.assertThat(store.nodeCount(labelIds[2], NULL_CONTEXT))
                    .as("count: (:%s)", labels[2])
                    .isEqualTo(1);
            softly.assertThat(store.nodeCount(labelIds[3], NULL_CONTEXT))
                    .as("count: (:%s)", labels[3])
                    .isEqualTo(0);
        }
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreUnusedNodeRecordsInTheDB() throws IOException, KernelException {
        DatabaseManagementService managementService = dbBuilder.build();
        final GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);

        Label[] labels;
        int[] labelIds;
        Node[] nodes;

        try (Transaction tx = db.beginTx()) {
            labels = createLabels(4);
            labelIds = getLabelIdsFrom(tx, labels);

            nodes = new Node[] {
                tx.createNode(labels[0]), tx.createNode(labels[1]), tx.createNode(labels[2]), tx.createNode()
            };
            nodes[2].delete();
            tx.commit();
        }
        long lastCommittedTransactionId = getLastTxId(db);
        managementService.shutdown();

        rebuildCounts(lastCommittedTransactionId);

        try (var store = createCountsStore(getDBOpenOptions(db))) {
            softly.assertThat(store.txId()).as("Store Transaction id").isEqualTo(lastCommittedTransactionId);

            softly.assertThat(store.nodeCount(ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()")
                    .isEqualTo(nodes.length - 1);
            softly.assertThat(store.nodeCount(labelIds[0], NULL_CONTEXT))
                    .as("count: (:%s)", labels[0])
                    .isEqualTo(1);
            softly.assertThat(store.nodeCount(labelIds[1], NULL_CONTEXT))
                    .as("count: (:%s)", labels[1])
                    .isEqualTo(1);
            softly.assertThat(store.nodeCount(labelIds[2], NULL_CONTEXT))
                    .as("count: (:%s)", labels[2])
                    .isEqualTo(0);
            softly.assertThat(store.nodeCount(labelIds[3], NULL_CONTEXT))
                    .as("count: (:%s)", labels[3])
                    .isEqualTo(0);
        }
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreUnusedRelationshipRecordsInTheDB() throws IOException, KernelException {
        DatabaseManagementService managementService = dbBuilder.build();
        final GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);

        Label[] labels;
        int[] labelIds;
        RelationshipType[] relTypes;
        int[] relTypeIds;
        Node[] nodes;
        Relationship[] rels;

        try (Transaction tx = db.beginTx()) {
            labels = createLabels(4);
            labelIds = getLabelIdsFrom(tx, labels);
            relTypes = createRelationShipTypes(2);
            relTypeIds = getRelTypeIdsFrom(tx, relTypes);

            nodes = new Node[] {tx.createNode(labels[0]), tx.createNode(labels[1])};

            rels = new Relationship[] {
                nodes[0].createRelationshipTo(nodes[1], relTypes[0]),
                nodes[1].createRelationshipTo(nodes[0], relTypes[1])
            };
            rels[0].delete();
            tx.commit();
        }
        long lastCommittedTransactionId = getLastTxId(db);
        managementService.shutdown();

        rebuildCounts(lastCommittedTransactionId);

        try (var store = createCountsStore(getDBOpenOptions(db))) {
            softly.assertThat(store.txId()).as("Store Transaction id").isEqualTo(lastCommittedTransactionId);

            softly.assertThat(store.nodeCount(ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()")
                    .isEqualTo(nodes.length);
            softly.assertThat(store.nodeCount(labelIds[0], NULL_CONTEXT))
                    .as("count: (:%s)", labels[0])
                    .isEqualTo(1);
            softly.assertThat(store.nodeCount(labelIds[1], NULL_CONTEXT))
                    .as("count: (:%s)", labels[1])
                    .isEqualTo(1);
            softly.assertThat(store.nodeCount(labelIds[2], NULL_CONTEXT))
                    .as("count: (:%s)", labels[2])
                    .isEqualTo(0);
            softly.assertThat(store.nodeCount(labelIds[3], NULL_CONTEXT))
                    .as("count: (:%s)", labels[3])
                    .isEqualTo(0);

            softly.assertThat(store.relationshipCount(ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, NULL_CONTEXT))
                    .as("()-[]->()")
                    .isEqualTo(rels.length - 1);
            softly.assertThat(store.relationshipCount(ANY_LABEL, relTypeIds[0], ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()-[:%s]->()", relTypes[0])
                    .isEqualTo(0);
            softly.assertThat(store.relationshipCount(ANY_LABEL, relTypeIds[1], ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()-[:%s]->()", relTypes[1])
                    .isEqualTo(1);
        }
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreNodesAndRelationshipsInTheDB() throws IOException, KernelException {
        DatabaseManagementService managementService = dbBuilder.build();
        final GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);

        Label[] labels;
        int[] labelIds;
        RelationshipType[] relTypes;
        int[] relTypeIds;
        Node[] nodes;
        Relationship[] rels;

        try (Transaction tx = db.beginTx()) {
            labels = createLabels(4);
            labelIds = getLabelIdsFrom(tx, labels);
            relTypes = createRelationShipTypes(3);
            relTypeIds = getRelTypeIdsFrom(tx, relTypes);

            nodes = new Node[] {
                tx.createNode(labels[0]), tx.createNode(labels[1]), tx.createNode(labels[2]), tx.createNode()
            };

            rels = new Relationship[] {
                nodes[0].createRelationshipTo(nodes[2], relTypes[0]),
                nodes[3].createRelationshipTo(nodes[1], relTypes[1])
            };
            tx.commit();
        }
        long lastCommittedTransactionId = getLastTxId(db);
        managementService.shutdown();

        rebuildCounts(lastCommittedTransactionId);

        try (var store = createCountsStore(getDBOpenOptions(db))) {
            softly.assertThat(store.txId()).as("Store Transaction id").isEqualTo(lastCommittedTransactionId);

            softly.assertThat(store.nodeCount(ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()")
                    .isEqualTo(nodes.length);
            softly.assertThat(store.nodeCount(labelIds[0], NULL_CONTEXT))
                    .as("count: (:%s)", labels[0])
                    .isEqualTo(1);
            softly.assertThat(store.nodeCount(labelIds[1], NULL_CONTEXT))
                    .as("count: (:%s)", labels[1])
                    .isEqualTo(1);
            softly.assertThat(store.nodeCount(labelIds[2], NULL_CONTEXT))
                    .as("count: (:%s)", labels[2])
                    .isEqualTo(1);
            softly.assertThat(store.nodeCount(labelIds[3], NULL_CONTEXT))
                    .as("count: (:%s)", labels[3])
                    .isEqualTo(0);

            softly.assertThat(store.relationshipCount(ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, NULL_CONTEXT))
                    .as("()-[]->()")
                    .isEqualTo(rels.length);
            softly.assertThat(store.relationshipCount(ANY_LABEL, relTypeIds[0], ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()-[:%s]->()", relTypes[0])
                    .isEqualTo(1);
            softly.assertThat(store.relationshipCount(ANY_LABEL, relTypeIds[1], ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()-[:%s]->()", relTypes[1])
                    .isEqualTo(1);
            softly.assertThat(store.relationshipCount(ANY_LABEL, relTypeIds[2], ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()-[:%s]->()", relTypes[2])
                    .isEqualTo(0);
            softly.assertThat(store.relationshipCount(ANY_LABEL, relTypeIds[1], labelIds[1], NULL_CONTEXT))
                    .as("count: ()-[:%s]->(:%s)", relTypes[1], labels[1])
                    .isEqualTo(1);
            softly.assertThat(store.relationshipCount(ANY_LABEL, relTypeIds[0], labelIds[1], NULL_CONTEXT))
                    .as("count: ()-[:%s]->(:%s)", relTypes[0], labels[1])
                    .isEqualTo(0);
        }
    }

    @Test
    void shouldCreateACountStoreWhenDBContainsDenseNodes() throws IOException, KernelException {
        DatabaseManagementService managementService = dbBuilder
                .setConfig(GraphDatabaseSettings.dense_node_threshold, 2)
                .build();
        final GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);

        Label[] labels;
        int[] labelIds;
        RelationshipType[] relTypes;
        int[] relTypeIds;
        Node[] nodes;
        Relationship[] rels;

        try (Transaction tx = db.beginTx()) {
            labels = createLabels(4);
            labelIds = getLabelIdsFrom(tx, labels);
            relTypes = createRelationShipTypes(5);
            relTypeIds = getRelTypeIdsFrom(tx, relTypes);

            nodes = new Node[] {tx.createNode(labels[0]), tx.createNode(labels[1]), tx.createNode(labels[2])};

            rels = new Relationship[] {
                nodes[0].createRelationshipTo(nodes[0], relTypes[0]),
                nodes[0].createRelationshipTo(nodes[1], relTypes[1]),
                nodes[0].createRelationshipTo(nodes[2], relTypes[2]),
                nodes[2].createRelationshipTo(nodes[1], relTypes[3])
            };
            tx.commit();
        }
        long lastCommittedTransactionId = getLastTxId(db);
        managementService.shutdown();

        rebuildCounts(lastCommittedTransactionId);

        try (var store = createCountsStore(getDBOpenOptions(db))) {
            softly.assertThat(store.txId()).as("Store Transaction id").isEqualTo(lastCommittedTransactionId);

            softly.assertThat(store.nodeCount(ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()")
                    .isEqualTo(nodes.length);
            softly.assertThat(store.nodeCount(labelIds[0], NULL_CONTEXT))
                    .as("count: (:%s)", labels[0])
                    .isEqualTo(1);
            softly.assertThat(store.nodeCount(labelIds[1], NULL_CONTEXT))
                    .as("count: (:%s)", labels[1])
                    .isEqualTo(1);
            softly.assertThat(store.nodeCount(labelIds[2], NULL_CONTEXT))
                    .as("count: (:%s)", labels[2])
                    .isEqualTo(1);
            softly.assertThat(store.nodeCount(labelIds[3], NULL_CONTEXT))
                    .as("count: (:%s)", labels[3])
                    .isEqualTo(0);

            softly.assertThat(store.relationshipCount(ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, NULL_CONTEXT))
                    .as("()-[]->()")
                    .isEqualTo(rels.length);
            softly.assertThat(store.relationshipCount(ANY_LABEL, relTypeIds[0], ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()-[:%s]->()", relTypes[0])
                    .isEqualTo(1);
            softly.assertThat(store.relationshipCount(ANY_LABEL, relTypeIds[1], ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()-[:%s]->()", relTypes[1])
                    .isEqualTo(1);
            softly.assertThat(store.relationshipCount(ANY_LABEL, relTypeIds[2], ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()-[:%s]->()", relTypes[2])
                    .isEqualTo(1);
            softly.assertThat(store.relationshipCount(ANY_LABEL, relTypeIds[3], ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()-[:%s]->()", relTypes[3])
                    .isEqualTo(1);
            softly.assertThat(store.relationshipCount(ANY_LABEL, relTypeIds[4], ANY_LABEL, NULL_CONTEXT))
                    .as("count: ()-[:%s]->()", relTypes[4])
                    .isEqualTo(0);
            softly.assertThat(store.relationshipCount(ANY_LABEL, relTypeIds[1], labelIds[1], NULL_CONTEXT))
                    .as("count: ()-[:%s]->(:%s)", relTypes[1], labels[1])
                    .isEqualTo(1);
            softly.assertThat(store.relationshipCount(ANY_LABEL, ANY_RELATIONSHIP_TYPE, labelIds[1], NULL_CONTEXT))
                    .as("count: ()-[]->(:%s)", labels[1])
                    .isEqualTo(2);
            softly.assertThat(store.relationshipCount(labelIds[0], ANY_RELATIONSHIP_TYPE, ANY_LABEL, NULL_CONTEXT))
                    .as("count: (:%s)-[]->()", labels[0])
                    .isEqualTo(3);
        }
    }

    private static Label[] createLabels(int n) {
        return IntStream.range(0, n)
                .mapToObj(i -> "Label" + i)
                .map(Label::label)
                .toArray(Label[]::new);
    }

    private static int[] getLabelIdsFrom(Transaction tx, Label... labels) throws KernelException {
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        TokenWrite tokenWrite = ktx.tokenWrite();

        int[] ids = new int[labels.length];
        tokenWrite.relationshipTypeGetOrCreateForNames(
                Arrays.stream(labels).map(Label::name).toArray(String[]::new), ids);
        return ids;
    }

    private static RelationshipType[] createRelationShipTypes(int n) {
        return IntStream.range(0, n)
                .mapToObj(i -> "TYPE" + i)
                .map(RelationshipType::withName)
                .toArray(RelationshipType[]::new);
    }

    private static int[] getRelTypeIdsFrom(Transaction tx, RelationshipType... types) throws KernelException {
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        TokenWrite tokenWrite = ktx.tokenWrite();

        int[] ids = new int[types.length];
        tokenWrite.relationshipTypeGetOrCreateForNames(
                Arrays.stream(types).map(RelationshipType::name).toArray(String[]::new), ids);
        return ids;
    }

    private Path countsStoreFile() {
        return databaseLayout.countStore();
    }

    private static long getLastTxId(GraphDatabaseAPI db) {
        return db.getDependencyResolver()
                .resolveDependency(TransactionIdStore.class)
                .getLastCommittedTransactionId();
    }

    private class AssertEmptyCountStoreVisitor extends CountsVisitor.Adapter {
        @Override
        public void visitNodeCount(int labelId, long count) {
            softly.assertThat(count).as("count: (%d)", labelId).isEqualTo(0);
        }

        @Override
        public void visitRelationshipCount(int startLabelId, int typeId, int endLabelId, long count) {
            softly.assertThat(count)
                    .as("count: (%d)-[%d]->(%d)", startLabelId, typeId, endLabelId)
                    .isEqualTo(0);
        }
    }

    private void cleanupCountsForRebuilding() throws IOException {
        fileSystem.deleteFile(countsStoreFile());
    }

    private GBPTreeCountsStore createCountsStore(ImmutableSet<OpenOption> openOptions) throws IOException {
        return createCountsStore(CountsBuilder.EMPTY, openOptions);
    }

    private GBPTreeCountsStore createCountsStore(CountsBuilder builder, ImmutableSet<OpenOption> openOptions)
            throws IOException {
        return new GBPTreeCountsStore(
                pageCache,
                databaseLayout.countStore(),
                fileSystem,
                immediate(),
                builder,
                false,
                GBPTreeCountsStore.NO_MONITOR,
                databaseLayout.getDatabaseName(),
                1_000,
                NullLogProvider.getInstance(),
                CONTEXT_FACTORY,
                PAGE_CACHE_TRACER,
                openOptions);
    }

    private void rebuildCounts(long lastCommittedTransactionId) throws IOException {
        rebuildCounts(lastCommittedTransactionId, ProgressMonitorFactory.NONE);
    }

    private void rebuildCounts(long lastCommittedTransactionId, ProgressMonitorFactory progressMonitorFactory)
            throws IOException {
        cleanupCountsForRebuilding();

        IdGeneratorFactory idGenFactory = new DefaultIdGeneratorFactory(
                fileSystem, immediate(), PAGE_CACHE_TRACER, databaseLayout.getDatabaseName());
        StoreFactory storeFactory = new StoreFactory(
                databaseLayout,
                CONFIG,
                idGenFactory,
                pageCache,
                PAGE_CACHE_TRACER,
                fileSystem,
                LOG_PROVIDER,
                CONTEXT_FACTORY,
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);

        try (NeoStores neoStores = storeFactory.openAllNeoStores()) {
            NodeStore nodeStore = neoStores.getNodeStore();
            RelationshipStore relationshipStore = neoStores.getRelationshipStore();
            var labelTokenStore = neoStores.getLabelTokenStore();
            int highLabelId = (int) labelTokenStore.getIdGenerator().getHighId();
            var relTypeTokenStore = neoStores.getRelationshipTypeTokenStore();
            int highRelationshipTypeId =
                    (int) relTypeTokenStore.getIdGenerator().getHighId();
            CountsComputer countsComputer = new CountsComputer(
                    neoStores,
                    lastCommittedTransactionId,
                    nodeStore,
                    relationshipStore,
                    highLabelId,
                    highRelationshipTypeId,
                    NumberArrayFactories.AUTO_WITHOUT_PAGECACHE,
                    progressMonitorFactory,
                    CONTEXT_FACTORY,
                    INSTANCE);
            try (var countsStore = createCountsStore(countsComputer, neoStores.getOpenOptions())) {
                countsStore.start(NULL_CONTEXT, INSTANCE);
                countsStore.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private CountsBuilder matchingBuilder(long lastCommittedTransactionId) {
        return new CountsBuilder() {
            @Override
            public void initialize(CountsUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
                throw new UnsupportedOperationException(
                        "Expected a matching transaction ID " + lastCommittedTransactionId);
            }

            @Override
            public long lastCommittedTxId() {
                return lastCommittedTransactionId;
            }
        };
    }

    private ImmutableSet<OpenOption> getDBOpenOptions(GraphDatabaseAPI db) {
        return db.getDependencyResolver()
                .resolveDependency(RecordStorageEngine.class)
                .getOpenOptions();
    }

    private static class InvocationTrackingProgressMonitorFactory extends ProgressMonitorFactory {
        private boolean addInvoked;
        private boolean startInvoked;

        boolean isAddInvoked() {
            return addInvoked;
        }

        boolean isStartInvoked() {
            return startInvoked;
        }

        @Override
        protected Indicator newIndicator(String process) {
            return new Indicator(100) {
                @Override
                public void startProcess(long totalCount) {
                    startInvoked = true;
                }

                @Override
                protected void progress(int from, int to) {
                    addInvoked = true;
                }
            };
        }
    }
}
