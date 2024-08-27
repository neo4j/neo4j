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
package org.neo4j.kernel.impl.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.GROUP_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.test.utils.PageCacheConfig.config;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;

@Neo4jLayoutExtension
class RelationshipGroupStoreTest {
    @RegisterExtension
    static final PageCacheSupportExtension pageCacheExtension =
            new PageCacheSupportExtension(config().withInconsistentReads(false));

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    private int defaultThreshold;
    private GraphDatabaseAPI db;
    private DatabaseManagementService managementService;
    private PageCache pageCache;

    @BeforeEach
    void before() {
        defaultThreshold = dense_node_threshold.defaultValue();
        pageCache = pageCacheExtension.getPageCache(fs);
    }

    @AfterEach
    void after() {
        if (db != null) {
            managementService.shutdown();
        }
        pageCache.close();
    }

    @Test
    void createWithDefaultThreshold() {
        createAndVerify(null);
    }

    @Test
    void createWithCustomThreshold() {
        createAndVerify(defaultThreshold * 2);
    }

    @Test
    void createDenseNodeWithLowThreshold() {
        newDb(2);

        // Create node with two relationships
        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST);
            node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST2);
            assertEquals(2, node.getDegree());
            assertEquals(1, node.getDegree(MyRelTypes.TEST));
            assertEquals(1, node.getDegree(MyRelTypes.TEST2));
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            tx.getNodeById(node.getId()).createRelationshipTo(tx.createNode(), MyRelTypes.TEST);
            tx.commit();
        }

        managementService.shutdown();
    }

    private void newDb(int denseNodeThreshold) {
        managementService = new TestDatabaseManagementServiceBuilder()
                .impermanent()
                .setConfig(db_format, RecordFormatSelector.defaultFormat().name())
                .setConfig(dense_node_threshold, denseNodeThreshold)
                .build();
        db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        fs = db.getDependencyResolver().resolveDependency(FileSystemAbstraction.class);
    }

    private void createAndVerify(Integer customThreshold) {
        int expectedThreshold = customThreshold != null ? customThreshold : defaultThreshold;
        StoreFactory factory = factory(customThreshold);
        NeoStores neoStores = factory.openAllNeoStores();
        assertEquals(expectedThreshold, neoStores.getRelationshipGroupStore().getStoreHeaderInt());
        neoStores.close();

        // Next time we open it it should be the same
        neoStores = factory.openAllNeoStores();
        assertEquals(expectedThreshold, neoStores.getRelationshipGroupStore().getStoreHeaderInt());
        neoStores.close();

        // Even if we open with a different config setting it should just ignore it
        factory = factory(999999);
        neoStores = factory.openAllNeoStores();
        assertEquals(expectedThreshold, neoStores.getRelationshipGroupStore().getStoreHeaderInt());
        neoStores.close();
    }

    private StoreFactory factory(Integer customThreshold) {
        return factory(customThreshold, pageCache);
    }

    private StoreFactory factory(Integer customThreshold, PageCache pageCache) {
        Config.Builder config = Config.newBuilder();
        if (customThreshold != null) {
            config.set(dense_node_threshold, customThreshold);
        }
        PageCacheTracer pageCacheTracer = PageCacheTracer.NULL;
        return new StoreFactory(
                databaseLayout,
                config.build(),
                new DefaultIdGeneratorFactory(fs, immediate(), pageCacheTracer, databaseLayout.getDatabaseName()),
                pageCache,
                pageCacheTracer,
                fs,
                NullLogProvider.getInstance(),
                new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
    }

    @Test
    void makeSureRelationshipGroupsNextAndPrevGetsAssignedCorrectly() {
        newDb(1);

        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            Node node0 = tx.createNode();
            Node node2 = tx.createNode();
            node0.createRelationshipTo(node, MyRelTypes.TEST);
            node.createRelationshipTo(node2, MyRelTypes.TEST2);

            Iterables.forEach(node.getRelationships(), Relationship::delete);
            node.delete();
            tx.commit();
        }

        managementService.shutdown();
    }

    @Test
    void verifyRecordsForDenseNodeWithOneRelType() {
        newDb(2);

        Node node;
        Relationship rel1;
        Relationship rel2;
        Relationship rel3;
        Relationship rel4;
        Relationship rel5;
        Relationship rel6;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            rel1 = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST);
            rel2 = tx.createNode().createRelationshipTo(node, MyRelTypes.TEST);
            rel3 = node.createRelationshipTo(node, MyRelTypes.TEST);
            rel4 = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST);
            rel5 = tx.createNode().createRelationshipTo(node, MyRelTypes.TEST);
            rel6 = node.createRelationshipTo(node, MyRelTypes.TEST);
            tx.commit();
        }

        NeoStores neoStores = db.getDependencyResolver()
                .resolveDependency(RecordStorageEngine.class)
                .testAccessNeoStores();
        NodeStore nodeStore = neoStores.getNodeStore();
        RecordStore<RelationshipGroupRecord> groupStore = neoStores.getRelationshipGroupStore();
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        try (var nodeCursor = nodeStore.openPageCursorForReading(0, NULL_CONTEXT);
                var groupCursor = groupStore.openPageCursorForReading(0, NULL_CONTEXT);
                var relCursor = relationshipStore.openPageCursorForReading(0, NULL_CONTEXT)) {
            NodeRecord nodeRecord = nodeStore.getRecordByCursor(
                    node.getId(), nodeStore.newRecord(), NORMAL, nodeCursor, EmptyMemoryTracker.INSTANCE);
            long group = nodeRecord.getNextRel();
            RelationshipGroupRecord groupRecord = groupStore.getRecordByCursor(
                    group, groupStore.newRecord(), NORMAL, groupCursor, EmptyMemoryTracker.INSTANCE);
            assertEquals(-1, groupRecord.getNext());
            assertEquals(-1, groupRecord.getPrev());
            assertRelationshipChain(
                    relationshipStore, relCursor, node, groupRecord.getFirstOut(), rel1.getId(), rel4.getId());
            assertRelationshipChain(
                    relationshipStore, relCursor, node, groupRecord.getFirstIn(), rel2.getId(), rel5.getId());
            assertRelationshipChain(
                    relationshipStore, relCursor, node, groupRecord.getFirstLoop(), rel3.getId(), rel6.getId());
        }
    }

    @Test
    void verifyRecordsForDenseNodeWithTwoRelTypes() {
        newDb(2);

        Node node;
        Relationship rel1;
        Relationship rel2;
        Relationship rel3;
        Relationship rel4;
        Relationship rel5;
        Relationship rel6;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            rel1 = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST);
            rel2 = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST);
            rel3 = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST);
            rel4 = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST2);
            rel5 = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST2);
            rel6 = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST2);
            tx.commit();
        }

        NeoStores neoStores = db.getDependencyResolver()
                .resolveDependency(RecordStorageEngine.class)
                .testAccessNeoStores();
        NodeStore nodeStore = neoStores.getNodeStore();
        RecordStore<RelationshipGroupRecord> groupStore = neoStores.getRelationshipGroupStore();
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        try (var nodeCursor = nodeStore.openPageCursorForReading(0, NULL_CONTEXT);
                var groupCursor = groupStore.openPageCursorForReading(0, NULL_CONTEXT);
                var relCursor = relationshipStore.openPageCursorForReading(0, NULL_CONTEXT)) {
            NodeRecord nodeRecord = nodeStore.getRecordByCursor(
                    node.getId(), nodeStore.newRecord(), NORMAL, nodeCursor, EmptyMemoryTracker.INSTANCE);
            long group = nodeRecord.getNextRel();

            RelationshipGroupRecord groupRecord = groupStore.getRecordByCursor(
                    group, groupStore.newRecord(), NORMAL, groupCursor, EmptyMemoryTracker.INSTANCE);
            assertNotEquals(groupRecord.getNext(), -1);
            assertRelationshipChain(
                    relationshipStore,
                    relCursor,
                    node,
                    groupRecord.getFirstOut(),
                    rel1.getId(),
                    rel2.getId(),
                    rel3.getId());

            RelationshipGroupRecord otherGroupRecord = groupStore.getRecordByCursor(
                    groupRecord.getNext(), groupStore.newRecord(), NORMAL, groupCursor, EmptyMemoryTracker.INSTANCE);
            assertEquals(-1, otherGroupRecord.getNext());
            assertRelationshipChain(
                    relationshipStore,
                    relCursor,
                    node,
                    otherGroupRecord.getFirstOut(),
                    rel4.getId(),
                    rel5.getId(),
                    rel6.getId());
        }
    }

    @Test
    void verifyGroupIsDeletedWhenNeeded() {
        // TODO test on a lower level instead

        newDb(2);

        Transaction tx = db.beginTx();
        Node node = tx.createNode();
        Relationship rel1 = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST);
        Relationship rel2 = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST);
        Relationship rel3 = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST);
        Relationship rel4 = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST2);
        Relationship rel5 = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST2);
        Relationship rel6 = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST2);
        tx.commit();

        NeoStores neoStores = db.getDependencyResolver()
                .resolveDependency(RecordStorageEngine.class)
                .testAccessNeoStores();
        NodeStore nodeStore = neoStores.getNodeStore();
        RecordStore<RelationshipGroupRecord> groupStore = neoStores.getRelationshipGroupStore();
        try (var nodeCursor = nodeStore.openPageCursorForReading(0, NULL_CONTEXT);
                var groupCursor = groupStore.openPageCursorForReading(0, NULL_CONTEXT)) {
            NodeRecord nodeRecord = nodeStore.getRecordByCursor(
                    node.getId(), nodeStore.newRecord(), NORMAL, nodeCursor, EmptyMemoryTracker.INSTANCE);
            long group = nodeRecord.getNextRel();

            RelationshipGroupRecord groupRecord = groupStore.getRecordByCursor(
                    group, groupStore.newRecord(), NORMAL, groupCursor, EmptyMemoryTracker.INSTANCE);
            assertNotEquals(groupRecord.getNext(), -1);
            RelationshipGroupRecord otherGroupRecord = groupStore.getRecordByCursor(
                    groupRecord.getNext(), groupStore.newRecord(), NORMAL, groupCursor, EmptyMemoryTracker.INSTANCE);
            assertEquals(-1, otherGroupRecord.getNext());
        }

        // TODO Delete all relationships of one type and see to that the correct group is deleted.
    }

    @Test
    void checkingIfRecordIsInUseMustHappenAfterConsistentRead() {
        AtomicBoolean nextReadIsInconsistent = new AtomicBoolean(false);
        try (PageCache pageCache =
                PageCacheSupportExtension.getPageCache(fs, config().withInconsistentReads(nextReadIsInconsistent))) {
            StoreFactory factory = factory(null, pageCache);

            try (NeoStores neoStores = factory.openAllNeoStores();
                    var storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT)) {
                RecordStore<RelationshipGroupRecord> relationshipGroupStore = neoStores.getRelationshipGroupStore();
                RelationshipGroupRecord record = new RelationshipGroupRecord(1)
                        .initialize(true, 2, 3, 4, 5, 6, Record.NO_NEXT_RELATIONSHIP.intValue());
                try (var storeCursor = storeCursors.writeCursor(GROUP_CURSOR)) {
                    relationshipGroupStore.updateRecord(
                            record, storeCursor, CursorContext.NULL_CONTEXT, StoreCursors.NULL);
                }
                nextReadIsInconsistent.set(true);
                // Now the following should not throw any RecordNotInUse exceptions
                RelationshipGroupRecord readBack = relationshipGroupStore.getRecordByCursor(
                        1,
                        relationshipGroupStore.newRecord(),
                        NORMAL,
                        storeCursors.readCursor(GROUP_CURSOR),
                        EmptyMemoryTracker.INSTANCE);
                assertThat(readBack.toString()).isEqualTo(record.toString());
            }
        }
    }

    private static void assertRelationshipChain(
            RelationshipStore relationshipStore, PageCursor pageCursor, Node node, long firstId, long... chainedIds) {
        long nodeId = node.getId();
        RelationshipRecord record = relationshipStore.getRecordByCursor(
                firstId, relationshipStore.newRecord(), NORMAL, pageCursor, EmptyMemoryTracker.INSTANCE);
        Set<Long> readChain = new HashSet<>();
        readChain.add(firstId);
        while (true) {
            long nextId = record.getFirstNode() == nodeId ? record.getFirstNextRel() : record.getSecondNextRel();
            if (nextId == -1) {
                break;
            }

            readChain.add(nextId);
            relationshipStore.getRecordByCursor(nextId, record, NORMAL, pageCursor, EmptyMemoryTracker.INSTANCE);
        }

        Set<Long> expectedChain = new HashSet<>(Collections.singletonList(firstId));
        for (long id : chainedIds) {
            expectedChain.add(id);
        }
        assertEquals(expectedChain, readChain);
    }

    enum MyRelTypes implements RelationshipType {
        TEST,
        TEST2
    }
}
