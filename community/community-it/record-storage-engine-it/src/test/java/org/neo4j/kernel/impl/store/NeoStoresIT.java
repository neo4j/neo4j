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

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.apache.commons.lang3.exception.ExceptionUtils.indexOfThrowable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.Race;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
class NeoStoresIT {
    @Inject
    private GraphDatabaseAPI db;

    private static final RelationshipType FRIEND = RelationshipType.withName("FRIEND");
    private static final String LONG_STRING_VALUE = randomAscii(2048);
    private final CursorContextFactory contextFactory =
            new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.db_format, FormatFamily.ALIGNED.name());
        builder.setConfig(GraphDatabaseSettings.dense_node_threshold, 1);
    }

    @Test
    void tracePageCacheAccessOnHighIdScan() {
        var storageEngine = db.getDependencyResolver().resolveDependency(RecordStorageEngine.class);
        var neoStores = storageEngine.testAccessNeoStores();
        var propertyStore = neoStores.getPropertyStore();

        for (int i = 0; i < 1000; i++) {
            try (Transaction transaction = db.beginTx()) {
                var node = transaction.createNode();
                node.setProperty("a", randomAscii(1024));
                transaction.commit();
            }
        }

        var cursorContext = contextFactory.create("tracePageCacheAccessOnHighIdScan");
        propertyStore.scanForHighId(cursorContext);

        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertEquals(1, cursorTracer.hits());
        assertEquals(1, cursorTracer.pins());
        assertEquals(1, cursorTracer.unpins());
    }

    @Test
    void tracePageCacheAccessOnGetRawRecordData() throws IOException {
        var storageEngine = db.getDependencyResolver().resolveDependency(RecordStorageEngine.class);
        var neoStores = storageEngine.testAccessNeoStores();
        var propertyStore = neoStores.getPropertyStore();

        try (Transaction transaction = db.beginTx()) {
            var node = transaction.createNode();
            node.setProperty("a", "b");
            transaction.commit();
        }

        var cursorContext = contextFactory.create("tracePageCacheAccessOnGetRawRecordData");
        try (var storeCursors = storageEngine.createStorageCursors(cursorContext)) {
            propertyStore.getRawRecordData(1L, storeCursors.readCursor(PROPERTY_CURSOR));
        }

        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertEquals(1, cursorTracer.hits());
        assertEquals(1, cursorTracer.pins());
        assertEquals(1, cursorTracer.unpins());
    }

    @Test
    void tracePageCacheAccessOnInUseCheck() {
        var storageEngine = db.getDependencyResolver().resolveDependency(RecordStorageEngine.class);
        var neoStores = storageEngine.testAccessNeoStores();
        var propertyStore = neoStores.getPropertyStore();

        try (Transaction transaction = db.beginTx()) {
            var node = transaction.createNode();
            node.setProperty("a", "b");
            transaction.commit();
        }

        var cursorContext = contextFactory.create("tracePageCacheAccessOnInUseCheck");
        try (var cursor = propertyStore.openPageCursorForReading(1L, cursorContext)) {
            propertyStore.isInUse(1L, cursor);
        }

        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertEquals(1, cursorTracer.hits());
        assertEquals(1, cursorTracer.pins());
        assertEquals(1, cursorTracer.unpins());
    }

    @Test
    void tracePageCacheAccessOnGetRecord() {
        var storageEngine = db.getDependencyResolver().resolveDependency(RecordStorageEngine.class);
        var neoStores = storageEngine.testAccessNeoStores();
        var nodeStore = neoStores.getNodeStore();

        long nodeId;
        try (Transaction transaction = db.beginTx()) {
            var node = transaction.createNode();
            node.setProperty("a", "b");
            nodeId = node.getId();
            transaction.commit();
        }

        var cursorContext = contextFactory.create("tracePageCacheAccessOnGetRecord");
        NodeRecord nodeRecord = new NodeRecord(nodeId);
        try (var cursor = nodeStore.openPageCursorForReading(nodeId, cursorContext)) {
            nodeStore.getRecordByCursor(nodeId, nodeRecord, RecordLoad.NORMAL, cursor, EmptyMemoryTracker.INSTANCE);
        }

        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertEquals(1, cursorTracer.hits());
        assertEquals(1, cursorTracer.pins());
        assertEquals(1, cursorTracer.unpins());
    }

    @Test
    void tracePageCacheAccessOnUpdateRecord() {
        var storageEngine = db.getDependencyResolver().resolveDependency(RecordStorageEngine.class);
        var neoStores = storageEngine.testAccessNeoStores();
        var storeCursors = storageEngine.createStorageCursors(NULL_CONTEXT);
        var nodeStore = neoStores.getNodeStore();

        long nodeId;
        try (Transaction transaction = db.beginTx()) {
            var node = transaction.createNode();
            node.setProperty("a", "b");
            nodeId = node.getId();
            transaction.commit();
        }

        var cursorContext = contextFactory.create("tracePageCacheAccessOnUpdateRecord");
        try (var storeCursor = storeCursors.writeCursor(NODE_CURSOR)) {
            nodeStore.updateRecord(new NodeRecord(nodeId), storeCursor, cursorContext, storeCursors);
        }

        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertEquals(4, cursorTracer.hits());
        assertEquals(5, cursorTracer.pins());
        assertEquals(5, cursorTracer.unpins());
    }

    @Test
    void tracePageCacheAccessOnTokenReads() {
        var storageEngine = db.getDependencyResolver().resolveDependency(RecordStorageEngine.class);
        var neoStores = storageEngine.testAccessNeoStores();
        var propertyKeys = neoStores.getPropertyKeyTokenStore();

        try (Transaction transaction = db.beginTx()) {
            var node = transaction.createNode();
            node.setProperty("a", "b");
            transaction.commit();
        }

        var cursorContext = contextFactory.create("tracePageCacheAccessOnTokenReads");
        try (StoreCursors storageCursors = storageEngine.createStorageCursors(cursorContext)) {
            propertyKeys.getAllReadableTokens(storageCursors, EmptyMemoryTracker.INSTANCE);
        }

        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertEquals(2, cursorTracer.hits());
        assertEquals(2, cursorTracer.pins());
        assertEquals(2, cursorTracer.unpins());
    }

    @Test
    void shouldWriteOutTheDynamicChainBeforeUpdatingThePropertyRecord() throws Throwable {
        Race race = new Race();
        long[] latestNodeId = new long[1];
        AtomicLong writes = new AtomicLong();
        AtomicLong reads = new AtomicLong();
        long endTime = currentTimeMillis() + SECONDS.toMillis(2);
        race.withEndCondition(() -> (writes.get() > 100 && reads.get() > 10_000) || currentTimeMillis() > endTime);
        race.addContestant(() -> {
            try (Transaction tx = db.beginTx()) {
                Node node = tx.createNode();
                latestNodeId[0] = node.getId();
                node.setProperty("largeProperty", LONG_STRING_VALUE);
                tx.commit();
            }
            writes.incrementAndGet();
        });
        race.addContestant(() -> {
            try (Transaction tx = db.beginTx()) {
                Node node = tx.getNodeById(latestNodeId[0]);
                for (String propertyKey : node.getPropertyKeys()) {
                    node.getProperty(propertyKey);
                }
                tx.commit();
            } catch (NotFoundException e) {
                // This will catch nodes not found (expected) and also PropertyRecords not found (shouldn't happen
                // but handled in shouldWriteOutThePropertyRecordBeforeReferencingItFromANodeRecord)
            }
            reads.incrementAndGet();
        });
        race.go();
    }

    @Test
    void shouldWriteOutThePropertyRecordBeforeReferencingItFromANodeRecord() throws Throwable {
        Race race = new Race();
        long[] latestNodeId = new long[1];
        AtomicLong writes = new AtomicLong();
        AtomicLong reads = new AtomicLong();
        long endTime = currentTimeMillis() + SECONDS.toMillis(2);
        race.withEndCondition(() -> (writes.get() > 100 && reads.get() > 10_000) || currentTimeMillis() > endTime);
        race.addContestant(() -> {
            try (Transaction tx = db.beginTx()) {
                Node node = tx.createNode();
                latestNodeId[0] = node.getId();
                node.setProperty("largeProperty", LONG_STRING_VALUE);
                tx.commit();
            }
            writes.incrementAndGet();
        });
        race.addContestant(() -> {
            try (Transaction tx = db.beginTx()) {
                Node node = tx.getNodeById(latestNodeId[0]);

                for (String propertyKey : node.getPropertyKeys()) {
                    node.getProperty(propertyKey);
                }
                tx.commit();
            } catch (NotFoundException e) {
                if (indexOfThrowable(e, InvalidRecordException.class) != -1) {
                    throw e;
                }
            }
            reads.incrementAndGet();
        });
        race.go();
    }

    @Test
    void shouldWriteOutThePropertyRecordBeforeReferencingItFromARelationshipRecord() throws Throwable {
        final long node1Id;
        final long node2Id;
        try (Transaction tx = db.beginTx()) {
            Node node1 = tx.createNode();
            node1Id = node1.getId();

            Node node2 = tx.createNode();
            node2Id = node2.getId();

            tx.commit();
        }

        Race race = new Race();
        final long[] latestRelationshipId = new long[1];
        AtomicLong writes = new AtomicLong();
        AtomicLong reads = new AtomicLong();
        long endTime = currentTimeMillis() + SECONDS.toMillis(2);
        race.withEndCondition(() -> (writes.get() > 100 && reads.get() > 10_000) || currentTimeMillis() > endTime);
        race.addContestant(() -> {
            try (Transaction tx = db.beginTx()) {
                Node node1 = tx.getNodeById(node1Id);
                Node node2 = tx.getNodeById(node2Id);

                Relationship rel = node1.createRelationshipTo(node2, FRIEND);
                latestRelationshipId[0] = rel.getId();
                rel.setProperty("largeProperty", LONG_STRING_VALUE);

                tx.commit();
            }
            writes.incrementAndGet();
        });
        race.addContestant(() -> {
            try (Transaction tx = db.beginTx()) {
                Relationship rel = tx.getRelationshipById(latestRelationshipId[0]);

                for (String propertyKey : rel.getPropertyKeys()) {
                    rel.getProperty(propertyKey);
                }
                tx.commit();
            } catch (NotFoundException e) {
                if (indexOfThrowable(e, InvalidRecordException.class) != -1) {
                    throw e;
                }
            }
            reads.incrementAndGet();
        });
        race.go();
    }
}
