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
package org.neo4j.kernel.impl.transaction.state.storeview;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.api.index.StoreScan.NO_EXTERNAL_UPDATES;
import static org.neo4j.lock.LockType.SHARED;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;
import static org.neo4j.test.PageCacheTracerAssertions.assertThatTracing;
import static org.neo4j.test.PageCacheTracerAssertions.pins;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.Lock;
import org.neo4j.lock.LockService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.Values;

@DbmsExtension
class FullScanStoreViewTest {
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    @Inject
    private GraphDatabaseAPI graphDb;

    @Inject
    private StorageEngine storageEngine;

    @Inject
    private CheckPointer checkPointer;

    @Inject
    private JobScheduler jobScheduler;

    private final Map<Long, Lock> lockMocks = new HashMap<>();
    private final Label label = Label.label("Person");
    private final RelationshipType relationshipType = RelationshipType.withName("Knows");

    private FullScanStoreView storeView;

    private int labelId;
    private int relTypeId;
    private int propertyKeyId;
    private int relPropertyKeyId;

    private Node alistair;
    private Node stefan;
    private LockService locks;
    private Relationship aKnowsS;
    private Relationship sKnowsA;
    private StorageReader reader;

    @BeforeEach
    void before() throws KernelException {
        createAlistairAndStefanNodes();
        getOrCreateIds();

        jobScheduler = JobSchedulerFactory.createInitialisedScheduler();

        locks = mock(LockService.class);
        when(locks.acquireNodeLock(anyLong(), any())).thenAnswer(invocation -> {
            Long nodeId = invocation.getArgument(0);
            return lockMocks.computeIfAbsent(nodeId, k -> mock(Lock.class));
        });
        when(locks.acquireRelationshipLock(anyLong(), any())).thenAnswer(invocation -> {
            Long nodeId = invocation.getArgument(0);
            return lockMocks.computeIfAbsent(nodeId, k -> mock(Lock.class));
        });
        storeView = new FullScanStoreView(locks, storageEngine, Config.defaults(), jobScheduler);
        reader = storageEngine.newReader();
    }

    @AfterEach
    void after() throws Exception {
        jobScheduler.close();
        reader.close();
    }

    @Test
    void shouldScanExistingNodesForALabel() {
        // given
        TestPropertyScanConsumer propertyScanConsumer = new TestPropertyScanConsumer();
        try (StoreScan storeScan = storeView.visitNodes(
                new int[] {labelId},
                PropertySelection.selection(propertyKeyId),
                propertyScanConsumer,
                new TestTokenScanConsumer(),
                false,
                true,
                CONTEXT_FACTORY,
                INSTANCE)) {

            // when
            storeScan.run(NO_EXTERNAL_UPDATES);
        }

        // then
        assertThat(propertyScanConsumer.batches.get(0))
                .containsExactlyInAnyOrder(
                        record(alistair.getId(), propertyKeyId, "Alistair", new long[] {labelId}),
                        record(stefan.getId(), propertyKeyId, "Stefan", new long[] {labelId}));
    }

    @Test
    void shouldScanExistingRelationshipsForARelationshipType() {
        // given
        TestPropertyScanConsumer propertyScanConsumer = new TestPropertyScanConsumer();
        StoreScan storeScan = storeView.visitRelationships(
                new int[] {relTypeId},
                PropertySelection.selection(relPropertyKeyId),
                propertyScanConsumer,
                null,
                true,
                true,
                CONTEXT_FACTORY,
                INSTANCE);

        // when
        storeScan.run(NO_EXTERNAL_UPDATES);

        // then
        assertThat(propertyScanConsumer.batches.get(0))
                .containsExactlyInAnyOrder(
                        record(aKnowsS.getId(), relPropertyKeyId, "long", new long[] {relTypeId}),
                        record(sKnowsA.getId(), relPropertyKeyId, "lengthy", new long[] {relTypeId}));
    }

    @Test
    void shouldIgnoreDeletedNodesDuringScan() {
        // given
        deleteAlistairAndStefanNodes();

        TestPropertyScanConsumer propertyScanConsumer = new TestPropertyScanConsumer();
        try (StoreScan storeScan = storeView.visitNodes(
                new int[] {labelId},
                PropertySelection.selection(propertyKeyId),
                propertyScanConsumer,
                new TestTokenScanConsumer(),
                false,
                true,
                CONTEXT_FACTORY,
                INSTANCE)) {

            // when
            storeScan.run(NO_EXTERNAL_UPDATES);
        }

        // then
        assertTrue(propertyScanConsumer.batches.isEmpty());
    }

    @Test
    void shouldIgnoreDeletedRelationshipsDuringScan() {
        // given
        deleteAlistairAndStefanNodes();

        TestPropertyScanConsumer propertyScanConsumer = new TestPropertyScanConsumer();
        StoreScan storeScan = storeView.visitRelationships(
                new int[] {relTypeId},
                PropertySelection.selection(relPropertyKeyId),
                propertyScanConsumer,
                null,
                true,
                true,
                CONTEXT_FACTORY,
                INSTANCE);

        // when
        storeScan.run(NO_EXTERNAL_UPDATES);

        // then
        assertTrue(propertyScanConsumer.batches.isEmpty());
    }

    @Test
    void shouldLockNodesWhileReadingThem() {
        // given
        try (StoreScan storeScan = storeView.visitNodes(
                new int[] {labelId},
                PropertySelection.selection(propertyKeyId),
                new TestPropertyScanConsumer(),
                null,
                false,
                true,
                CONTEXT_FACTORY,
                INSTANCE)) {

            // when
            storeScan.run(NO_EXTERNAL_UPDATES);
        }

        // then
        assertThat(lockMocks.size())
                .as("allocated locks: " + lockMocks.keySet())
                .isGreaterThanOrEqualTo(2);
        Lock lock0 = lockMocks.get(0L);
        Lock lock1 = lockMocks.get(1L);
        assertNotNull(lock0, "Lock[node=0] never acquired");
        assertNotNull(lock1, "Lock[node=1] never acquired");
        InOrder order = inOrder(locks, lock0, lock1);
        order.verify(locks).acquireNodeLock(0, SHARED);
        order.verify(lock0).close();
        order.verify(locks).acquireNodeLock(1, SHARED);
        order.verify(lock1).close();
    }

    @Test
    void shouldLockRelationshipsWhileReadingThem() {
        // given
        StoreScan storeScan = storeView.visitRelationships(
                new int[] {relTypeId},
                PropertySelection.selection(relPropertyKeyId),
                new TestPropertyScanConsumer(),
                null,
                true,
                true,
                CONTEXT_FACTORY,
                INSTANCE);

        // when
        storeScan.run(NO_EXTERNAL_UPDATES);

        // then
        assertThat(lockMocks.size())
                .as("allocated locks: " + lockMocks.keySet())
                .isGreaterThanOrEqualTo(2);
        Lock lock0 = lockMocks.get(aKnowsS.getId());
        Lock lock1 = lockMocks.get(sKnowsA.getId());
        assertNotNull(lock0, "Lock[relationship=" + aKnowsS.getId() + "] never acquired");
        assertNotNull(lock1, "Lock[relationship=" + sKnowsA.getId() + "] never acquired");
        InOrder order = inOrder(locks, lock0, lock1);
        order.verify(locks).acquireRelationshipLock(aKnowsS.getId(), SHARED);
        order.verify(lock0).close();
        order.verify(locks).acquireRelationshipLock(sKnowsA.getId(), SHARED);
        order.verify(lock1).close();
    }

    @Test
    void tracePageCacheAccessOnStoreViewNodeScan() throws IOException {
        // enforce checkpoint to flush tree caches
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("forcedCheckpoint"));

        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        var propertyScanConsumer = new TestPropertyScanConsumer();
        var scan = new NodeStoreScan(
                Config.defaults(),
                storageEngine.newReader(),
                storageEngine::createStorageCursors,
                locks,
                null,
                propertyScanConsumer,
                new int[] {labelId},
                ALL_PROPERTIES,
                false,
                jobScheduler,
                contextFactory,
                INSTANCE,
                false);
        scan.run(NO_EXTERNAL_UPDATES);

        assertThat(propertyScanConsumer.batches.get(0).size()).isEqualTo(2);

        assertThatTracing(graphDb)
                .record(pins(4).noFaults())
                .block(pins(3).noFaults())
                .matches(pageCacheTracer);
    }

    @Test
    void tracePageCacheAccessOnRelationshipStoreScan() throws Exception {
        // enforce checkpoint to flush tree caches
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("forcedCheckpoint"));

        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        var propertyScanConsumer = new TestPropertyScanConsumer();
        var scan = new RelationshipStoreScan(
                Config.defaults(),
                storageEngine.newReader(),
                storageEngine::createStorageCursors,
                locks,
                null,
                propertyScanConsumer,
                new int[] {relTypeId},
                ALL_PROPERTIES,
                false,
                jobScheduler,
                contextFactory,
                INSTANCE,
                false);
        scan.run(NO_EXTERNAL_UPDATES);

        assertThat(propertyScanConsumer.batches.get(0).size()).isEqualTo(2);

        assertThatTracing(graphDb)
                .record(pins(3).noFaults())
                .block(pins(3).noFaults())
                .matches(pageCacheTracer);
    }

    @Test
    void processAllRelationshipTypes() {
        // Given
        TestTokenScanConsumer tokenScanConsumer = new TestTokenScanConsumer();
        StoreScan storeViewRelationshipStoreScan = storeView.visitRelationships(
                EMPTY_INT_ARRAY, ALL_PROPERTIES, null, tokenScanConsumer, true, true, CONTEXT_FACTORY, INSTANCE);

        // When
        storeViewRelationshipStoreScan.run(NO_EXTERNAL_UPDATES);

        // Then
        var updates = tokenScanConsumer.batches.get(0);
        assertThat(updates.size()).isEqualTo(2);
        for (var update : updates) {
            long[] tokensAfter = update.getTokens();
            assertThat(tokensAfter.length).isEqualTo(1);
            assertThat(tokensAfter[0]).isEqualTo(0);
            assertThat(update.getEntityId()).satisfiesAnyOf(id -> assertThat(id).isEqualTo(0), id -> assertThat(id)
                    .isEqualTo(1));
        }
    }

    private static TestPropertyScanConsumer.Record record(long nodeId, int propertyKeyId, Object value, long[] labels) {
        return new TestPropertyScanConsumer.Record(nodeId, labels, Map.of(propertyKeyId, Values.of(value)));
    }

    private void createAlistairAndStefanNodes() {
        try (Transaction tx = graphDb.beginTx()) {
            alistair = tx.createNode(label);
            alistair.setProperty("name", "Alistair");
            alistair.setProperty("country", "UK");
            stefan = tx.createNode(label);
            stefan.setProperty("name", "Stefan");
            stefan.setProperty("country", "Deutschland");
            aKnowsS = alistair.createRelationshipTo(stefan, relationshipType);
            aKnowsS.setProperty("duration", "long");
            aKnowsS.setProperty("irrelevant", "prop");
            sKnowsA = stefan.createRelationshipTo(alistair, relationshipType);
            sKnowsA.setProperty("duration", "lengthy");
            sKnowsA.setProperty("irrelevant", "prop");
            tx.commit();
        }
    }

    private void deleteAlistairAndStefanNodes() {
        try (Transaction tx = graphDb.beginTx()) {
            tx.getRelationshipById(aKnowsS.getId()).delete();
            tx.getRelationshipById(sKnowsA.getId()).delete();
            tx.getNodeById(alistair.getId()).delete();
            tx.getNodeById(stefan.getId()).delete();
            tx.commit();
        }
    }

    private void getOrCreateIds() throws KernelException {
        try (Transaction tx = graphDb.beginTx()) {
            TokenWrite tokenWrite =
                    ((InternalTransaction) tx).kernelTransaction().tokenWrite();
            labelId = tokenWrite.labelGetOrCreateForName("Person");
            relTypeId = tokenWrite.relationshipTypeGetOrCreateForName("Knows");
            propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName("name");
            relPropertyKeyId = tokenWrite.propertyKeyGetOrCreateForName("duration");
            tx.commit();
        }
    }
}
