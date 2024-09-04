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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.locking.LockManager.NO_LOCKS_LOCK_MANAGER;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService.IndexProxyProvider;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@ExtendWith(RandomExtension.class)
class DynamicIndexStoreViewTest {
    private final JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    @Inject
    private RandomSupport random;

    @AfterEach
    void tearDown() throws Exception {
        jobScheduler.close();
    }

    @Test
    void shouldVisitNodesUsingTokenIndex() throws Exception {
        long[] nodeIds = {1, 2, 3, 4, 5, 6, 7, 8};
        int[] indexedLabels = {2, 6};
        StubStorageCursors cursors = new StubStorageCursors().withTokenIndexes();
        StorageEngine storageEngine = mockedStorageEngine(cursors, false);
        IndexProxy indexProxy = mock(IndexProxy.class);
        IndexProxyProvider indexProxies = mock(IndexProxyProvider.class);
        StubTokenIndexReader tokenReader = new StubTokenIndexReader();
        IndexDescriptor descriptor = forSchema(
                        ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR, AllIndexProviderDescriptors.TOKEN_DESCRIPTOR)
                .withName("index")
                .materialise(0);
        when(indexProxy.getState()).thenReturn(InternalIndexState.ONLINE);
        when(indexProxy.newTokenReader()).thenReturn(tokenReader);
        when(indexProxy.getDescriptor()).thenReturn(descriptor);
        when(indexProxies.getIndexProxy(any())).thenReturn(indexProxy);
        // Nodes indexed by label
        for (long nodeId : nodeIds) {
            cursors.withNode(nodeId).propertyId(1).relationship(1).labels(2, 6);
            tokenReader.index(indexedLabels, nodeId);
        }

        // Nodes not indexed
        cursors.withNode(9).labels(5);
        cursors.withNode(10).labels(6);

        DynamicIndexStoreView storeView = dynamicIndexStoreView(storageEngine, indexProxies);
        TestTokenScanConsumer consumer = new TestTokenScanConsumer();
        try (StoreScan storeScan = storeView.visitNodes(
                indexedLabels,
                PropertySelection.ALL_PROPERTIES,
                new TestPropertyScanConsumer(),
                consumer,
                false,
                true,
                CONTEXT_FACTORY,
                INSTANCE)) {
            storeScan.run(StoreScan.NO_EXTERNAL_UPDATES);
        }

        assertThat(consumer.batches.size()).isEqualTo(1);
        assertThat(consumer.batches.get(0).size()).isEqualTo(nodeIds.length);
    }

    @Test
    void shouldVisitRelationshipsUsingTokenIndex() throws Throwable {
        // Given
        StubTokenIndexReader tokenReader = new StubTokenIndexReader();
        StubStorageCursors cursors = new StubStorageCursors().withTokenIndexes();
        StorageEngine storageEngine = mockedStorageEngine(cursors, false);
        IndexProxy indexProxy = mock(IndexProxy.class);
        IndexProxyProvider indexProxies = mock(IndexProxyProvider.class);
        IndexDescriptor descriptor = forSchema(
                        ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR, AllIndexProviderDescriptors.TOKEN_DESCRIPTOR)
                .withName("index")
                .materialise(0);
        when(indexProxy.getState()).thenReturn(InternalIndexState.ONLINE);
        when(indexProxy.getDescriptor()).thenReturn(descriptor);
        when(indexProxy.newTokenReader()).thenReturn(tokenReader);
        when(indexProxies.getIndexProxy(any())).thenReturn(indexProxy);

        int targetType = 1;
        int notTargetType = 2;
        int[] indexedTypes = {targetType};
        String targetPropertyKey = "key";
        String notTargetPropertyKey = "not-key";
        Value propertyValue = Values.stringValue("value");
        MutableLongList relationshipsWithTargetType = LongLists.mutable.empty();
        long id = 0;
        int wantedPropertyUpdates = 5;
        for (int i = 0; i < wantedPropertyUpdates; i++) {
            // Relationships that are indexed
            cursors.withRelationship(id, 1, targetType, 3).properties(targetPropertyKey, propertyValue);
            tokenReader.index(indexedTypes, id);
            relationshipsWithTargetType.add(id++);

            // Relationship with wrong property
            cursors.withRelationship(id++, 1, targetType, 3).properties(notTargetPropertyKey, propertyValue);

            // Relationship with wrong type
            cursors.withRelationship(id++, 1, notTargetType, 3).properties(targetPropertyKey, propertyValue);
        }

        // When
        DynamicIndexStoreView storeView = dynamicIndexStoreView(storageEngine, indexProxies);
        TestTokenScanConsumer tokenConsumer = new TestTokenScanConsumer();
        TestPropertyScanConsumer propertyScanConsumer = new TestPropertyScanConsumer();
        StoreScan storeScan = storeView.visitRelationships(
                indexedTypes,
                PropertySelection.ALL_PROPERTIES,
                propertyScanConsumer,
                tokenConsumer,
                false,
                true,
                CONTEXT_FACTORY,
                INSTANCE);
        storeScan.run(StoreScan.NO_EXTERNAL_UPDATES);

        // Then make sure all the fitting relationships where included
        assertThat(propertyScanConsumer.batches.size()).isEqualTo(1);
        assertThat(propertyScanConsumer.batches.get(0).size()).isEqualTo(wantedPropertyUpdates);
        // and that we didn't visit any more relationships than what we would get from scan store
        assertThat(tokenConsumer.batches.size()).isEqualTo(1);
        assertThat(tokenConsumer.batches.get(0).size()).isEqualTo(relationshipsWithTargetType.size());
    }

    @Test
    void shouldVisitAllNodesWithoutTokenIndexes() {
        long[] nodeIds = {1, 2, 3, 4, 5, 6, 7, 8};
        int[] indexedLabels = {2, 6};
        StubStorageCursors cursors = new StubStorageCursors().withoutTokenIndexes();
        StorageEngine storageEngine = mockedStorageEngine(cursors, false);
        IndexProxyProvider indexProxies = mock(IndexProxyProvider.class);
        // Nodes indexed by label
        for (long nodeId : nodeIds) {
            cursors.withNode(nodeId).propertyId(1).relationship(1).labels(2, 6);
        }

        // Nodes not in index
        cursors.withNode(9).labels(5);
        cursors.withNode(10).labels(6);

        DynamicIndexStoreView storeView = dynamicIndexStoreView(storageEngine, indexProxies);
        TestTokenScanConsumer consumer = new TestTokenScanConsumer();
        try (StoreScan storeScan = storeView.visitNodes(
                indexedLabels,
                PropertySelection.ALL_PROPERTIES,
                new TestPropertyScanConsumer(),
                consumer,
                false,
                true,
                CONTEXT_FACTORY,
                INSTANCE)) {
            storeScan.run(StoreScan.NO_EXTERNAL_UPDATES);
        }

        assertThat(consumer.batches.size()).isEqualTo(1);
        assertThat(consumer.batches.get(0).size()).isEqualTo(nodeIds.length + 2);
    }

    @Test
    void shouldVisitAllRelationshipsWithoutTokenIndexes() {
        StubStorageCursors cursors = new StubStorageCursors().withoutTokenIndexes();
        StorageEngine storageEngine = mockedStorageEngine(cursors, false);
        IndexProxyProvider indexProxies = mock(IndexProxyProvider.class);

        int targetType = 1;
        int notTargetType = 2;
        int[] targetTypeArray = {targetType};
        String targetPropertyKey = "key";
        Value propertyValue = Values.stringValue("value");
        MutableLongList relationshipsWithTargetType = LongLists.mutable.empty();
        long id = 0;
        int wantedPropertyUpdates = 5;
        for (int i = 0; i < wantedPropertyUpdates; i++) {
            // Relationship fitting our target
            cursors.withRelationship(id, 1, targetType, 3).properties(targetPropertyKey, propertyValue);
            relationshipsWithTargetType.add(id++);

            // Relationship with different type
            cursors.withRelationship(id, 1, notTargetType, 3).properties(targetPropertyKey, propertyValue);
            relationshipsWithTargetType.add(id++);
        }
        int targetPropertyKeyId = cursors.propertyKeyTokenHolder().getIdByName(targetPropertyKey);
        var propertyKeyIdFilter = PropertySelection.selection(targetPropertyKeyId);

        DynamicIndexStoreView storeView = dynamicIndexStoreView(storageEngine, indexProxies);
        TestTokenScanConsumer tokenConsumer = new TestTokenScanConsumer();
        TestPropertyScanConsumer propertyScanConsumer = new TestPropertyScanConsumer();
        StoreScan storeScan = storeView.visitRelationships(
                targetTypeArray,
                propertyKeyIdFilter,
                propertyScanConsumer,
                tokenConsumer,
                false,
                true,
                CONTEXT_FACTORY,
                INSTANCE);
        storeScan.run(StoreScan.NO_EXTERNAL_UPDATES);

        assertThat(tokenConsumer.batches.size()).isEqualTo(1);
        assertThat(tokenConsumer.batches.get(0).size()).isEqualTo(relationshipsWithTargetType.size());
    }

    @Test
    void shouldVisitAllRelationshipsFromNodeBasedRelationshipTypeLookupIndex() throws Exception {
        // Given
        StubTokenIndexReader tokenReader = new StubTokenIndexReader();
        StubStorageCursors cursors = new StubStorageCursors().withTokenIndexes();
        StorageEngine storageEngine = mockedStorageEngine(cursors, true);
        IndexProxy indexProxy = mock(IndexProxy.class);
        IndexProxyProvider indexProxies = mock(IndexProxyProvider.class);
        IndexDescriptor descriptor = forSchema(
                        ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR, AllIndexProviderDescriptors.TOKEN_DESCRIPTOR)
                .withName("index")
                .materialise(0);
        when(indexProxy.getState()).thenReturn(InternalIndexState.ONLINE);
        when(indexProxy.getDescriptor()).thenReturn(descriptor);
        when(indexProxy.newTokenReader()).thenReturn(tokenReader);
        when(indexProxies.getIndexProxy(any())).thenReturn(indexProxy);

        int targetType = 1;
        int notTargetType = 2;
        int[] indexedTypes = {targetType};
        String targetPropertyKey = "key";
        String notTargetPropertyKey = "not-key";
        Value propertyValue = Values.stringValue("value");
        long id = -1;
        int numWantedPropertyUpdates = 5;
        Set<TestPropertyScanConsumer.Record> wantedPropertyUpdates = new HashSet<>();

        int numNodes = 10;
        List<StubStorageCursors.NodeData> nodes = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            nodes.add(cursors.withNode(i));
        }

        MutableLongSet indexedNodes = LongSets.mutable.empty();
        for (int i = 0; i < numWantedPropertyUpdates; i++) {
            // Relationships that are indexed
            var startNode = random.among(nodes);
            var endNode = random.among(nodes);
            cursors.withRelationship(++id, startNode.getId(), targetType, endNode.getId())
                    .properties(targetPropertyKey, propertyValue);
            wantedPropertyUpdates.add(new TestPropertyScanConsumer.Record(
                    id,
                    new int[] {targetType},
                    Map.of(cursors.propertyKeyTokenHolder().getIdByName(targetPropertyKey), propertyValue)));
            indexedNodes.add(startNode.getId());

            // Relationship with wrong property
            cursors.withRelationship(++id, startNode.getId(), targetType, endNode.getId())
                    .properties(notTargetPropertyKey, propertyValue);

            // Relationship with wrong type
            cursors.withRelationship(++id, startNode.getId(), notTargetType, endNode.getId())
                    .properties(targetPropertyKey, propertyValue);
        }
        indexedNodes.forEach(nodeId -> tokenReader.index(indexedTypes, nodeId));
        int targetPropertyKeyId = cursors.propertyKeyTokenHolder().getIdByName(targetPropertyKey);

        // When
        DynamicIndexStoreView storeView = dynamicIndexStoreView(storageEngine, indexProxies);
        TestPropertyScanConsumer propertyScanConsumer = new TestPropertyScanConsumer();
        StoreScan storeScan = storeView.visitRelationships(
                indexedTypes,
                PropertySelection.selection(targetPropertyKeyId),
                propertyScanConsumer,
                null,
                false,
                true,
                NULL_CONTEXT_FACTORY,
                INSTANCE);
        storeScan.run(StoreScan.NO_EXTERNAL_UPDATES);

        // Then make sure all the fitting relationships where included
        assertThat(propertyScanConsumer.batches.size()).isEqualTo(1);
        assertThat(new HashSet<>(propertyScanConsumer.batches.get(0))).isEqualTo(wantedPropertyUpdates);
    }

    private DynamicIndexStoreView dynamicIndexStoreView(
            StorageEngine storageEngine, IndexProxyProvider indexingService) {
        return dynamicIndexStoreView(
                storageEngine,
                indexingService,
                new FullScanStoreView(NO_LOCK_SERVICE, storageEngine, Config.defaults(), jobScheduler));
    }

    private static DynamicIndexStoreView dynamicIndexStoreView(
            StorageEngine storageEngine, IndexProxyProvider indexingService, FullScanStoreView fullScanStoreView) {
        return new DynamicIndexStoreView(
                fullScanStoreView,
                NO_LOCKS_LOCK_MANAGER,
                NO_LOCK_SERVICE,
                Config.defaults(),
                indexingService,
                storageEngine,
                NullLogProvider.getInstance());
    }

    private StorageEngine mockedStorageEngine(StubStorageCursors cursors, boolean nodeBased) {
        var storageEngine = mock(StorageEngine.class);
        when(storageEngine.newReader()).thenReturn(cursors);
        var indexingBehaviour = mock(StorageEngineIndexingBehaviour.class);
        when(indexingBehaviour.useNodeIdsInRelationshipTokenIndex()).thenReturn(nodeBased);
        when(storageEngine.indexingBehaviour()).thenReturn(indexingBehaviour);
        when(storageEngine.createStorageCursors(any())).thenReturn(StoreCursors.NULL);
        when(storageEngine.getOpenOptions()).thenReturn(Sets.immutable.empty());
        return storageEngine;
    }
}
