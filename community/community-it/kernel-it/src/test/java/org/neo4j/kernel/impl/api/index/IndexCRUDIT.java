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
package org.neo4j.kernel.impl.api.index;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.schema.IndexType.LOOKUP;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceIndexProviderFactory;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.IndexingTestUtil;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.index.schema.CollectingIndexUpdater;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.values.storable.Values;

@ExtendWith(EphemeralFileSystemExtension.class)
class IndexCRUDIT {
    private FileSystemAbstraction fs;

    private GraphDatabaseAPI db;
    private final IndexProvider mockedIndexProvider = mock(IndexProvider.class);
    private final ExtensionFactory<?> mockedIndexProviderFactory =
            singleInstanceIndexProviderFactory("none", mockedIndexProvider);
    private final Label myLabel = Label.label("MYLABEL");

    private DatabaseManagementService managementService;

    @Test
    void addingANodeWithPropertyShouldGetIndexed() throws Exception {
        // Given
        String indexProperty = "indexProperty";
        GatheringIndexWriter writer = newWriter();
        createIndex(db, myLabel, indexProperty);

        // When
        int value1 = 12;
        String otherProperty = "otherProperty";
        int otherValue = 17;
        Node node = createNode(map(indexProperty, value1, otherProperty, otherValue), myLabel);

        // Then, for now, this should trigger two NodePropertyUpdates
        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            TokenRead tokenRead = ktx.tokenRead();
            int propertyKey1 = tokenRead.propertyKey(indexProperty);
            int label = tokenRead.nodeLabel(myLabel.name());
            var descriptor = SchemaDescriptors.forLabel(label, propertyKey1);
            assertThat(writer.updatesCommitted)
                    .isEqualTo(asSet(IndexEntryUpdate.add(node.getId(), () -> descriptor, Values.of(value1))));
            tx.commit();
        }
        // We get two updates because we both add a label and a property to be indexed
        // in the same transaction, in the future, we should optimize this down to
        // one NodePropertyUpdate.
    }

    @Test
    void addingALabelToPreExistingNodeShouldGetIndexed() throws Exception {
        // GIVEN
        String indexProperty = "indexProperty";
        GatheringIndexWriter writer = newWriter();
        createIndex(db, myLabel, indexProperty);

        // WHEN
        String otherProperty = "otherProperty";
        int value = 12;
        int otherValue = 17;
        Node node = createNode(map(indexProperty, value, otherProperty, otherValue));

        // THEN
        assertThat(writer.updatesCommitted.size()).isEqualTo(0);

        // AND WHEN
        try (Transaction tx = db.beginTx()) {
            node = tx.getNodeById(node.getId());
            node.addLabel(myLabel);
            tx.commit();
        }

        // THEN
        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            TokenRead tokenRead = ktx.tokenRead();
            int propertyKey1 = tokenRead.propertyKey(indexProperty);
            int label = tokenRead.nodeLabel(myLabel.name());
            var descriptor = SchemaDescriptors.forLabel(label, propertyKey1);
            assertThat(writer.updatesCommitted)
                    .isEqualTo(asSet(IndexEntryUpdate.add(node.getId(), () -> descriptor, Values.of(value))));
            tx.commit();
        }
    }

    private Node createNode(Map<String, Object> properties, Label... labels) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode(labels);
            for (Map.Entry<String, Object> prop : properties.entrySet()) {
                node.setProperty(prop.getKey(), prop.getValue());
            }
            tx.commit();
            return node;
        }
    }

    @BeforeEach
    void before() {
        when(mockedIndexProvider.getProviderDescriptor()).thenReturn(PROVIDER_DESCRIPTOR);
        when(mockedIndexProvider.getMinimumRequiredVersion()).thenReturn(KernelVersion.EARLIEST);
        when(mockedIndexProvider.storeMigrationParticipant(
                        any(FileSystemAbstraction.class), any(PageCache.class), any(), any(), any()))
                .thenReturn(StoreMigrationParticipant.NOT_PARTICIPATING);
        when(mockedIndexProvider.completeConfiguration(any(IndexDescriptor.class), any()))
                .then(inv -> inv.getArgument(0));
        when(mockedIndexProvider.getIndexType()).thenReturn(LOOKUP);

        managementService = new TestDatabaseManagementServiceBuilder()
                .setFileSystem(fs)
                .addExtension(mockedIndexProviderFactory)
                .noOpSystemGraphInitializer()
                .impermanent()
                .build();

        db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
    }

    private GatheringIndexWriter newWriter() throws IOException {
        GatheringIndexWriter writer = new GatheringIndexWriter();
        when(mockedIndexProvider.getPopulator(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(),
                        any(),
                        any(TokenNameLookup.class),
                        any(),
                        any()))
                .thenReturn(writer);
        when(mockedIndexProvider.getOnlineAccessor(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(TokenNameLookup.class),
                        any(),
                        any()))
                .thenReturn(writer);
        return writer;
    }

    @AfterEach
    void after() {
        managementService.shutdown();
    }

    private static class GatheringIndexWriter extends IndexAccessor.Adapter implements IndexPopulator {
        private final Set<IndexEntryUpdate<?>> updatesCommitted = new HashSet<>();
        private final Map<Object, Set<Long>> indexSamples = new HashMap<>();

        @Override
        public void create() {}

        @Override
        public void add(Collection<? extends IndexEntryUpdate<?>> updates, CursorContext cursorContext) {
            updatesCommitted.addAll(updates);
        }

        @Override
        public IndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
            return newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false);
        }

        @Override
        public IndexUpdater newUpdater(final IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
            return new CollectingIndexUpdater(updatesCommitted::addAll);
        }

        @Override
        public void close(boolean populationCompletedSuccessfully, CursorContext cursorContext) {}

        @Override
        public void markAsFailed(String failure) {}

        @Override
        public void includeSample(IndexEntryUpdate<?> update) {
            addValueToSample(update.getEntityId(), ((ValueIndexEntryUpdate<?>) update).values()[0]);
        }

        @Override
        public IndexSample sample(CursorContext cursorContext) {
            long indexSize = 0;
            for (Set<Long> nodeIds : indexSamples.values()) {
                indexSize += nodeIds.size();
            }
            return new IndexSample(indexSize, indexSamples.size(), indexSize);
        }

        private void addValueToSample(long nodeId, Object propertyValue) {
            Set<Long> nodeIds = indexSamples.computeIfAbsent(propertyValue, k -> new HashSet<>());
            nodeIds.add(nodeId);
        }
    }

    private static void createIndex(GraphDatabaseAPI db, Label label, String property) throws KernelException {
        try (TransactionImpl tx = (TransactionImpl) db.beginTx()) {
            IndexingTestUtil.createNodePropIndexWithSpecifiedProvider(tx, PROVIDER_DESCRIPTOR, label, property);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, MINUTES);
        }
    }
}
