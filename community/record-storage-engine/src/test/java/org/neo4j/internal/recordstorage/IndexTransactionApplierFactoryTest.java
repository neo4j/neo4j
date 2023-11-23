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
package org.neo4j.internal.recordstorage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;

import org.junit.jupiter.api.Test;
import org.neo4j.common.Subject;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.lock.LockGroup;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.storageengine.util.IndexUpdatesWorkSync;
import org.neo4j.test.LatestVersions;

class IndexTransactionApplierFactoryTest {
    private static final LogCommandSerialization LATEST_LOG_SERIALIZATION =
            RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION);

    @Test
    void shouldProvideTokenIndexUpdatesSortedByNodeId() throws Exception {
        // GIVEN
        OrderVerifyingUpdateListener indexUpdateListener = new OrderVerifyingUpdateListener(10, 15, 20);
        IndexUpdatesWorkSync indexUpdatesSync = new IndexUpdatesWorkSync(indexUpdateListener, false);
        PropertyStore propertyStore = mock(PropertyStore.class);
        IndexTransactionApplierFactory applier = new IndexTransactionApplierFactory(INTERNAL, indexUpdateListener);
        final SchemaCache mock = mock(SchemaCache.class);
        IndexDescriptor nli = IndexPrototype.forSchema(
                        SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR,
                        new IndexProviderDescriptor("token-lookup", "1.0"))
                .withName("NLI")
                .withIndexType(IndexType.LOOKUP)
                .materialise(1);
        when(mock.indexForSchemaAndType(any(), any())).thenReturn(nli);
        try (var batchContext = new BatchContextImpl(
                indexUpdateListener,
                indexUpdatesSync,
                mock(NodeStore.class),
                propertyStore,
                mock(RecordStorageEngine.class),
                mock,
                NULL_CONTEXT,
                INSTANCE,
                mock(IdUpdateListener.class),
                StoreCursors.NULL)) {
            try (TransactionApplier txApplier = applier.startTx(new GroupOfCommands(StoreCursors.NULL), batchContext)) {
                // WHEN
                txApplier.visitNodeCommand(node(15));
                txApplier.visitNodeCommand(node(20));
                txApplier.visitNodeCommand(node(10));
            }
        }
        indexUpdateListener.done();
        // THEN all assertions happen inside the UpdateListener and #close
    }

    @Test
    void shouldRegisterIndexesToActivateIntoTheActivator() throws Exception {
        // given
        IndexUpdateListener indexUpdateListener = mock(IndexUpdateListener.class);
        IndexActivator indexActivator = new IndexActivator(indexUpdateListener);
        long indexId1 = 1;
        long indexId2 = 2;
        long indexId3 = 3;
        long constraintId1 = 10;
        long constraintId2 = 11;
        long constraintId3 = 12;
        String providerKey = "index-key";
        String providerVersion = "v1";
        IndexDescriptor rule1 = uniqueForSchema(forLabel(1, 1), providerKey, providerVersion, indexId1, constraintId1);
        IndexDescriptor rule2 = uniqueForSchema(forLabel(2, 1), providerKey, providerVersion, indexId2, constraintId2);
        IndexDescriptor rule3 = uniqueForSchema(forLabel(3, 1), providerKey, providerVersion, indexId3, constraintId3);
        IndexTransactionApplierFactory applier = new IndexTransactionApplierFactory(INTERNAL, indexUpdateListener);
        var batchContext = mock(BatchContext.class);
        when(batchContext.getLockGroup()).thenReturn(new LockGroup());
        when(batchContext.indexUpdates()).thenReturn(mock(IndexUpdates.class));
        when(batchContext.getIndexActivator()).thenReturn(indexActivator);
        try (var txApplier = applier.startTx(new GroupOfCommands(StoreCursors.NULL), batchContext)) {
            // activate index 1
            txApplier.visitSchemaRuleCommand(new Command.SchemaRuleCommand(
                    LATEST_LOG_SERIALIZATION, new SchemaRecord(rule1.getId()), asSchemaRecord(rule1, true), rule1));

            // activate index 2
            txApplier.visitSchemaRuleCommand(new Command.SchemaRuleCommand(
                    LATEST_LOG_SERIALIZATION, new SchemaRecord(rule2.getId()), asSchemaRecord(rule2, true), rule2));

            // activate index 3
            txApplier.visitSchemaRuleCommand(new Command.SchemaRuleCommand(
                    LATEST_LOG_SERIALIZATION, new SchemaRecord(rule3.getId()), asSchemaRecord(rule3, true), rule3));

            // drop index 2
            txApplier.visitSchemaRuleCommand(new Command.SchemaRuleCommand(
                    LATEST_LOG_SERIALIZATION, asSchemaRecord(rule2, true), asSchemaRecord(rule2, false), rule2));
        }

        verify(indexUpdateListener).dropIndex(rule2);
        indexActivator.close();
        verify(indexUpdateListener).activateIndex(rule1);
        verify(indexUpdateListener).activateIndex(rule3);
        verifyNoMoreInteractions(indexUpdateListener);
    }

    private static IndexDescriptor uniqueForSchema(
            SchemaDescriptor schema, String providerKey, String providerVersion, long id, long owningConstraint) {
        final IndexProviderDescriptor indexProvider = new IndexProviderDescriptor(providerKey, providerVersion);
        return IndexPrototype.uniqueForSchema(schema, indexProvider)
                .withName("constraint_" + id)
                .materialise(id)
                .withOwningConstraintId(owningConstraint);
    }

    private static SchemaRecord asSchemaRecord(SchemaRule rule, boolean inUse) {
        // Only used to transfer
        return new SchemaRecord(rule.getId()).initialize(inUse, NO_NEXT_PROPERTY.longValue());
    }

    private static NodeCommand node(long nodeId) {
        NodeRecord after = new NodeRecord(nodeId)
                .initialize(true, NO_NEXT_PROPERTY.intValue(), false, NO_NEXT_RELATIONSHIP.intValue(), 0);
        NodeLabelsField.parseLabelsField(after).add(1, null, null, NULL_CONTEXT, StoreCursors.NULL, INSTANCE);

        return new NodeCommand(LATEST_LOG_SERIALIZATION, new NodeRecord(nodeId), after);
    }

    private static class OrderVerifyingUpdateListener implements IndexUpdateListener {
        private final long[] expectedNodeIds;
        private int cursor;

        OrderVerifyingUpdateListener(long... expectedNodeIds) {
            this.expectedNodeIds = expectedNodeIds;
        }

        @Override
        public void createIndexes(Subject subject, IndexDescriptor... indexes) {}

        @Override
        public void activateIndex(IndexDescriptor index) {}

        @Override
        public void dropIndex(IndexDescriptor index) {}

        @Override
        public void applyUpdates(
                Iterable<IndexEntryUpdate<IndexDescriptor>> updates, CursorContext cursorContext, boolean parallel) {
            for (IndexEntryUpdate<IndexDescriptor> update : updates) {
                assertEquals(expectedNodeIds[cursor], update.getEntityId());
                cursor++;
            }
        }

        void done() {
            assertEquals(cursor, expectedNodeIds.length);
        }
    }
}
