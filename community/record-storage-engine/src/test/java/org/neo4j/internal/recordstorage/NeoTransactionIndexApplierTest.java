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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.common.Subject.SYSTEM;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;

class NeoTransactionIndexApplierTest {
    private final IndexUpdateListener indexingService = mock(IndexUpdateListener.class);
    private final List<DynamicRecord> emptyDynamicRecords = Collections.emptyList();
    private final StorageEngineTransaction transactionToApply = new GroupOfCommands(1L, StoreCursors.NULL);
    private final BatchContext batchContext = mock(BatchContext.class, RETURNS_MOCKS);
    private static final LogCommandSerialization LATEST_LOG_SERIALIZATION =
            RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION);

    @Test
    void shouldUpdateLabelStoreScanOnNodeCommands() throws Exception {
        // given
        IndexTransactionApplierFactory applier = newIndexTransactionApplier();
        NodeRecord before = new NodeRecord(11);
        before.setLabelField(17, emptyDynamicRecords);
        NodeRecord after = new NodeRecord(12);
        after.setLabelField(18, emptyDynamicRecords);
        Command.NodeCommand command = new Command.NodeCommand(LATEST_LOG_SERIALIZATION, before, after);

        // when
        boolean result;
        try (TransactionApplier txApplier = applier.startTx(transactionToApply, batchContext)) {
            result = txApplier.visitNodeCommand(command);
        }
        // then
        assertFalse(result);
    }

    private IndexTransactionApplierFactory newIndexTransactionApplier() {
        return new IndexTransactionApplierFactory(TransactionApplicationMode.INTERNAL, indexingService);
    }

    @Test
    void shouldCreateIndexGivenCreateSchemaRuleCommand() throws Exception {
        // Given
        IndexDescriptor indexRule = indexRule(1, 42, 42);

        IndexTransactionApplierFactory applier = newIndexTransactionApplier();

        SchemaRecord before = new SchemaRecord(1);
        SchemaRecord after = new SchemaRecord(before).initialize(true, 39);
        after.setCreated();
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, indexRule);

        // When
        boolean result;
        try (TransactionApplier txApplier = applier.startTx(transactionToApply, batchContext)) {
            result = txApplier.visitSchemaRuleCommand(command);
        }

        // Then
        assertFalse(result);
        verify(indexingService).createIndexes(SYSTEM, indexRule);
    }

    private static IndexDescriptor indexRule(long ruleId, int labelId, int propertyId) {
        return IndexPrototype.forSchema(forLabel(labelId, propertyId))
                .withName("index_" + ruleId)
                .materialise(ruleId);
    }

    @Test
    void shouldDropIndexGivenDropSchemaRuleCommand() throws Exception {
        // Given
        IndexDescriptor indexRule = indexRule(1, 42, 42);

        IndexTransactionApplierFactory applier = newIndexTransactionApplier();

        SchemaRecord before = new SchemaRecord(1).initialize(true, 39);
        SchemaRecord after = new SchemaRecord(1);
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, indexRule);

        // When
        boolean result;
        try (TransactionApplier txApplier = applier.startTx(transactionToApply, batchContext)) {
            result = txApplier.visitSchemaRuleCommand(command);
        }

        // Then
        assertFalse(result);
        verify(indexingService).dropIndex(indexRule);
    }
}
