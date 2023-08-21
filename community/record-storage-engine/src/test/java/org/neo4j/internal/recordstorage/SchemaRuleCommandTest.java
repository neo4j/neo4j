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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.common.Subject.SYSTEM;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;

import java.util.function.Predicate;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.recordstorage.Command.SchemaRuleCommand;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptorPredicates;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.lock.LockService;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdGeneratorUpdatesWorkSync;
import org.neo4j.test.LatestVersions;

class SchemaRuleCommandTest {
    private final int labelId = 2;
    private final int propertyKey = 8;
    private final long id = 0;
    private final long txId = 1337L;
    private final NeoStores neoStores = mock(NeoStores.class);
    private final MetaDataStore metaDataStore = mock(MetaDataStore.class);
    private final SchemaStore schemaStore = mock(SchemaStore.class);
    private final IndexUpdateListener indexUpdateListener = mock(IndexUpdateListener.class);
    private LockGuardedNeoStoreTransactionApplierFactory storeApplier;
    private final IndexTransactionApplierFactory indexApplier =
            new IndexTransactionApplierFactory(INTERNAL, indexUpdateListener);
    private final LogCommandSerialization serialization =
            RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION);
    private final IndexDescriptor rule = IndexPrototype.forSchema(SchemaDescriptors.forLabel(labelId, propertyKey))
            .withName("index")
            .materialise(id);

    @BeforeEach
    void setup() {
        IdGeneratorUpdatesWorkSync idGeneratorWorkSyncs = new IdGeneratorUpdatesWorkSync();
        Stream.of(RecordIdType.values()).forEach(idType -> idGeneratorWorkSyncs.add(mock(IdGenerator.class)));
        storeApplier = new LockGuardedNeoStoreTransactionApplierFactory(
                INTERNAL, neoStores, mock(CacheAccessBackDoor.class), LockService.NO_LOCK_SERVICE);
    }

    @Test
    void shouldWriteCreatedSchemaRuleToStore() throws Exception {
        // GIVEN
        SchemaRecord before = new SchemaRecord(id).initialize(false, NO_NEXT_PROPERTY.longValue());
        SchemaRecord after = new SchemaRecord(id).initialize(true, 42);

        when(neoStores.getSchemaStore()).thenReturn(schemaStore);

        // WHEN
        visitSchemaRuleCommand(storeApplier, new SchemaRuleCommand(serialization, before, after, rule));

        // THEN
        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
    }

    @Test
    void shouldCreateIndexForCreatedSchemaRule() throws Exception {
        // GIVEN
        SchemaRecord before = new SchemaRecord(id).initialize(false, NO_NEXT_PROPERTY.longValue());
        SchemaRecord after = new SchemaRecord(id).initialize(true, 42);
        after.setCreated();

        when(neoStores.getSchemaStore()).thenReturn(schemaStore);

        // WHEN
        visitSchemaRuleCommand(indexApplier, new SchemaRuleCommand(serialization, before, after, rule));

        // THEN
        verify(indexUpdateListener).createIndexes(SYSTEM, rule);
    }

    @Test
    void shouldSetLatestConstraintRule() throws Exception {
        // Given
        SchemaRecord before = new SchemaRecord(id).initialize(true, 42);
        before.setCreated();
        SchemaRecord after = new SchemaRecord(id).initialize(true, 42);
        after.setConstraint(true);

        when(neoStores.getSchemaStore()).thenReturn(schemaStore);

        ConstraintDescriptor schemaRule = ConstraintDescriptorFactory.uniqueForLabel(labelId, propertyKey)
                .withId(id)
                .withOwnedIndexId(0);

        // WHEN
        visitSchemaRuleCommand(storeApplier, new SchemaRuleCommand(serialization, before, after, schemaRule));

        // THEN
        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
    }

    @Test
    void shouldDropSchemaRuleFromStore() throws Exception {
        // GIVEN
        SchemaRecord before = new SchemaRecord(id).initialize(true, 42);
        before.setCreated();
        SchemaRecord after = new SchemaRecord(id).initialize(false, NO_NEXT_PROPERTY.longValue());

        when(neoStores.getSchemaStore()).thenReturn(schemaStore);

        // WHEN
        visitSchemaRuleCommand(storeApplier, new SchemaRuleCommand(serialization, before, after, rule));

        // THEN
        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
    }

    @Test
    void shouldDropSchemaRuleFromIndex() throws Exception {
        // GIVEN
        SchemaRecord before = new SchemaRecord(id).initialize(true, 42);
        before.setCreated();
        SchemaRecord after = new SchemaRecord(id).initialize(false, NO_NEXT_PROPERTY.longValue());

        when(neoStores.getSchemaStore()).thenReturn(schemaStore);

        // WHEN
        visitSchemaRuleCommand(indexApplier, new SchemaRuleCommand(serialization, before, after, rule));

        // THEN
        verify(indexUpdateListener).dropIndex(rule);
    }

    @Test
    void shouldWriteSchemaRuleToLog() throws Exception {
        // GIVEN
        SchemaRecord before = new SchemaRecord(id).initialize(false, NO_NEXT_PROPERTY.longValue());
        SchemaRecord after = new SchemaRecord(id).initialize(true, 42);
        after.setCreated();

        SchemaRuleCommand command = new SchemaRuleCommand(serialization, before, after, rule);
        InMemoryClosableChannel buffer = new InMemoryClosableChannel();

        when(neoStores.getSchemaStore()).thenReturn(schemaStore);

        // WHEN
        command.serialize(buffer);
        StorageCommand readCommand = serialization.read(buffer);

        // THEN
        assertThat(readCommand).isInstanceOf(SchemaRuleCommand.class);

        assertSchemaRule((SchemaRuleCommand) readCommand);
    }

    @Test
    void shouldRecreateSchemaRuleWhenDeleteCommandReadFromDisk() throws Exception {
        // GIVEN
        SchemaRecord before = new SchemaRecord(id).initialize(true, 42);
        before.setCreated();
        SchemaRecord after = new SchemaRecord(id).initialize(false, NO_NEXT_PROPERTY.longValue());

        SchemaRuleCommand command = new SchemaRuleCommand(serialization, before, after, rule);
        InMemoryClosableChannel buffer = new InMemoryClosableChannel();
        when(neoStores.getSchemaStore()).thenReturn(schemaStore);

        // WHEN
        command.serialize(buffer);
        StorageCommand readCommand = serialization.read(buffer);

        // THEN
        assertThat(readCommand).isInstanceOf(SchemaRuleCommand.class);

        assertSchemaRule((SchemaRuleCommand) readCommand);
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @RepeatedTest(1000)
    void writeAndReadOfArbitrarySchemaRules() throws Exception {
        RandomSchema randomSchema = new RandomSchema();
        SchemaRule rule = randomSchema
                .schemaRules()
                .filter(indexBackedConstraintsWithoutIndexes())
                .findFirst()
                .get();
        long ruleId = rule.getId();

        SchemaRecord before = new SchemaRecord(ruleId).initialize(false, NO_NEXT_PROPERTY.longValue());
        SchemaRecord after = new SchemaRecord(ruleId).initialize(true, 42);
        after.setCreated();

        SchemaRuleCommand command = new SchemaRuleCommand(serialization, before, after, rule);
        InMemoryClosableChannel buffer = new InMemoryClosableChannel((int) ByteUnit.kibiBytes(5));
        when(neoStores.getSchemaStore()).thenReturn(schemaStore);

        // WHEN
        command.serialize(buffer);
        SchemaRuleCommand readCommand = (SchemaRuleCommand) serialization.read(buffer);

        // THEN
        assertEquals(ruleId, readCommand.getKey());
        assertThat(readCommand.getSchemaRule()).isEqualTo(rule);
    }

    /**
     * When we get to committing a schema rule command that writes a constraint rule, it is illegal for an index-backed constraint rule to not have a reference
     * to an index that it owns. However, the {@link RandomSchema} might generate such {@link ConstraintDescriptor ConstraintDescriptors},
     * so we have to filter them out.
     */
    private static Predicate<? super SchemaRule> indexBackedConstraintsWithoutIndexes() {
        return r -> {
            if (r instanceof ConstraintDescriptor constraint) {
                return constraint.isIndexBackedConstraint()
                        && constraint.asIndexBackedConstraint().hasOwnedIndexId();
            }
            return true;
        };
    }

    private void assertSchemaRule(SchemaRuleCommand readSchemaCommand) {
        assertEquals(id, readSchemaCommand.getKey());
        assertTrue(SchemaDescriptorPredicates.hasLabel(readSchemaCommand.getSchemaRule(), labelId));
        assertTrue(SchemaDescriptorPredicates.hasProperty(readSchemaCommand.getSchemaRule(), propertyKey));
    }

    private void visitSchemaRuleCommand(TransactionApplierFactory applier, SchemaRuleCommand command) throws Exception {
        CommandHandlerContract.apply(applier, new GroupOfCommands(txId, StoreCursors.NULL, command));
    }
}
