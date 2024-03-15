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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.common.Subject.AUTH_DISABLED;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.lock.LockType.EXCLUSIVE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.RECOVERY;

import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.SchemaIdType;
import org.neo4j.internal.recordstorage.Command.LabelTokenCommand;
import org.neo4j.internal.recordstorage.Command.PropertyKeyTokenCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipTypeTokenCommand;
import org.neo4j.internal.recordstorage.CommandHandlerContract.ApplyFunction;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.lock.LockService;
import org.neo4j.storageengine.api.CommandBatchToApply;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdGeneratorUpdatesWorkSync;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.storageengine.util.IndexUpdatesWorkSync;
import org.neo4j.test.LatestVersions;
import org.neo4j.token.api.NamedToken;

class LockGuardedNeoStoreTransactionApplierTest {
    private static final LogCommandSerialization LATEST_LOG_SERIALIZATION =
            RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION);

    private final NeoStores neoStores = mock(NeoStores.class);
    private final IndexUpdateListener indexingService = mock(IndexUpdateListener.class);
    private final IndexUpdateListener indexUpdateListener = mock(IndexUpdateListener.class);
    private final CacheAccessBackDoor cacheAccess = mock(CacheAccessBackDoor.class);
    private final LockService lockService = mock(LockService.class);

    private final MetaDataStore metaDataStore = mockedStore(MetaDataStore.class, null);
    private final NodeStore nodeStore = mockedStore(NodeStore.class, RecordIdType.NODE);
    private final RelationshipStore relationshipStore = mockedStore(RelationshipStore.class, RecordIdType.RELATIONSHIP);
    private final PropertyStore propertyStore = mockedStore(PropertyStore.class, RecordIdType.PROPERTY);
    private final RelationshipGroupStore relationshipGroupStore =
            mockedStore(RelationshipGroupStore.class, RecordIdType.RELATIONSHIP_GROUP);
    private final RelationshipTypeTokenStore relationshipTypeTokenStore =
            mockedStore(RelationshipTypeTokenStore.class, SchemaIdType.RELATIONSHIP_TYPE_TOKEN);
    private final LabelTokenStore labelTokenStore = mockedStore(LabelTokenStore.class, SchemaIdType.LABEL_TOKEN);
    private final PropertyKeyTokenStore propertyKeyTokenStore =
            mockedStore(PropertyKeyTokenStore.class, SchemaIdType.PROPERTY_KEY_TOKEN);
    private final SchemaStore schemaStore = mockedStore(SchemaStore.class, SchemaIdType.SCHEMA);
    private final DynamicArrayStore dynamicLabelStore = mockedStore(DynamicArrayStore.class, RecordIdType.ARRAY_BLOCK);
    private final DynamicStringStore dynamicStringStore =
            mockedStore(DynamicStringStore.class, RecordIdType.STRING_BLOCK);

    private final long transactionId = 55555;
    private final DynamicRecord one = new DynamicRecord(1).initialize(true, true, Record.NO_NEXT_BLOCK.intValue(), -1);
    private final DynamicRecord two = new DynamicRecord(2).initialize(true, true, Record.NO_NEXT_BLOCK.intValue(), -1);
    private final DynamicRecord three =
            new DynamicRecord(3).initialize(true, true, Record.NO_NEXT_BLOCK.intValue(), -1);
    private final CommandBatchToApply transactionToApply = mock(CommandBatchToApply.class);
    private final IndexUpdatesWorkSync indexUpdatesSync = new IndexUpdatesWorkSync(indexUpdateListener, false);
    private final IndexActivator indexActivator = new IndexActivator(indexingService);

    @BeforeEach
    void setup() {
        when(neoStores.getMetaDataStore()).thenReturn(metaDataStore);
        when(neoStores.getNodeStore()).thenReturn(nodeStore);
        when(neoStores.getRelationshipStore()).thenReturn(relationshipStore);
        when(neoStores.getPropertyStore()).thenReturn(propertyStore);
        when(neoStores.getRelationshipGroupStore()).thenReturn(relationshipGroupStore);
        when(neoStores.getRelationshipTypeTokenStore()).thenReturn(relationshipTypeTokenStore);
        when(neoStores.getLabelTokenStore()).thenReturn(labelTokenStore);
        when(neoStores.getPropertyKeyTokenStore()).thenReturn(propertyKeyTokenStore);
        when(neoStores.getSchemaStore()).thenReturn(schemaStore);
        when(nodeStore.getDynamicLabelStore()).thenReturn(dynamicLabelStore);
        when(lockService.acquireNodeLock(anyLong(), any())).thenReturn(LockService.NO_LOCK);
        when(lockService.acquireRelationshipLock(anyLong(), any())).thenReturn(LockService.NO_LOCK);
        when(transactionToApply.transactionId()).thenReturn(transactionId);
        when(transactionToApply.cursorContext()).thenReturn(NULL_CONTEXT);
        when(transactionToApply.storeCursors()).thenReturn(StoreCursors.NULL);
        when(transactionToApply.subject()).thenReturn(AUTH_DISABLED);
        when(labelTokenStore.getNameStore()).thenReturn(dynamicStringStore);
        when(propertyKeyTokenStore.getNameStore()).thenReturn(dynamicStringStore);
        when(relationshipTypeTokenStore.getNameStore()).thenReturn(dynamicStringStore);
    }

    private static <T extends CommonAbstractStore> T mockedStore(Class<T> cls, IdType idType) {
        T store = mock(cls);
        when(store.getIdGenerator()).thenReturn(mock(IdGenerator.class));
        when(store.getIdType()).thenReturn(idType);
        return store;
    }

    // NODE COMMAND

    @Test
    void shouldApplyNodeCommandToTheStore() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(false);
        NodeRecord before = new NodeRecord(11);
        before.setLabelField(42, asList(one, two));
        NodeRecord after = new NodeRecord(12);
        after.setInUse(true);
        after.setLabelField(42, asList(one, two, three));
        Command.NodeCommand command = new Command.NodeCommand(LATEST_LOG_SERIALIZATION, before, after);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(lockService).acquireNodeLock(command.getKey(), EXCLUSIVE);
        verify(nodeStore).updateRecord(eq(after), any(), any(), any(), any());
    }

    @Test
    void shouldApplyNodeCommandToTheStoreAndInvalidateTheCache() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(false);
        NodeRecord before = new NodeRecord(11);
        before.setLabelField(42, asList(one, two));
        NodeRecord after = new NodeRecord(12);
        after.setInUse(false);
        after.setLabelField(42, asList(one, two, three));
        Command.NodeCommand command = new Command.NodeCommand(LATEST_LOG_SERIALIZATION, before, after);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(lockService).acquireNodeLock(command.getKey(), EXCLUSIVE);
        verify(nodeStore).updateRecord(eq(after), any(), any(), any(), any());
    }

    @Test
    void shouldApplyNodeCommandToTheStoreInRecoveryMode() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(true);
        NodeRecord before = new NodeRecord(11);
        before.setLabelField(42, asList(one, two));
        NodeRecord after = new NodeRecord(12);
        after.setInUse(true);
        after.setLabelField(42, asList(one, two, three));
        Command.NodeCommand command = new Command.NodeCommand(LATEST_LOG_SERIALIZATION, before, after);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(lockService).acquireNodeLock(command.getKey(), EXCLUSIVE);
        verify(nodeStore.getIdGenerator()).setHighestPossibleIdInUse(after.getId());
        verify(nodeStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(dynamicLabelStore.getIdGenerator()).setHighestPossibleIdInUse(three.getId());
    }

    @Test
    void shouldInvalidateTheCacheWhenTheNodeBecomesDense() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(false);
        NodeRecord before = new NodeRecord(11);
        before.setLabelField(42, singletonList(one));
        before.setInUse(true);
        before.setDense(false);
        NodeRecord after = new NodeRecord(12);
        after.setInUse(true);
        after.setDense(true);
        after.setLabelField(42, asList(one, two, three));
        Command.NodeCommand command = new Command.NodeCommand(LATEST_LOG_SERIALIZATION, before, after);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(lockService).acquireNodeLock(command.getKey(), EXCLUSIVE);
        verify(nodeStore).updateRecord(eq(after), any(), any(), any(), any());
    }

    // RELATIONSHIP COMMAND

    @Test
    void shouldApplyRelationshipCommandToTheStore() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(false);
        RelationshipRecord before = new RelationshipRecord(12);
        RelationshipRecord record = new RelationshipRecord(12);
        record.setLinks(3, 4, 5);
        record.setInUse(true);

        Command command = new Command.RelationshipCommand(LATEST_LOG_SERIALIZATION, before, record);
        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(relationshipStore).updateRecord(eq(record), any(), any(), any(), any());
    }

    @Test
    void shouldApplyRelationshipCommandToTheStoreAndInvalidateTheCache() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(false);
        RelationshipRecord before = new RelationshipRecord(12);
        RelationshipRecord record = new RelationshipRecord(12);
        record.setLinks(3, 4, 5);
        record.setInUse(false);

        Command command = new Command.RelationshipCommand(LATEST_LOG_SERIALIZATION, before, record);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(relationshipStore).updateRecord(eq(record), any(), any(), any(), any());
    }

    @Test
    void shouldApplyRelationshipCommandToTheStoreInRecovery() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(true);
        RelationshipRecord before = new RelationshipRecord(12);
        RelationshipRecord record = new RelationshipRecord(12);
        record.setLinks(3, 4, 5);
        record.setInUse(true);
        Command command = new Command.RelationshipCommand(LATEST_LOG_SERIALIZATION, before, record);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(relationshipStore.getIdGenerator()).setHighestPossibleIdInUse(record.getId());
        verify(relationshipStore).updateRecord(eq(record), any(), any(), any(), any());
    }

    // PROPERTY COMMAND

    @Test
    void shouldApplyNodePropertyCommandToTheStore() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(false);
        PropertyRecord before = new PropertyRecord(11);
        PropertyRecord after = new PropertyRecord(12);
        after.setNodeId(42);
        Command command = new Command.PropertyCommand(LATEST_LOG_SERIALIZATION, before, after);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(lockService).acquireNodeLock(42, EXCLUSIVE);
        verify(propertyStore).updateRecord(eq(after), any(), any(), any(), any());
    }

    @Test
    void shouldApplyNodePropertyCommandToTheStoreInRecovery() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(true);
        PropertyRecord before = new PropertyRecord(11);
        PropertyRecord after = new PropertyRecord(12);
        after.setNodeId(42);
        Command command = new Command.PropertyCommand(LATEST_LOG_SERIALIZATION, before, after);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(lockService).acquireNodeLock(42, EXCLUSIVE);
        verify(propertyStore.getIdGenerator()).setHighestPossibleIdInUse(after.getId());
        verify(propertyStore).updateRecord(eq(after), any(), any(), any(), any());
    }

    @Test
    void shouldApplyRelPropertyCommandToTheStore() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(false);
        PropertyRecord before = new PropertyRecord(11);
        PropertyRecord after = new PropertyRecord(12);
        after.setRelId(42);
        Command command = new Command.PropertyCommand(LATEST_LOG_SERIALIZATION, before, after);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(propertyStore).updateRecord(eq(after), any(), any(), any(), any());
    }

    @Test
    void shouldApplyRelPropertyCommandToTheStoreInRecovery() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(true);
        PropertyRecord before = new PropertyRecord(11);
        PropertyRecord after = new PropertyRecord(12);
        after.setRelId(42);
        Command command = new Command.PropertyCommand(LATEST_LOG_SERIALIZATION, before, after);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(propertyStore.getIdGenerator()).setHighestPossibleIdInUse(12);
        verify(propertyStore).updateRecord(eq(after), any(), any(), any(), any());
    }

    // RELATIONSHIP GROUP COMMAND

    @Test
    void shouldApplyRelationshipGroupCommandToTheStore() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(false);
        // when
        RelationshipGroupRecord before = new RelationshipGroupRecord(42)
                .initialize(
                        false,
                        1,
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue());
        RelationshipGroupRecord after = new RelationshipGroupRecord(42).initialize(true, 1, 2, 3, 4, 5, 6);
        Command command = new Command.RelationshipGroupCommand(LATEST_LOG_SERIALIZATION, before, after);
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(relationshipGroupStore).updateRecord(eq(after), any(), any(), any(), any());
    }

    @Test
    void shouldApplyRelationshipGroupCommandToTheStoreInRecovery() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(true);
        // when
        RelationshipGroupRecord before = new RelationshipGroupRecord(42)
                .initialize(
                        false,
                        1,
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue());
        RelationshipGroupRecord after = new RelationshipGroupRecord(42).initialize(true, 1, 2, 3, 4, 5, 6);
        Command command = new Command.RelationshipGroupCommand(LATEST_LOG_SERIALIZATION, before, after);

        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(relationshipGroupStore.getIdGenerator()).setHighestPossibleIdInUse(after.getId());
        verify(relationshipGroupStore).updateRecord(eq(after), any(), any(), any(), any());
    }

    // RELATIONSHIP TYPE TOKEN COMMAND

    @Test
    void shouldApplyRelationshipTypeTokenCommandToTheStore() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(false);
        RelationshipTypeTokenRecord before = new RelationshipTypeTokenRecord(42);
        RelationshipTypeTokenRecord after = new RelationshipTypeTokenRecord(42);
        after.setInUse(true);
        after.setNameId(323);
        DynamicRecord dynamicRecord = new DynamicRecord(323);
        dynamicRecord.initialize(true, true, -1, -1);
        after.addNameRecord(dynamicRecord);
        Command command = new RelationshipTypeTokenCommand(LATEST_LOG_SERIALIZATION, before, after);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(relationshipTypeTokenStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(cacheAccess).addRelationshipTypeToken(any(), eq(true));
    }

    @Test
    void shouldApplyRelationshipTypeTokenCommandToTheStoreInRecovery() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(true);

        RelationshipTypeTokenRecord before = new RelationshipTypeTokenRecord(42);
        RelationshipTypeTokenRecord after = new RelationshipTypeTokenRecord(42);
        after.setInUse(true);
        after.setNameId(323);
        NamedToken token = new NamedToken("token", after.getIntId());
        DynamicRecord dynamicRecord = new DynamicRecord(323);
        dynamicRecord.initialize(true, true, -1, -1);
        dynamicRecord.setData(token.name().getBytes());
        after.addNameRecord(dynamicRecord);
        Command.RelationshipTypeTokenCommand command =
                new Command.RelationshipTypeTokenCommand(LATEST_LOG_SERIALIZATION, before, after);
        when(relationshipTypeTokenStore.getToken(eq(command.tokenId()), any(StoreCursors.class)))
                .thenReturn(token);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(relationshipTypeTokenStore.getIdGenerator()).setHighestPossibleIdInUse(after.getId());
        verify(relationshipTypeTokenStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(cacheAccess).addRelationshipTypeToken(token, false);
    }

    // LABEL TOKEN COMMAND

    @Test
    void shouldApplyLabelTokenCommandToTheStore() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(false);
        LabelTokenRecord before = new LabelTokenRecord(42);
        LabelTokenRecord after = new LabelTokenRecord(42);
        after.setInUse(true);
        after.setNameId(323);
        DynamicRecord dynamicRecord = new DynamicRecord(323);
        dynamicRecord.initialize(true, true, -1, -1);
        after.addNameRecord(dynamicRecord);
        Command command = new LabelTokenCommand(LATEST_LOG_SERIALIZATION, before, after);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(labelTokenStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(cacheAccess).addLabelToken(any(), eq(true));
    }

    @Test
    void shouldApplyLabelTokenCommandToTheStoreInRecovery() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(true);
        LabelTokenRecord before = new LabelTokenRecord(42);
        LabelTokenRecord after = new LabelTokenRecord(42);
        after.setInUse(true);
        after.setNameId(323);
        NamedToken token = new NamedToken("token", after.getIntId());
        DynamicRecord dynamicRecord = new DynamicRecord(323);
        dynamicRecord.initialize(true, true, -1, -1);
        dynamicRecord.setData(token.name().getBytes());
        after.addNameRecord(dynamicRecord);
        Command.LabelTokenCommand command = new Command.LabelTokenCommand(LATEST_LOG_SERIALIZATION, before, after);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(labelTokenStore.getIdGenerator()).setHighestPossibleIdInUse(after.getId());
        verify(labelTokenStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(cacheAccess).addLabelToken(token, false);
    }

    // PROPERTY KEY TOKEN COMMAND

    @Test
    void shouldApplyPropertyKeyTokenCommandToTheStore() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(false);
        PropertyKeyTokenRecord before = new PropertyKeyTokenRecord(42);
        PropertyKeyTokenRecord after = new PropertyKeyTokenRecord(42);
        after.setInUse(true);
        after.setNameId(323);
        DynamicRecord dynamicRecord = new DynamicRecord(323);
        dynamicRecord.initialize(true, true, -1, -1);
        after.addNameRecord(dynamicRecord);
        Command command = new PropertyKeyTokenCommand(LATEST_LOG_SERIALIZATION, before, after);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(propertyKeyTokenStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(cacheAccess).addPropertyKeyToken(any(), eq(true));
    }

    @Test
    void shouldApplyPropertyKeyTokenCommandToTheStoreInRecovery() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(true);

        PropertyKeyTokenRecord before = new PropertyKeyTokenRecord(42);
        PropertyKeyTokenRecord after = new PropertyKeyTokenRecord(42);
        after.setInUse(true);
        after.setNameId(323);
        NamedToken token = new NamedToken("token", after.getIntId());
        DynamicRecord dynamicRecord = new DynamicRecord(323);
        dynamicRecord.initialize(true, true, -1, -1);
        dynamicRecord.setData(token.name().getBytes());
        after.addNameRecord(dynamicRecord);
        Command.PropertyKeyTokenCommand command =
                new Command.PropertyKeyTokenCommand(LATEST_LOG_SERIALIZATION, before, after);
        when(propertyKeyTokenStore.getToken(eq(command.tokenId()), any(StoreCursors.class)))
                .thenReturn(token);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(propertyKeyTokenStore.getIdGenerator()).setHighestPossibleIdInUse(after.getId());
        verify(propertyKeyTokenStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(cacheAccess).addPropertyKeyToken(token, false);
    }

    @Test
    void shouldApplyCreateIndexRuleSchemaRuleCommandToTheStore() throws Exception {
        // given
        TransactionApplierFactory applier = newApplierFacade(newApplier(false), newIndexApplier());
        IndexDescriptor rule = indexRule(0, 1, 2, "K", "X.Y");
        SchemaRecord before = new SchemaRecord(rule.getId());
        SchemaRecord after = new SchemaRecord(before).initialize(true, Record.NO_NEXT_PROPERTY.longValue());
        after.setCreated();
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, rule);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(indexingService).createIndexes(AUTH_DISABLED, rule);
        verify(cacheAccess).addSchemaRule(rule);
    }

    @Test
    void shouldApplyCreateIndexRuleSchemaRuleCommandToTheStoreInRecovery() throws Exception {
        // given
        TransactionApplierFactory applier = newApplierFacade(newIndexApplier(), newApplier(true));
        SchemaRecord before = new SchemaRecord(21);
        SchemaRecord after = new SchemaRecord(before).initialize(true, Record.NO_NEXT_PROPERTY.longValue());
        after.setCreated();
        IndexDescriptor rule = indexRule(21, 1, 2, "K", "X.Y");
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, rule);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(schemaStore.getIdGenerator()).setHighestPossibleIdInUse(after.getId());
        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(indexingService).createIndexes(AUTH_DISABLED, rule);
        verify(cacheAccess).addSchemaRule(rule);
    }

    @Test
    void shouldApplyUpdateIndexRuleSchemaRuleCommandToTheStore() throws Exception {
        // given
        var batchContext = new BatchContextImpl(
                indexingService,
                indexUpdatesSync,
                nodeStore,
                propertyStore,
                mock(RecordStorageEngine.class),
                mock(SchemaCache.class),
                NULL_CONTEXT,
                INSTANCE,
                IdUpdateListener.IGNORE,
                StoreCursors.NULL);
        TransactionApplierFactory applier = newApplierFacade(newIndexApplier(), newApplier(false));
        SchemaRecord before = new SchemaRecord(21);
        SchemaRecord after = new SchemaRecord(before).initialize(true, Record.NO_NEXT_PROPERTY.longValue());
        after.setConstraint(true);
        IndexDescriptor rule = constraintIndexRule(21, 1, 2, "K", "X.Y", 42L);
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, rule);

        // when
        boolean result = apply(applier, command::handle, batchContext, transactionToApply);

        // then
        assertFalse(result);

        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(indexingService).activateIndex(rule);
        verify(cacheAccess).addSchemaRule(rule);
    }

    @Test
    void shouldApplyUpdateIndexRuleSchemaRuleCommandToTheStoreInRecovery() throws Exception {
        // given
        var batchContext = new BatchContextImpl(
                indexingService,
                indexUpdatesSync,
                nodeStore,
                propertyStore,
                mock(RecordStorageEngine.class),
                mock(SchemaCache.class),
                NULL_CONTEXT,
                INSTANCE,
                IdUpdateListener.IGNORE,
                StoreCursors.NULL);
        TransactionApplierFactory applier = newApplierFacade(newIndexApplier(), newApplier(true));
        SchemaRecord before = new SchemaRecord(21);
        SchemaRecord after = new SchemaRecord(before).initialize(true, Record.NO_NEXT_PROPERTY.longValue());
        IndexDescriptor rule = constraintIndexRule(0, 1, 2, "K", "X.Y", 42L);
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, rule);

        // when
        boolean result = apply(applier, command::handle, batchContext, transactionToApply);

        // then
        assertFalse(result);

        verify(schemaStore.getIdGenerator()).setHighestPossibleIdInUse(after.getId());
        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(indexingService).activateIndex(rule);
        verify(cacheAccess).addSchemaRule(rule);
    }

    @Test
    void shouldCloseReaderAndNasNoUpdatesOnContextClose() throws Exception {
        RecordStorageEngine storageEngine = mock(RecordStorageEngine.class);
        RecordStorageReader storageReader = mock(RecordStorageReader.class);
        when(storageEngine.newReader()).thenReturn(storageReader);
        var batchContext = new BatchContextImpl(
                indexingService,
                indexUpdatesSync,
                nodeStore,
                propertyStore,
                storageEngine,
                mock(SchemaCache.class),
                NULL_CONTEXT,
                INSTANCE,
                IdUpdateListener.IGNORE,
                StoreCursors.NULL);

        IndexUpdates indexEntryUpdates = batchContext.indexUpdates();
        ((OnlineIndexUpdates) indexEntryUpdates).getUpdates().add(IndexEntryUpdate.add(1, IndexDescriptor.NO_INDEX));

        assertTrue(batchContext.hasUpdates());

        batchContext.close();

        assertFalse(batchContext.hasUpdates());
        verify(storageReader).close();
    }

    @Test
    void shouldNasNoUpdatesAfterPendingUpdatesApplied() throws Exception {
        RecordStorageEngine storageEngine = mock(RecordStorageEngine.class);
        RecordStorageReader storageReader = mock(RecordStorageReader.class);
        when(storageEngine.newReader()).thenReturn(storageReader);
        var batchContext = new BatchContextImpl(
                indexingService,
                indexUpdatesSync,
                nodeStore,
                propertyStore,
                storageEngine,
                mock(SchemaCache.class),
                NULL_CONTEXT,
                INSTANCE,
                IdUpdateListener.IGNORE,
                StoreCursors.NULL);

        IndexUpdates indexEntryUpdates = batchContext.indexUpdates();
        ((OnlineIndexUpdates) indexEntryUpdates).getUpdates().add(IndexEntryUpdate.add(1, IndexDescriptor.NO_INDEX));

        assertTrue(batchContext.hasUpdates());

        batchContext.applyPendingIndexUpdates();

        assertFalse(batchContext.hasUpdates());
    }

    @Test
    void shouldApplyUpdateIndexRuleSchemaRuleCommandToTheStoreThrowingIndexProblem() {
        // given
        var batchContext = mock(BatchContext.class);
        var indexActivator = mock(IndexActivator.class);
        when(batchContext.getIndexActivator()).thenReturn(indexActivator);
        TransactionApplierFactory applier = newIndexApplier();
        var runtimeException = new RuntimeException("");
        doThrow(runtimeException).when(indexActivator).activateIndex(any());

        SchemaRecord before = new SchemaRecord(21);
        SchemaRecord after = new SchemaRecord(before).initialize(true, Record.NO_NEXT_PROPERTY.longValue());
        IndexDescriptor rule = constraintIndexRule(0, 1, 2, "K", "X.Y", 42L);
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, rule);

        var e = assertThrows(Exception.class, () -> apply(applier, command::handle, batchContext, transactionToApply));
        assertSame(runtimeException, e);
    }

    @Test
    void shouldApplyDeleteIndexRuleSchemaRuleCommandToTheStore() throws Exception {
        // given
        TransactionApplierFactory base = newApplier(false);
        TransactionApplierFactory indexApplier = newIndexApplier();
        IdGeneratorUpdatesWorkSync idGeneratorUpdatesWorkSync = new IdGeneratorUpdatesWorkSync();
        Stream.of(RecordIdType.values()).forEach(idType -> idGeneratorUpdatesWorkSync.add(mock(IdGenerator.class)));
        TransactionApplierFactoryChain applier =
                new TransactionApplierFactoryChain(IdGeneratorUpdatesWorkSync::newBatch, base, indexApplier);
        SchemaRecord before = new SchemaRecord(21).initialize(true, Record.NO_NEXT_PROPERTY.longValue());
        SchemaRecord after = new SchemaRecord(before).initialize(false, Record.NO_NEXT_PROPERTY.longValue());
        IndexDescriptor rule = indexRule(0, 1, 2, "K", "X.Y");
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, rule);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(indexingService).dropIndex(rule);
        verify(cacheAccess).removeSchemaRuleFromCache(command.getKey());
    }

    @Test
    void shouldApplyDeleteIndexRuleSchemaRuleCommandToTheStoreInRecovery() throws Exception {
        // given
        TransactionApplierFactory applier = newApplierFacade(newIndexApplier(), newApplier(true));
        SchemaRecord before = new SchemaRecord(21).initialize(true, Record.NO_NEXT_PROPERTY.longValue());
        SchemaRecord after = new SchemaRecord(before).initialize(false, Record.NO_NEXT_PROPERTY.longValue());
        IndexDescriptor rule = indexRule(0, 1, 2, "K", "X.Y");
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, rule);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(schemaStore.getIdGenerator()).setHighestPossibleIdInUse(after.getId());
        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(indexingService).dropIndex(rule);
        verify(cacheAccess).removeSchemaRuleFromCache(command.getKey());
    }

    @Test
    void shouldApplyCreateUniquenessConstraintRuleSchemaRuleCommandToTheStore() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(false);
        SchemaRecord before = new SchemaRecord(21);
        SchemaRecord after = new SchemaRecord(before).initialize(true, Record.NO_NEXT_PROPERTY.longValue());
        after.setCreated();
        after.setConstraint(true);
        ConstraintDescriptor rule = uniquenessConstraintRule(0L, 1, 2, 3L);
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, rule);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(cacheAccess).addSchemaRule(rule);
    }

    @Test
    void shouldApplyCreateUniquenessConstraintRuleSchemaRuleCommandToTheStoreInRecovery() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(true);
        SchemaRecord before = new SchemaRecord(21);
        SchemaRecord after = new SchemaRecord(before).initialize(true, Record.NO_NEXT_PROPERTY.longValue());
        after.setCreated();
        after.setConstraint(true);
        ConstraintDescriptor rule = uniquenessConstraintRule(0L, 1, 2, 3L);
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, rule);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(schemaStore.getIdGenerator()).setHighestPossibleIdInUse(after.getId());
        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(cacheAccess).addSchemaRule(rule);
    }

    @Test
    void shouldApplyUpdateUniquenessConstraintRuleSchemaRuleCommandToTheStore() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(false);
        SchemaRecord before = new SchemaRecord(21);
        SchemaRecord after = new SchemaRecord(before).initialize(true, Record.NO_NEXT_PROPERTY.longValue());
        after.setConstraint(true);
        ConstraintDescriptor rule = uniquenessConstraintRule(0L, 1, 2, 3L);
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, rule);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(cacheAccess).addSchemaRule(rule);
    }

    @Test
    void shouldApplyUpdateUniquenessConstraintRuleSchemaRuleCommandToTheStoreInRecovery() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(true);
        SchemaRecord before = new SchemaRecord(21);
        SchemaRecord after = new SchemaRecord(before).initialize(true, Record.NO_NEXT_PROPERTY.longValue());
        after.setConstraint(true);
        ConstraintDescriptor rule = uniquenessConstraintRule(0L, 1, 2, 3L);
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, rule);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(schemaStore.getIdGenerator()).setHighestPossibleIdInUse(after.getId());
        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(cacheAccess).addSchemaRule(rule);
    }

    @Test
    void shouldApplyDeleteUniquenessConstraintRuleSchemaRuleCommandToTheStore() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(false);
        SchemaRecord before = new SchemaRecord(21).initialize(true, Record.NO_NEXT_PROPERTY.longValue());
        SchemaRecord after = new SchemaRecord(before).initialize(false, Record.NO_NEXT_PROPERTY.longValue());
        ConstraintDescriptor rule = uniquenessConstraintRule(0L, 1, 2, 3L);
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, rule);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(cacheAccess).removeSchemaRuleFromCache(command.getKey());
    }

    @Test
    void shouldApplyDeleteUniquenessConstraintRuleSchemaRuleCommandToTheStoreInRecovery() throws Exception {
        // given
        TransactionApplierFactory applier = newApplier(true);
        SchemaRecord before = new SchemaRecord(21).initialize(true, Record.NO_NEXT_PROPERTY.longValue());
        SchemaRecord after = new SchemaRecord(before).initialize(false, Record.NO_NEXT_PROPERTY.longValue());
        ConstraintDescriptor rule = uniquenessConstraintRule(0L, 1, 2, 3L);
        Command.SchemaRuleCommand command =
                new Command.SchemaRuleCommand(LATEST_LOG_SERIALIZATION, before, after, rule);

        // when
        boolean result = apply(applier, command::handle, transactionToApply);

        // then
        assertFalse(result);

        verify(schemaStore.getIdGenerator()).setHighestPossibleIdInUse(after.getId());
        verify(schemaStore).updateRecord(eq(after), any(), any(), any(), any());
        verify(cacheAccess).removeSchemaRuleFromCache(command.getKey());
    }

    private TransactionApplierFactory newApplier(boolean recovery) {
        TransactionApplierFactory applier = new LockGuardedNeoStoreTransactionApplierFactory(
                recovery ? RECOVERY : INTERNAL, neoStores, cacheAccess, lockService);
        if (recovery) {
            applier = newApplierFacade(new HighIdTransactionApplierFactory(neoStores), applier);
        }
        return applier;
    }

    private static TransactionApplierFactory newApplierFacade(TransactionApplierFactory... appliers) {
        return new TransactionApplierFactoryChain(IdGeneratorUpdatesWorkSync::newBatch, appliers);
    }

    private TransactionApplierFactory newIndexApplier() {
        return new IndexTransactionApplierFactory(INTERNAL, indexingService);
    }

    private boolean apply(
            TransactionApplierFactory applier, ApplyFunction function, CommandBatchToApply transactionToApply)
            throws Exception {
        try {
            return CommandHandlerContract.apply(applier, function, transactionToApply);
        } finally {
            indexActivator.close();
        }
    }

    private boolean apply(
            TransactionApplierFactory applier,
            ApplyFunction function,
            BatchContext context,
            CommandBatchToApply transactionToApply)
            throws Exception {
        try (context) {
            return CommandHandlerContract.apply(applier, function, context, transactionToApply);
        } finally {
            indexActivator.close();
        }
    }

    // SCHEMA RULE COMMAND

    private static IndexDescriptor indexRule(
            long id, int label, int propertyKeyId, String providerKey, String providerVersion) {
        IndexProviderDescriptor indexProvider = new IndexProviderDescriptor(providerKey, providerVersion);
        return IndexPrototype.forSchema(forLabel(label, propertyKeyId), indexProvider)
                .withName("index_" + id)
                .materialise(id);
    }

    private static IndexDescriptor constraintIndexRule(
            long id, int label, int propertyKeyId, String providerKey, String providerVersion, long constraintId) {
        LabelSchemaDescriptor schema = forLabel(label, propertyKeyId);
        IndexProviderDescriptor indexProvider = new IndexProviderDescriptor(providerKey, providerVersion);
        return IndexPrototype.uniqueForSchema(schema, indexProvider)
                .withName("constraint_" + id)
                .materialise(id)
                .withOwningConstraintId(constraintId);
    }

    private static ConstraintDescriptor uniquenessConstraintRule(
            long id, int labelId, int propertyKeyId, long ownedIndexRule) {
        return ConstraintDescriptorFactory.uniqueForLabel(labelId, propertyKeyId)
                .withId(id)
                .withOwnedIndexId(ownedIndexRule);
    }
}
