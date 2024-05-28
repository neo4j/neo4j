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
package org.neo4j.kernel.impl.newapi;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.schema.SchemaDescriptors.forRelType;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForRelType;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForSchema;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.keyForSchema;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.uniqueForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.uniqueForSchema;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.TestRelationshipChain;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.ExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.internal.schema.constraints.KeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingProvidersService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.index.schema.RangeIndexProvider;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.locking.ResourceIds;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;
import org.neo4j.logging.FormattedLogFormat;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.test.InMemoryTokens;
import org.neo4j.test.LatestVersions;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class PlainOperationsTest extends OperationsTest {

    @Override
    FormattedLogFormat getFormat() {
        return FormattedLogFormat.PLAIN;
    }

    @Test
    void shouldAcquireEntityWriteLockCreatingRelationship() throws Exception {
        // when
        long sourceNode = 1;
        long targetNode = 3;
        int relationshipType = 2;
        long rId = operations.relationshipCreate(sourceNode, relationshipType, targetNode);

        // then
        order.verify(storageLocks)
                .acquireRelationshipCreationLock(LockTracer.NONE, sourceNode, targetNode, false, false);
        order.verify(storageLocks).acquireExclusiveRelationshipLock(LockTracer.NONE, rId);
        order.verify(txState).relationshipDoCreate(rId, relationshipType, sourceNode, targetNode);
    }

    @Test
    void shouldAcquireNodeLocksWhenCreatingRelationshipInOrderOfAscendingId() throws Exception {
        // GIVEN
        long lowId = 3;
        long highId = 5;
        int relationshipLabel = 0;

        // WHEN
        operations.relationshipCreate(lowId, relationshipLabel, highId);

        // THEN
        InOrder lockingOrder = inOrder(creationContext, storageLocks);
        lockingOrder
                .verify(storageLocks)
                .acquireRelationshipCreationLock(
                        eq(LockTracer.NONE), eq(lowId), eq(highId), anyBoolean(), anyBoolean());
        lockingOrder.verify(creationContext).reserveRelationship(lowId, highId, relationshipLabel, false, false);
        lockingOrder.verify(storageLocks).acquireExclusiveRelationshipLock(eq(LockTracer.NONE), anyLong());
        lockingOrder.verifyNoMoreInteractions();
        reset(creationContext);

        // WHEN
        operations.relationshipCreate(highId, relationshipLabel, lowId);

        // THEN
        InOrder lowLockingOrder = inOrder(creationContext, storageLocks);
        lowLockingOrder
                .verify(storageLocks)
                .acquireRelationshipCreationLock(
                        eq(LockTracer.NONE), eq(highId), eq(lowId), anyBoolean(), anyBoolean());
        lowLockingOrder.verify(creationContext).reserveRelationship(highId, lowId, relationshipLabel, false, false);
        lowLockingOrder.verify(storageLocks).acquireExclusiveRelationshipLock(eq(LockTracer.NONE), anyLong());
        lowLockingOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldCheckNodeExistenceBeforeRelationshipIdAllocation() throws EntityNotFoundException {
        // Given
        long sourceNode = 1;
        long targetNode = 3;
        int relationshipType = 2;

        // When
        operations.relationshipCreate(sourceNode, relationshipType, targetNode);

        // Then
        InOrder inOrder = inOrder(storageReader, storageLocks, creationContext);

        inOrder.verify(storageLocks)
                .acquireRelationshipCreationLock(
                        eq(LockTracer.NONE), eq(sourceNode), eq(targetNode), anyBoolean(), anyBoolean());
        inOrder.verify(storageReader).nodeExists(eq(sourceNode), any());
        inOrder.verify(storageReader).nodeExists(eq(targetNode), any());
        inOrder.verify(creationContext)
                .reserveRelationship(eq(sourceNode), eq(targetNode), eq(relationshipType), eq(false), eq(false));
        inOrder.verify(storageLocks).acquireExclusiveRelationshipLock(eq(LockTracer.NONE), anyLong());
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquireNodeLocksWhenDeletingRelationshipInOrderOfAscendingId() {
        // GIVEN
        final long relationshipId = 10;
        final long lowId = 3;
        final long highId = 5;
        int relationshipLabel = 0;

        // and GIVEN
        setStoreRelationship(relationshipId, lowId, highId, relationshipLabel);

        // WHEN
        operations.relationshipDelete(relationshipId);

        // THEN
        InOrder lockingOrder = inOrder(storageLocks);
        lockingOrder
                .verify(storageLocks)
                .acquireRelationshipDeletionLock(LockTracer.NONE, lowId, highId, relationshipId, false, false, false);
        lockingOrder.verifyNoMoreInteractions();
        reset(storageLocks);

        // and GIVEN
        setStoreRelationship(relationshipId, highId, lowId, relationshipLabel);

        // WHEN
        operations.relationshipDelete(relationshipId);

        // THEN
        InOrder highLowIdOrder = inOrder(storageLocks);
        highLowIdOrder
                .verify(storageLocks)
                .acquireRelationshipDeletionLock(LockTracer.NONE, highId, lowId, relationshipId, false, false, false);
        highLowIdOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquireEntityWriteLockBeforeAddingLabelToNode() throws Exception {
        // given
        when(nodeCursor.next()).thenReturn(true);
        when(nodeCursor.labels()).thenReturn(TokenSet.NONE);

        // when
        operations.nodeAddLabel(123L, 456);

        // then
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.NODE, 123L);
        order.verify(txState).nodeDoAddLabel(456, 123L);
    }

    @Test
    void shouldNotAcquireEntityWriteLockBeforeAddingLabelToJustCreatedNode() throws Exception {
        // given
        when(nodeCursor.next()).thenReturn(true);
        when(nodeCursor.labels()).thenReturn(TokenSet.NONE);
        when(transaction.hasTxStateWithChanges()).thenReturn(true);

        // when
        txState.nodeDoCreate(123);
        operations.nodeAddLabel(123, 456);

        // then
        verify(locks, never()).acquireExclusive(LockTracer.NONE, ResourceType.NODE, 123);
    }

    @Test
    void shouldAcquireSchemaReadLockBeforeAddingLabelToNode() throws Exception {
        // given
        when(nodeCursor.next()).thenReturn(true);
        when(nodeCursor.labels()).thenReturn(TokenSet.NONE);

        // when
        int labelId = 456;
        operations.nodeAddLabel(123, labelId);

        // then
        order.verify(locks).acquireShared(LockTracer.NONE, ResourceType.LABEL, labelId);
        order.verify(txState).nodeDoAddLabel(labelId, 123);
    }

    @Test
    void shouldAcquireEntityWriteLockBeforeSettingPropertyOnNode() throws Exception {
        // given
        when(nodeCursor.next()).thenReturn(true);
        when(nodeCursor.labelsAndProperties(any(PropertyCursor.class), any(PropertySelection.class)))
                .thenReturn(TokenSet.NONE);
        int propertyKeyId = 8;
        Value value = Values.of(9);
        when(propertyCursor.next()).thenReturn(true);
        when(propertyCursor.propertyKey()).thenReturn(propertyKeyId);
        when(propertyCursor.propertyValue()).thenReturn(NO_VALUE);

        // when
        operations.nodeSetProperty(123, propertyKeyId, value);

        // then
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.NODE, 123);
        order.verify(txState).nodeDoAddProperty(123, propertyKeyId, value);
    }

    @Test
    void shouldAcquireSchemaReadLockBeforeSettingPropertyOnNode() throws Exception {
        // given
        int relatedLabelId = 50;
        int unrelatedLabelId = 51;
        int propertyKeyId = 8;
        when(nodeCursor.next()).thenReturn(true);
        TokenSet tokenSet = mock(TokenSet.class);
        when(tokenSet.all()).thenReturn(new int[] {relatedLabelId});
        when(nodeCursor.labelsAndProperties(any(PropertyCursor.class), any(PropertySelection.class)))
                .thenReturn(tokenSet);
        Value value = Values.of(9);
        when(propertyCursor.next()).thenReturn(true);
        when(propertyCursor.propertyKey()).thenReturn(propertyKeyId);
        when(propertyCursor.propertyValue()).thenReturn(NO_VALUE);

        // when
        operations.nodeSetProperty(123, propertyKeyId, value);

        // then
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.NODE, 123);
        order.verify(locks).acquireShared(LockTracer.NONE, ResourceType.LABEL, relatedLabelId);
        order.verify(locks, never()).acquireShared(LockTracer.NONE, ResourceType.LABEL, unrelatedLabelId);
        order.verify(txState).nodeDoAddProperty(123, propertyKeyId, value);
    }

    @Test
    void shouldAcquireEntityWriteLockBeforeSettingPropertyOnRelationship() throws Exception {
        // given
        when(relationshipCursor.next()).thenReturn(true);
        int propertyKeyId = 8;
        Value value = Values.of(9);
        when(propertyCursor.next()).thenReturn(true);
        when(propertyCursor.propertyKey()).thenReturn(propertyKeyId);
        when(propertyCursor.propertyValue()).thenReturn(NO_VALUE);

        // when
        operations.relationshipSetProperty(123, propertyKeyId, value);

        // then
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.RELATIONSHIP, 123);
        order.verify(txState)
                .relationshipDoReplaceProperty(
                        eq(123L), anyInt(), anyLong(), anyLong(), eq(propertyKeyId), eq(NO_VALUE), eq(value));
    }

    @Test
    void shouldNotAcquireEntityWriteLockBeforeSettingPropertyOnJustCreatedNode() throws Exception {
        // given
        when(nodeCursor.next()).thenReturn(true);
        when(nodeCursor.labels()).thenReturn(TokenSet.NONE);
        when(nodeCursor.labelsAndProperties(any(PropertyCursor.class), any(PropertySelection.class)))
                .thenReturn(TokenSet.NONE);
        when(transaction.hasTxStateWithChanges()).thenReturn(true);
        txState.nodeDoCreate(123);
        int propertyKeyId = 8;
        Value value = Values.of(9);

        // when
        operations.nodeSetProperty(123, propertyKeyId, value);

        // then
        verify(locks, never()).acquireExclusive(LockTracer.NONE, ResourceType.NODE, 123);
        verify(txState).nodeDoAddProperty(123, propertyKeyId, value);
    }

    @Test
    void shouldNotAcquireEntityWriteLockBeforeSettingPropertyOnJustCreatedRelationship() throws Exception {
        // given
        when(relationshipCursor.next()).thenReturn(true);
        when(transaction.hasTxStateWithChanges()).thenReturn(true);
        txState.relationshipDoCreate(123, 42, 43, 45);
        int propertyKeyId = 8;
        Value value = Values.of(9);

        // when
        operations.relationshipSetProperty(123, propertyKeyId, value);

        // then
        verify(locks, never()).acquireExclusive(LockTracer.NONE, ResourceType.RELATIONSHIP, 123);
        order.verify(txState)
                .relationshipDoReplaceProperty(
                        eq(123L), anyInt(), anyLong(), anyLong(), eq(propertyKeyId), eq(NO_VALUE), eq(value));
    }

    @Test
    void shouldAcquireEntityWriteLockBeforeDeletingNode() {
        // GIVEN
        when(nodeCursor.next()).thenReturn(true);
        when(nodeCursor.labels()).thenReturn(TokenSet.NONE);

        // WHEN
        operations.nodeDelete(123);

        // THEN
        order.verify(storageLocks).acquireNodeDeletionLock(txState, LockTracer.NONE, 123);
        order.verify(txState).nodeDoDelete(123);
    }

    @Test
    void shouldNotAcquireEntityWriteLockBeforeDeletingJustCreatedNode() {
        // THEN
        txState.nodeDoCreate(123);
        when(transaction.hasTxStateWithChanges()).thenReturn(true);
        when(nodeCursor.next()).thenReturn(true);

        // WHEN
        operations.nodeDelete(123);

        // THEN
        verify(locks, never()).acquireExclusive(LockTracer.NONE, ResourceType.NODE, 123);
        verify(txState).nodeDoDelete(123);
    }

    @Test
    void shouldAcquireSchemaReadLockBeforeGettingConstraintsByLabelAndProperty() {
        // WHEN
        allStoreHolder.constraintsGetForSchema(schema);

        // THEN
        order.verify(locks).acquireShared(LockTracer.NONE, ResourceType.LABEL, schema.getLabelId());
        order.verify(storageReader).constraintsGetForSchema(schema);
    }

    @Test
    void shouldNotAcquireSchemaReadLockBeforeGettingIndexesByLabelAndProperty() {
        // WHEN
        allStoreHolder.index(schema);

        // THEN
        verifyNoMoreInteractions(locks);
        verify(storageReader).indexGetForSchema(schema);
    }

    @Test
    void shouldNotAcquireSchemaReadLockWhenGettingIndexesByLabelAndPropertyFromSnapshot() {
        // WHEN
        allStoreHolder.snapshot().index(schema);

        // THEN
        verifyNoMoreInteractions(locks);
        verify(storageReaderSnapshot).indexGetForSchema(schema);
    }

    @Test
    void shouldAcquireSchemaReadLockBeforeGettingConstraintsByLabel() {
        // WHEN
        allStoreHolder.constraintsGetForLabel(42);

        // THEN
        order.verify(locks).acquireShared(LockTracer.NONE, ResourceType.LABEL, 42);
        order.verify(storageReader).constraintsGetForLabel(42);
    }

    @Test
    void shouldAcquireSchemaReadLockBeforeGettingConstraintsByRelationshipType() {
        // WHEN
        allStoreHolder.constraintsGetForRelationshipType(42);

        // THEN
        order.verify(locks).acquireShared(LockTracer.NONE, ResourceType.RELATIONSHIP_TYPE, 42);
        order.verify(storageReader).constraintsGetForRelationshipType(42);
    }

    @Test
    void shouldNotAcquireSchemaReadLockBeforeGettingConstraintsByLabel() {
        // WHEN
        allStoreHolder.snapshot().constraintsGetForLabel(42);

        // THEN
        verifyNoMoreInteractions(locks);
        verify(storageReaderSnapshot).constraintsGetForLabel(42);
    }

    @Test
    void shouldNotAcquireSchemaReadLockBeforeGettingConstraintsByRelationshipType() {
        // WHEN
        allStoreHolder.snapshot().constraintsGetForRelationshipType(42);

        // THEN
        verifyNoMoreInteractions(locks);
        verify(storageReaderSnapshot).constraintsGetForRelationshipType(42);
    }

    @Test
    void shouldAcquireSchemaReadLockBeforeCheckingExistenceConstraints() {
        // WHEN
        allStoreHolder.constraintExists(ConstraintDescriptorFactory.uniqueForSchema(schema));

        // THEN
        order.verify(locks).acquireShared(LockTracer.NONE, ResourceType.LABEL, 123);
        order.verify(storageReader).constraintExists(any());
    }

    @Test
    void shouldAcquireSchemaReadLockLazilyBeforeGettingAllConstraints() {
        // given
        int labelId = 1;
        int relTypeId = 2;
        UniquenessConstraintDescriptor uniquenessConstraint = uniqueForLabel(labelId, 2, 3, 3);
        ExistenceConstraintDescriptor existenceConstraint = existsForRelType(false, relTypeId, 3, 4, 5);
        when(storageReader.constraintsGetAll())
                .thenReturn(Iterators.iterator(uniquenessConstraint, existenceConstraint));
        when(storageReader.constraintExists(uniquenessConstraint)).thenReturn(true);
        when(storageReader.constraintExists(existenceConstraint)).thenReturn(true);

        // when
        Iterator<ConstraintDescriptor> result = allStoreHolder.constraintsGetAll();

        // then
        assertThat(Iterators.count(result)).isEqualTo(2L);
        assertThat(asList(result)).isEmpty();
        order.verify(storageReader).constraintsGetAll();
        order.verify(locks, atLeastOnce()).acquireShared(LockTracer.NONE, ResourceType.LABEL, labelId);
        order.verify(locks, atLeastOnce()).acquireShared(LockTracer.NONE, ResourceType.RELATIONSHIP_TYPE, relTypeId);
    }

    @Test
    void shouldNotAcquireSchemaReadLockLazilyBeforeGettingAllConstraintsFromSnapshot() {
        // given
        int labelId = 1;
        int relTypeId = 2;
        UniquenessConstraintDescriptor uniquenessConstraint = uniqueForLabel(labelId, 2, 3, 3);
        ExistenceConstraintDescriptor existenceConstraint = existsForRelType(false, relTypeId, 3, 4, 5);
        when(storageReaderSnapshot.constraintsGetAll())
                .thenReturn(Iterators.iterator(uniquenessConstraint, existenceConstraint));

        // when
        Iterator<ConstraintDescriptor> result = allStoreHolder.snapshot().constraintsGetAll();

        // then
        assertThat(Iterators.count(result)).isEqualTo(2L);
        assertThat(asList(result)).isEmpty();
        verify(storageReaderSnapshot).constraintsGetAll();
        verifyNoMoreInteractions(locks);
    }

    @Test
    void shouldAcquireSchemaWriteLockBeforeRemovingIndexRule() throws Exception {
        // given
        IndexDescriptor index = IndexPrototype.forSchema(SchemaDescriptors.forLabel(0, 0))
                .withName("index")
                .materialise(0);
        IndexProxy indexProxy = mock(IndexProxy.class);
        when(indexProxy.getDescriptor()).thenReturn(index);
        when(indexingService.getIndexProxy(index)).thenReturn(indexProxy);
        when(storageReader.indexExists(index)).thenReturn(true);

        // when
        operations.indexDrop(index);

        // then
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.LABEL, 0);
        order.verify(txState).indexDoDrop(index);
    }

    @Test
    void shouldAcquireSchemaNameWriteLockBeforeRemovingIndexByName() throws Exception {
        // given
        String indexName = "My fancy index";
        IndexDescriptor index = IndexPrototype.forSchema(SchemaDescriptors.forLabel(0, 0))
                .withName(indexName)
                .materialise(0);
        IndexProxy indexProxy = mock(IndexProxy.class);
        when(indexProxy.getDescriptor()).thenReturn(index);
        when(indexingService.getIndexProxy(index)).thenReturn(indexProxy);
        when(storageReader.indexGetForName(indexName)).thenReturn(index);
        when(storageReader.indexExists(index)).thenReturn(true);

        // when
        operations.indexDrop(indexName);

        // then
        long indexNameLock = ResourceIds.schemaNameResourceId(indexName);
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.SCHEMA_NAME, indexNameLock);
        order.verify(txState).indexDoDrop(index);
    }

    @Test
    void shouldAcquireSchemaWriteLockBeforeCreatingUniquenessConstraint() throws Exception {
        // given
        IndexPrototype prototype = IndexPrototype.uniqueForSchema(schema)
                .withName("constraint name")
                .withIndexProvider(RangeIndexProvider.DESCRIPTOR);
        IndexDescriptor constraintIndex = prototype.materialise(42);
        when(constraintIndexCreator.createUniquenessConstraintIndex(any(), any(), eq(prototype), any()))
                .thenReturn(constraintIndex);
        IndexProxy indexProxy = mock(IndexProxy.class);
        when(indexProxy.getDescriptor()).thenReturn(constraintIndex);
        when(indexingService.getIndexProxy(constraintIndex)).thenReturn(indexProxy);
        when(storageReader.constraintsGetForSchema(schema)).thenReturn(Collections.emptyIterator());
        when(storageReader.indexGetForSchema(schema)).thenReturn(Collections.emptyIterator());

        // when
        operations.uniquePropertyConstraintCreate(prototype);

        // then
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.LABEL, schema.getLabelId());
        order.verify(txState).constraintDoAdd(ConstraintDescriptorFactory.uniqueForSchema(schema), constraintIndex);
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfConstraintCreationFails() throws Exception {
        // given
        UniquenessConstraintDescriptor constraint = uniqueForSchema(schema);
        storageReaderWithConstraints(constraint);
        int labelId = schema.getLabelId();
        int propertyId = schema.getPropertyId();
        when(tokenHolders.labelTokens().getTokenById(labelId)).thenReturn(new NamedToken("Label", labelId));
        when(tokenHolders.propertyKeyTokens().getTokenById(propertyId)).thenReturn(new NamedToken("prop", labelId));

        // when
        try {
            operations.uniquePropertyConstraintCreate(
                    IndexPrototype.uniqueForSchema(schema).withName("constraint name"));
            fail("Expected an exception because this schema should already be constrained.");
        } catch (AlreadyConstrainedException ignore) {
            // Good.
        }

        // then
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.LABEL, labelId);
        order.verify(storageReader).constraintsGetForSchema(schema);
        order.verify(locks).releaseExclusive(ResourceType.LABEL, labelId);
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfNodeKeyConstraintCreationFails() throws Exception {
        // given
        KeyConstraintDescriptor constraint = keyForSchema(schema);
        storageReaderWithConstraints(constraint);
        int labelId = schema.getLabelId();
        int propertyId = schema.getPropertyId();
        when(tokenHolders.labelTokens().getTokenById(labelId)).thenReturn(new NamedToken("Label", labelId));
        when(tokenHolders.propertyKeyTokens().getTokenById(propertyId)).thenReturn(new NamedToken("prop", labelId));

        // when
        try {
            operations.keyConstraintCreate(
                    IndexPrototype.uniqueForSchema(schema).withName("constraint name"));
            fail("Expected an exception because this schema should already be constrained.");
        } catch (AlreadyConstrainedException ignore) {
            // Good.
        }

        // then
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.LABEL, labelId);
        order.verify(storageReader).constraintsGetForSchema(schema);
        order.verify(locks).releaseExclusive(ResourceType.LABEL, labelId);
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfNodePropertyExistenceConstraintCreationFails() throws Exception {
        // given
        ExistenceConstraintDescriptor constraint = existsForSchema(schema, false);
        storageReaderWithConstraints(constraint);
        int labelId = schema.getLabelId();
        int propertyId = schema.getPropertyId();
        when(tokenHolders.labelTokens().getTokenById(labelId)).thenReturn(new NamedToken("Label", labelId));
        when(tokenHolders.propertyKeyTokens().getTokenById(propertyId)).thenReturn(new NamedToken("prop", labelId));

        // when
        try {
            operations.nodePropertyExistenceConstraintCreate(schema, "constraint name", false);
            fail("Expected an exception because this schema should already be constrained.");
        } catch (AlreadyConstrainedException ignore) {
            // Good.
        }

        // then
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.LABEL, labelId);
        order.verify(storageReader).constraintsGetForSchema(schema);
        order.verify(locks).releaseExclusive(ResourceType.LABEL, labelId);
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfRelationshipKeyConstraintCreationFails() throws Exception {
        // given
        RelationTypeSchemaDescriptor descriptor = forRelType(11, 13);
        KeyConstraintDescriptor constraint = keyForSchema(descriptor);
        storageReaderWithConstraints(constraint);
        int relTypeId = descriptor.getRelTypeId();
        int propertyId = descriptor.getPropertyId();
        when(tokenHolders.relationshipTypeTokens().getTokenById(relTypeId))
                .thenReturn(new NamedToken("RelType", relTypeId));
        when(tokenHolders.propertyKeyTokens().getTokenById(propertyId)).thenReturn(new NamedToken("prop", relTypeId));

        // when
        assertThatThrownBy(() -> operations.keyConstraintCreate(
                        IndexPrototype.uniqueForSchema(descriptor).withName("constraint name")))
                .isInstanceOf(AlreadyConstrainedException.class);

        // then
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.RELATIONSHIP_TYPE, relTypeId);
        order.verify(storageReader).constraintsGetForSchema(descriptor);
        order.verify(locks).releaseExclusive(ResourceType.RELATIONSHIP_TYPE, relTypeId);
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfRelationshipPropertyExistenceConstraintCreationFails() throws Exception {
        // given
        RelationTypeSchemaDescriptor descriptor = forRelType(11, 13);
        ExistenceConstraintDescriptor constraint = existsForSchema(descriptor, false);
        storageReaderWithConstraints(constraint);
        int relTypeId = descriptor.getRelTypeId();
        int propertyId = descriptor.getPropertyId();
        when(tokenHolders.relationshipTypeTokens().getTokenById(relTypeId))
                .thenReturn(new NamedToken("RelType", relTypeId));
        when(tokenHolders.propertyKeyTokens().getTokenById(propertyId)).thenReturn(new NamedToken("prop", relTypeId));

        // when
        try {
            operations.relationshipPropertyExistenceConstraintCreate(descriptor, "constraint name", false);
            fail("Expected an exception because this schema should already be constrained.");
        } catch (AlreadyConstrainedException ignore) {
            // Good.
        }

        // then
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.RELATIONSHIP_TYPE, relTypeId);
        order.verify(storageReader).constraintsGetForSchema(descriptor);
        order.verify(locks).releaseExclusive(ResourceType.RELATIONSHIP_TYPE, relTypeId);
    }

    @Test
    void shouldAcquireSchemaWriteLockBeforeDroppingConstraint() throws Exception {
        // given
        UniquenessConstraintDescriptor constraint = uniqueForSchema(schema).withName("constraint");
        IndexDescriptor index =
                IndexPrototype.uniqueForSchema(schema).withName("constraint").materialise(13);
        storageReaderWithConstraints(constraint);
        when(storageReader.indexExists(index)).thenReturn(true);
        when(storageReader.indexGetForName("constraint")).thenReturn(index);

        // when
        operations.constraintDrop(constraint, false);

        // then
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.LABEL, schema.getLabelId());
        order.verify(txState).constraintDoDrop(constraint);
        order.verify(txState).indexDoDrop(index);
    }

    @Test
    void shouldAcquireSchemaNameWriteLockBeforeDroppingConstraintByName() throws Exception {
        // given
        UniquenessConstraintDescriptor constraint = uniqueForSchema(schema).withName("constraint");
        IndexDescriptor index =
                IndexPrototype.uniqueForSchema(schema).withName("constraint").materialise(13);
        storageReaderWithConstraints(constraint);
        when(storageReader.indexExists(index)).thenReturn(true);
        when(storageReader.indexGetForName("constraint")).thenReturn(index);
        when(storageReader.constraintGetForName("constraint")).thenReturn(constraint);

        // when
        operations.constraintDrop("constraint", false);

        // then
        long nameLock = ResourceIds.schemaNameResourceId("constraint");
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.SCHEMA_NAME, nameLock);
        order.verify(txState).constraintDoDrop(constraint);
        order.verify(txState).indexDoDrop(index);
    }

    @Test
    void detachDeleteNodeWithoutRelationshipsExclusivelyLockNode() throws KernelException {
        long nodeId = 1L;
        returnRelationships(transaction, new TestRelationshipChain(nodeId));
        when(transaction.ambientNodeCursor()).thenReturn(new StubNodeCursor(false).withNode(nodeId));
        when(nodeCursor.next()).thenReturn(true);
        TokenSet labels = mock(TokenSet.class);
        when(labels.all()).thenReturn(EMPTY_INT_ARRAY);
        when(nodeCursor.labels()).thenReturn(labels);

        operations.nodeDetachDelete(nodeId);

        order.verify(storageLocks).acquireNodeDeletionLock(txState, LockTracer.NONE, nodeId);
        order.verify(locks, never()).releaseExclusive(ResourceType.NODE, nodeId);
        order.verify(txState).nodeDoDelete(nodeId);
    }

    @Test
    void detachDeleteNodeExclusivelyLockNodes() throws KernelException {
        long nodeId = 1L;
        returnRelationships(transaction, new TestRelationshipChain(nodeId).outgoing(1, 2L, 42));
        when(transaction.ambientNodeCursor()).thenReturn(new StubNodeCursor(false).withNode(nodeId));
        TokenSet labels = mock(TokenSet.class);
        when(labels.all()).thenReturn(EMPTY_INT_ARRAY);
        when(nodeCursor.labels()).thenReturn(labels);
        when(nodeCursor.next()).thenReturn(true);

        operations.nodeDetachDelete(nodeId);

        order.verify(storageLocks).acquireNodeDeletionLock(txState, LockTracer.NONE, nodeId);
        order.verify(locks, never()).releaseExclusive(ResourceType.NODE, nodeId);
        order.verify(locks, never()).releaseExclusive(ResourceType.NODE, 2L);
        order.verify(txState).nodeDoDelete(nodeId);
    }

    @Test
    void shouldAcquiredSharedLabelLocksWhenDeletingNode() {
        // given
        long nodeId = 1L;
        int labelId1 = 1;
        int labelId2 = 2;
        when(nodeCursor.next()).thenReturn(true);
        TokenSet labels = mock(TokenSet.class);
        when(labels.all()).thenReturn(new int[] {labelId1, labelId2});
        when(nodeCursor.labels()).thenReturn(labels);

        // when
        operations.nodeDelete(nodeId);

        // then
        InOrder order = inOrder(locks, creationContext, storageLocks);
        order.verify(storageLocks).acquireNodeDeletionLock(txState, LockTracer.NONE, nodeId);
        order.verify(locks).acquireShared(LockTracer.NONE, ResourceType.LABEL, labelId1, labelId2);
        order.verify(locks).acquireShared(LockTracer.NONE, ResourceType.LABEL, TOKEN_INDEX_RESOURCE_ID);
        order.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquiredSharedLabelLocksWhenDetachDeletingNode() throws KernelException {
        // given
        long nodeId = 1L;
        int labelId1 = 1;
        int labelId2 = 2;

        returnRelationships(transaction, new TestRelationshipChain(nodeId));
        when(transaction.ambientNodeCursor()).thenReturn(new StubNodeCursor(false).withNode(nodeId));
        when(nodeCursor.next()).thenReturn(true);
        TokenSet labels = mock(TokenSet.class);
        when(labels.all()).thenReturn(new int[] {labelId1, labelId2});
        when(nodeCursor.labels()).thenReturn(labels);

        // when
        operations.nodeDetachDelete(nodeId);

        // then
        InOrder order = inOrder(locks, creationContext, storageLocks);
        order.verify(storageLocks).acquireNodeDeletionLock(txState, LockTracer.NONE, nodeId);
        order.verify(locks).acquireShared(LockTracer.NONE, ResourceType.LABEL, labelId1, labelId2);
        order.verify(locks).acquireShared(LockTracer.NONE, ResourceType.LABEL, TOKEN_INDEX_RESOURCE_ID);
        order.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquiredSharedLabelLocksWhenRemovingNodeLabel() throws EntityNotFoundException {
        // given
        long nodeId = 1L;
        int labelId = 1;
        when(nodeCursor.next()).thenReturn(true);
        when(nodeCursor.hasLabel(labelId)).thenReturn(true);

        // when
        operations.nodeRemoveLabel(nodeId, labelId);

        // then
        InOrder order = inOrder(locks);
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.NODE, nodeId);
        order.verify(locks).acquireShared(LockTracer.NONE, ResourceType.LABEL, labelId);
        order.verify(locks).acquireShared(LockTracer.NONE, ResourceType.LABEL, TOKEN_INDEX_RESOURCE_ID);
        order.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquiredSharedLabelLocksWhenRemovingNodeProperty() throws EntityNotFoundException {
        // given
        long nodeId = 1L;
        int labelId1 = 1;
        int labelId2 = 1;
        int propertyKeyId = 5;
        when(nodeCursor.next()).thenReturn(true);
        TokenSet labels = mock(TokenSet.class);
        when(labels.all()).thenReturn(new int[] {labelId1, labelId2});
        when(nodeCursor.labels()).thenReturn(labels);
        when(propertyCursor.next()).thenReturn(true);
        when(propertyCursor.propertyKey()).thenReturn(propertyKeyId);
        when(propertyCursor.propertyValue()).thenReturn(Values.of("abc"));

        // when
        operations.nodeRemoveProperty(nodeId, propertyKeyId);

        // then
        InOrder order = inOrder(locks);
        order.verify(locks).acquireExclusive(LockTracer.NONE, ResourceType.NODE, nodeId);
        order.verify(locks).acquireShared(LockTracer.NONE, ResourceType.LABEL, labelId1, labelId2);
        order.verifyNoMoreInteractions();
    }

    @Test
    void mustAssignNameToIndexesThatDoNotHaveUserSuppliedName() throws Exception {
        when(creationContext.reserveSchema()).thenReturn(1L, 2L, 3L);
        when(tokenHolders.labelTokens().getTokenById(1)).thenReturn(new NamedToken("LabelA", 1));
        when(tokenHolders.labelTokens().getTokenById(2)).thenReturn(new NamedToken("LabelB", 1));
        when(tokenHolders.labelTokens().getTokenById(3)).thenReturn(new NamedToken("LabelC", 1));
        when(tokenHolders.propertyKeyTokens().getTokenById(1)).thenReturn(new NamedToken("PropA", 1));
        when(tokenHolders.propertyKeyTokens().getTokenById(2)).thenReturn(new NamedToken("PropB", 2));
        storageReaderWithoutConstraints();
        when(storageReader.indexGetForSchema(any())).thenReturn(Collections.emptyIterator());
        operations.indexCreate(IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 1)));
        operations.indexCreate(
                IndexPrototype.forSchema(SchemaDescriptors.fulltext(NODE, new int[] {2, 3}, new int[] {1, 2}))
                        .withIndexType(IndexType.FULLTEXT));
        operations.indexCreate(IndexPrototype.forSchema(SchemaDescriptors.forLabel(3, 1))
                .withIndexProvider(operations.indexProviderByName("provider-1.0")));
        IndexDescriptor[] indexDescriptors = txState.indexChanges().getAdded().stream()
                .sorted(Comparator.comparing(d -> d.schema().getEntityTokenIds()[0]))
                .toArray(IndexDescriptor[]::new);
        assertThat(indexDescriptors.length)
                .as(Arrays.toString(indexDescriptors))
                .isEqualTo(3);
        assertThat(indexDescriptors[0].getId())
                .as(indexDescriptors[0].toString())
                .isEqualTo(1L);
        assertThat(indexDescriptors[1].getId())
                .as(indexDescriptors[1].toString())
                .isEqualTo(2L);
        assertThat(indexDescriptors[2].getId())
                .as(indexDescriptors[2].toString())
                .isEqualTo(3L);
        assertThat(indexDescriptors[0].getName())
                .as(indexDescriptors[0].toString())
                .isEqualTo("index_b5ad8e5c");
        assertThat(indexDescriptors[1].getName())
                .as(indexDescriptors[1].toString())
                .isEqualTo("index_2813986a");
        assertThat(indexDescriptors[2].getName())
                .as(indexDescriptors[2].toString())
                .isEqualTo("index_b6cde845");
    }

    @Test
    void uniqueIndexesMustBeNamedAfterTheirConstraints() throws KernelException {
        when(creationContext.reserveSchema()).thenReturn(1L, 2L, 3L);
        when(storageReader.constraintsGetForSchema(any())).thenReturn(Iterators.emptyResourceIterator());
        when(storageReader.indexGetForSchema(any())).thenReturn(Iterators.emptyResourceIterator());
        String constraintName = "my_constraint";
        when(constraintIndexCreator.createUniquenessConstraintIndex(any(), any(), any(), any()))
                .thenAnswer(i -> {
                    IndexPrototype prototype = i.getArgument(2);
                    Optional<String> name = prototype.getName();
                    assertTrue(name.isPresent());
                    assertThat(name.get()).isEqualTo(constraintName);
                    return prototype.materialise(2);
                });
        IndexPrototype prototype = IndexPrototype.uniqueForSchema(schema).withName(constraintName);
        IndexBackedConstraintDescriptor constraint =
                operations.uniquePropertyConstraintCreate(prototype).asIndexBackedConstraint();
        assertThat(constraint.ownedIndexId()).isEqualTo(2L);
    }

    @Test
    void shouldAcquireTxStateBeforeAllocatingNodeIdInBareCreateMethod() {
        // given
        KernelTransactionImplementation ktx = mock(KernelTransactionImplementation.class);
        when(ktx.txState()).thenReturn(mock(TransactionState.class));
        when(ktx.securityContext()).thenReturn(SecurityContext.AUTH_DISABLED);
        when(ktx.securityAuthorizationHandler())
                .thenReturn(new SecurityAuthorizationHandler(CommunitySecurityLog.NULL_LOG));
        CommandCreationContext commandCreationContext = mock(CommandCreationContext.class);
        Operations operations = new Operations(
                mock(AllStoreHolder.class),
                mock(StorageReader.class),
                mock(IndexTxStateUpdater.class),
                commandCreationContext,
                mock(DbmsRuntimeVersionProvider.class),
                LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                mock(StorageLocks.class),
                ktx,
                mock(KernelToken.class),
                mock(DefaultPooledCursors.class),
                mock(ConstraintIndexCreator.class),
                mock(ConstraintSemantics.class),
                mock(IndexingProvidersService.class),
                Config.defaults(),
                INSTANCE);

        // when
        operations.nodeCreate();

        // then
        InOrder inOrder = inOrder(ktx, commandCreationContext);
        inOrder.verify(ktx).txState();
        inOrder.verify(commandCreationContext).reserveNode();
        inOrder.verify(ktx).lockTracer();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquireTxStateBeforeAllocatingNodeIdInCreateWithLabelsMethod() throws ConstraintValidationException {
        // given
        KernelTransactionImplementation ktx = mock(KernelTransactionImplementation.class);
        when(ktx.txState()).thenReturn(mock(TransactionState.class));
        when(ktx.securityAuthorizationHandler())
                .thenReturn(new SecurityAuthorizationHandler(CommunitySecurityLog.NULL_LOG));
        LockManager.Client lockClient = mock(LockManager.Client.class);
        when(ktx.lockClient()).thenReturn(lockClient);
        when(ktx.securityContext()).thenReturn(SecurityContext.AUTH_DISABLED);
        CommandCreationContext commandCreationContext = mock(CommandCreationContext.class);
        DefaultPooledCursors cursors = mock(DefaultPooledCursors.class);
        when(cursors.allocateFullAccessNodeCursor(NULL_CONTEXT)).thenReturn(mock(FullAccessNodeCursor.class));
        when(cursors.allocateFullAccessPropertyCursor(NULL_CONTEXT, INSTANCE))
                .thenReturn(mock(FullAccessPropertyCursor.class));

        Operations operations = new Operations(
                mock(AllStoreHolder.class),
                mock(StorageReader.class),
                mock(IndexTxStateUpdater.class),
                commandCreationContext,
                mock(DbmsRuntimeVersionProvider.class),
                LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                mock(StorageLocks.class),
                ktx,
                mock(KernelToken.class),
                cursors,
                mock(ConstraintIndexCreator.class),
                mock(ConstraintSemantics.class),
                mock(IndexingProvidersService.class),
                Config.defaults(),
                INSTANCE);
        operations.initialize(NULL_CONTEXT);

        // when
        operations.nodeCreateWithLabels(new int[] {1});

        // then
        InOrder inOrder = inOrder(ktx, commandCreationContext);
        inOrder.verify(ktx).txState();
        inOrder.verify(commandCreationContext).reserveNode();
        inOrder.verify(ktx).txState(); // for the constraints check for the label
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquireTxStateBeforeAllocatingRelationshipId() throws EntityNotFoundException {
        // given
        KernelTransactionImplementation ktx = mock(KernelTransactionImplementation.class);
        when(ktx.txState()).thenReturn(mock(TransactionState.class));
        LockManager.Client lockClient = mock(LockManager.Client.class);
        when(ktx.lockClient()).thenReturn(lockClient);
        when(ktx.securityContext()).thenReturn(SecurityContext.AUTH_DISABLED);
        when(ktx.securityAuthorizationHandler())
                .thenReturn(new SecurityAuthorizationHandler(CommunitySecurityLog.NULL_LOG));
        CommandCreationContext commandCreationContext = mock(CommandCreationContext.class);
        AllStoreHolder allStoreHolder = mock(AllStoreHolder.class);
        when(allStoreHolder.nodeExists(anyLong())).thenReturn(true);
        Operations operations = new Operations(
                allStoreHolder,
                mock(StorageReader.class),
                mock(IndexTxStateUpdater.class),
                commandCreationContext,
                mock(DbmsRuntimeVersionProvider.class),
                LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                mock(StorageLocks.class),
                ktx,
                mock(KernelToken.class),
                mock(DefaultPooledCursors.class),
                mock(ConstraintIndexCreator.class),
                mock(ConstraintSemantics.class),
                mock(IndexingProvidersService.class),
                Config.defaults(),
                INSTANCE);

        // when
        operations.relationshipCreate(0, 1, 2);

        // then
        InOrder inOrder = inOrder(ktx, commandCreationContext);
        inOrder.verify(ktx).txState();
        inOrder.verify(commandCreationContext)
                .reserveRelationship(anyLong(), anyLong(), anyInt(), anyBoolean(), anyBoolean());
        inOrder.verify(ktx).lockTracer();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquireTxStateBeforeAllocatingSchemaId() throws KernelException {
        // given
        KernelTransactionImplementation ktx = mock(KernelTransactionImplementation.class);
        when(ktx.txState()).thenReturn(mock(TransactionState.class));
        LockManager.Client lockClient = mock(LockManager.Client.class);
        when(ktx.lockClient()).thenReturn(lockClient);
        CommandCreationContext commandCreationContext = mock(CommandCreationContext.class);
        IndexProviderDescriptor indexProviderDescriptor = mock(IndexProviderDescriptor.class);
        IndexProvider indexProvider = mock(IndexProvider.class);
        when(indexProvider.getMinimumRequiredVersion()).thenReturn(KernelVersion.EARLIEST);
        IndexingProvidersService indexingProvidersService = mock(IndexingProvidersService.class);
        when(indexingProvidersService.getDefaultProvider()).thenReturn(indexProviderDescriptor);
        when(indexingProvidersService.getIndexProvider(any())).thenReturn(indexProvider);
        AllStoreHolder allStoreHolder = mock(AllStoreHolder.class);
        when(allStoreHolder.index(any(), any())).thenReturn(IndexDescriptor.NO_INDEX);
        when(allStoreHolder.indexGetForName(any())).thenReturn(IndexDescriptor.NO_INDEX);
        when(allStoreHolder.constraintsGetForSchema(any())).thenReturn(Iterators.emptyResourceIterator());
        Operations operations = new Operations(
                allStoreHolder,
                mock(StorageReader.class),
                mock(IndexTxStateUpdater.class),
                commandCreationContext,
                mock(DbmsRuntimeVersionProvider.class),
                LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                mock(StorageLocks.class),
                ktx,
                mock(KernelToken.class),
                mock(DefaultPooledCursors.class),
                mock(ConstraintIndexCreator.class),
                mock(ConstraintSemantics.class),
                indexingProvidersService,
                Config.defaults(),
                INSTANCE);

        // when
        operations.indexCreate(IndexPrototype.forSchema(schema).withName("name"));

        // then
        InOrder inOrder = inOrder(ktx, commandCreationContext);
        inOrder.verify(ktx).txState();
        inOrder.verify(commandCreationContext).reserveSchema();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void indexedBackedConstraintCreateMustThrowOnIndexTypeFullText() throws Exception {
        // given
        IndexPrototype prototype = IndexPrototype.uniqueForSchema(schema)
                .withName("constraint name")
                .withIndexProvider(RangeIndexProvider.DESCRIPTOR)
                .withIndexType(IndexType.FULLTEXT);
        IndexDescriptor constraintIndex = prototype.materialise(42);
        when(constraintIndexCreator.createUniquenessConstraintIndex(any(), any(), eq(prototype), any()))
                .thenReturn(constraintIndex);
        IndexProxy indexProxy = mock(IndexProxy.class);
        when(indexProxy.getDescriptor()).thenReturn(constraintIndex);
        when(indexingService.getIndexProxy(constraintIndex)).thenReturn(indexProxy);
        when(storageReader.constraintsGetForSchema(schema)).thenReturn(Collections.emptyIterator());
        when(storageReader.indexGetForSchema(schema)).thenReturn(Collections.emptyIterator());

        // when
        var e = assertThrows(KernelException.class, () -> operations.uniquePropertyConstraintCreate(prototype));
        assertThat(e.getUserMessage(new InMemoryTokens())).contains("FULLTEXT");
    }

    @Test
    void indexedBackedConstraintCreateMustThrowOnFulltextSchemas() throws Exception {
        // given
        when(tokenHolders.labelTokens().getTokenById(anyInt())).thenReturn(new NamedToken("Label", 123));
        when(tokenHolders.propertyKeyTokens().getTokenById(anyInt())).thenReturn(new NamedToken("prop", 456));
        SchemaDescriptor schema =
                SchemaDescriptors.fulltext(NODE, this.schema.getEntityTokenIds(), this.schema.getPropertyIds());
        IndexPrototype prototype = IndexPrototype.uniqueForSchema(schema)
                .withName("constraint name")
                .withIndexProvider(RangeIndexProvider.DESCRIPTOR);
        IndexDescriptor constraintIndex = prototype.materialise(42);
        when(constraintIndexCreator.createUniquenessConstraintIndex(any(), any(), eq(prototype), any()))
                .thenReturn(constraintIndex);
        IndexProxy indexProxy = mock(IndexProxy.class);
        when(indexProxy.getDescriptor()).thenReturn(constraintIndex);
        when(indexingService.getIndexProxy(constraintIndex)).thenReturn(indexProxy);
        when(storageReader.constraintsGetForSchema(schema)).thenReturn(Collections.emptyIterator());
        when(storageReader.indexGetForSchema(schema)).thenReturn(Collections.emptyIterator());

        // when
        var e = assertThrows(KernelException.class, () -> operations.uniquePropertyConstraintCreate(prototype));
        assertThat(e.getUserMessage(tokenHolders)).contains("full-text schema");
    }

    @Test
    void indexedBackedConstraintCreateMustThrowOnAnyTokenSchemas() throws Exception {
        // given
        SchemaDescriptor schema = SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR;
        IndexPrototype prototype = IndexPrototype.uniqueForSchema(schema)
                .withName("constraint name")
                .withIndexProvider(RangeIndexProvider.DESCRIPTOR);
        IndexDescriptor constraintIndex = prototype.materialise(42);
        when(constraintIndexCreator.createUniquenessConstraintIndex(any(), any(), eq(prototype), any()))
                .thenReturn(constraintIndex);
        IndexProxy indexProxy = mock(IndexProxy.class);
        when(indexProxy.getDescriptor()).thenReturn(constraintIndex);
        when(indexingService.getIndexProxy(constraintIndex)).thenReturn(indexProxy);
        when(storageReader.constraintsGetForSchema(schema)).thenReturn(Collections.emptyIterator());
        when(storageReader.indexGetForSchema(schema)).thenReturn(Collections.emptyIterator());

        // when
        var e = assertThrows(KernelException.class, () -> operations.uniquePropertyConstraintCreate(prototype));
        assertThat(e.getUserMessage(tokenHolders)).contains("any token schema");
    }

    @Test
    void indexedBackedConstraintCreateMustThrowOnNonUniqueIndexPrototypes() throws Exception {
        // given
        when(tokenHolders.labelTokens().getTokenById(anyInt())).thenReturn(new NamedToken("Label", 123));
        when(tokenHolders.propertyKeyTokens().getTokenById(anyInt())).thenReturn(new NamedToken("prop", 456));
        IndexPrototype prototype = IndexPrototype.forSchema(schema)
                .withName("constraint name")
                .withIndexProvider(RangeIndexProvider.DESCRIPTOR);
        IndexDescriptor constraintIndex = prototype.materialise(42);
        when(constraintIndexCreator.createUniquenessConstraintIndex(any(), any(), eq(prototype), any()))
                .thenReturn(constraintIndex);
        IndexProxy indexProxy = mock(IndexProxy.class);
        when(indexProxy.getDescriptor()).thenReturn(constraintIndex);
        when(indexingService.getIndexProxy(constraintIndex)).thenReturn(indexProxy);
        when(storageReader.constraintsGetForSchema(schema)).thenReturn(Collections.emptyIterator());
        when(storageReader.indexGetForSchema(schema)).thenReturn(Collections.emptyIterator());

        // when
        var e = assertThrows(KernelException.class, () -> operations.uniquePropertyConstraintCreate(prototype));
        assertThat(e.getUserMessage(tokenHolders))
                .containsIgnoringCase("index prototype")
                .containsIgnoringCase("not unique");
    }

    @Test
    void nodeAddLabelShouldSucceedWriteOnly() throws Exception {
        runForSecurityLevel(() -> operations.nodeAddLabel(1L, 2), AccessMode.Static.WRITE_ONLY, true);
    }

    @Test
    void nodeAddLabelShouldSucceedWrite() throws Exception {
        runForSecurityLevel(() -> operations.nodeAddLabel(1L, 2), AccessMode.Static.WRITE, true);
    }

    @Test
    void nodeAddLabelShouldSucceedWriteFull() throws Exception {
        runForSecurityLevel(() -> operations.nodeAddLabel(1L, 2), AccessMode.Static.FULL, true);
    }

    @Test
    void nodeRemoveLabelShouldSucceedWriteOnly() throws Exception {
        runForSecurityLevel(() -> operations.nodeRemoveLabel(1L, 3), AccessMode.Static.WRITE_ONLY, true);
    }

    @Test
    void nodeRemoveLabelShouldSucceedWrite() throws Exception {
        runForSecurityLevel(() -> operations.nodeRemoveLabel(1L, 3), AccessMode.Static.WRITE, true);
    }

    @Test
    void nodeRemoveLabelShouldSucceedWriteFull() throws Exception {
        runForSecurityLevel(() -> operations.nodeRemoveLabel(1L, 3), AccessMode.Static.FULL, true);
    }

    private static Iterator<ConstraintDescriptor> asIterator(ConstraintDescriptor constraint) {
        return singletonList(constraint).iterator();
    }

    private void storageReaderWithConstraints(ConstraintDescriptor constraint) {
        when(storageReader.constraintsGetForSchema(constraint.schema())).thenReturn(asIterator(constraint));
        when(storageReader.constraintExists(constraint)).thenReturn(true);
    }

    private void storageReaderWithoutConstraints() {
        when(storageReader.constraintsGetForSchema(any()))
                .thenReturn(Iterables.<ConstraintDescriptor>empty().iterator());
        when(storageReader.constraintExists(any())).thenReturn(false);
    }

    private void setStoreRelationship(long relationshipId, long sourceNode, long targetNode, int relationshipLabel) {
        when(relationshipCursor.next()).thenReturn(true);
        when(relationshipCursor.relationshipReference()).thenReturn(relationshipId);
        when(relationshipCursor.sourceNodeReference()).thenReturn(sourceNode);
        when(relationshipCursor.targetNodeReference()).thenReturn(targetNode);
        when(relationshipCursor.type()).thenReturn(relationshipLabel);
    }
}
