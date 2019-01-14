/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.newapi;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.TestRelationshipChain;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.explicitindex.AutoIndexOperations;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.api.store.DefaultIndexReference;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory.existsForRelType;
import static org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory.uniqueForLabel;
import static org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory.uniqueForSchema;
import static org.neo4j.kernel.impl.newapi.TwoPhaseNodeForRelationshipLockingTest.returnRelationships;
import static org.neo4j.values.storable.Values.NO_VALUE;

public class OperationsLockTest
{
    private KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
    private Operations operations;
    private final Locks.Client locks = mock( Locks.Client.class );
    private final Write write = mock( Write.class );
    private InOrder order;
    private DefaultNodeCursor nodeCursor;
    private DefaultPropertyCursor propertyCursor;
    private DefaultRelationshipScanCursor relationshipCursor;
    private TransactionState txState;
    private AllStoreHolder allStoreHolder;
    private final LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 123, 456 );
    private StoreReadLayer storeReadLayer;
    private ConstraintIndexCreator constraintIndexCreator;

    @Before
    public void setUp() throws InvalidTransactionTypeKernelException
    {
        txState = Mockito.spy( new TxState() );
        when( transaction.getReasonIfTerminated() ).thenReturn( Optional.empty() );
        when( transaction.statementLocks() ).thenReturn( new SimpleStatementLocks( locks ) );
        when( transaction.dataWrite() ).thenReturn( write );
        when( transaction.isOpen() ).thenReturn( true );
        when( transaction.lockTracer() ).thenReturn( LockTracer.NONE );
        when( transaction.txState() ).thenReturn( txState );
        when( transaction.securityContext() ).thenReturn( SecurityContext.AUTH_DISABLED );

        DefaultCursors cursors = mock( DefaultCursors.class );
        nodeCursor = mock( DefaultNodeCursor.class );
        propertyCursor = mock( DefaultPropertyCursor.class );
        relationshipCursor = mock( DefaultRelationshipScanCursor.class );
        when( cursors.allocateNodeCursor() ).thenReturn( nodeCursor );
        when( cursors.allocatePropertyCursor() ).thenReturn( propertyCursor );
        when( cursors.allocateRelationshipScanCursor() ).thenReturn( relationshipCursor );
        AutoIndexing autoindexing = mock( AutoIndexing.class );
        AutoIndexOperations autoIndexOperations = mock( AutoIndexOperations.class );
        when( autoindexing.nodes() ).thenReturn( autoIndexOperations );
        when( autoindexing.relationships() ).thenReturn( autoIndexOperations );
        StorageStatement storageStatement = mock( StorageStatement.class );
        StorageEngine engine = mock( StorageEngine.class );
        storeReadLayer = mock( StoreReadLayer.class );
        when( storeReadLayer.nodeExists( anyLong() ) ).thenReturn( true );
        when( storeReadLayer.constraintsGetForLabel( anyInt() )).thenReturn( Collections.emptyIterator() );
        when( storeReadLayer.constraintsGetAll() ).thenReturn( Collections.emptyIterator() );
        when( engine.storeReadLayer() ).thenReturn( storeReadLayer );
        allStoreHolder = new AllStoreHolder( engine, storageStatement,  transaction, cursors, mock(
                ExplicitIndexStore.class ), mock( Procedures.class ), mock( SchemaState.class ) );
        constraintIndexCreator = mock( ConstraintIndexCreator.class );
        operations = new Operations( allStoreHolder, mock( IndexTxStateUpdater.class ),
                storageStatement, transaction, new KernelToken( storeReadLayer, transaction ), cursors, autoindexing,
                constraintIndexCreator, mock( ConstraintSemantics.class ), mock( IndexProviderMap.class ) );
        operations.initialize();

        this.order = inOrder( locks, txState, storeReadLayer );
    }

    @After
    public void tearDown()
    {
        operations.release();
    }

    @Test
    public void shouldAcquireEntityWriteLockCreatingRelationship() throws Exception
    {
        // when
        long rId = operations.relationshipCreate( 1, 2, 3 );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 3 );
        order.verify( txState ).relationshipDoCreate( rId, 2, 1, 3 );
    }

    @Test
    public void shouldAcquireNodeLocksWhenCreatingRelationshipInOrderOfAscendingId() throws Exception
    {
        // GIVEN
        long lowId = 3;
        long highId = 5;
        int relationshipLabel = 0;

        {
            // WHEN
            operations.relationshipCreate( lowId, relationshipLabel, highId );

            // THEN
            InOrder lockingOrder = inOrder( locks );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, lowId );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, highId );
            lockingOrder.verifyNoMoreInteractions();
            reset( locks );
        }

        {
            // WHEN
            operations.relationshipCreate( highId, relationshipLabel, lowId );

            // THEN
            InOrder lockingOrder = inOrder( locks );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, lowId );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, highId );
            lockingOrder.verifyNoMoreInteractions();
        }
    }

    @Test
    public void shouldAcquireNodeLocksWhenDeletingRelationshipInOrderOfAscendingId() throws Exception
    {
        // GIVEN
        final long relationshipId = 10;
        final long lowId = 3;
        final long highId = 5;
        int relationshipLabel = 0;

        {
            // and GIVEN
            setStoreRelationship( relationshipId, lowId, highId, relationshipLabel );

            // WHEN
            operations.relationshipDelete( relationshipId );

            // THEN
            InOrder lockingOrder = inOrder( locks );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, lowId );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, highId );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, relationshipId );
            lockingOrder.verifyNoMoreInteractions();
            reset( locks );
        }

        {
            // and GIVEN
            setStoreRelationship( relationshipId, highId, lowId, relationshipLabel );

            // WHEN
            operations.relationshipDelete( relationshipId );

            // THEN
            InOrder lockingOrder = inOrder( locks );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, lowId );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, highId );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, relationshipId );
            lockingOrder.verifyNoMoreInteractions();
        }
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeAddingLabelToNode() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );

        // when
        operations.nodeAddLabel( 123L, 456 );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123L );
        order.verify( txState ).nodeDoAddLabel( 456, 123L );
    }

    @Test
    public void shouldNotAcquireEntityWriteLockBeforeAddingLabelToJustCreatedNode() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );
        when( transaction.hasTxStateWithChanges() ).thenReturn( true );

        // when
        txState.nodeDoCreate( 123 );
        operations.nodeAddLabel( 123, 456 );

        // then
        verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeAddingLabelToNode() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );

        // when
        int labelId = 456;
        operations.nodeAddLabel( 123, labelId );

        // then
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( txState ).nodeDoAddLabel( labelId, 123 );
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeSettingPropertyOnNode() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );
        int propertyKeyId = 8;
        Value value = Values.of( 9 );
        when( propertyCursor.next() ).thenReturn( true );
        when( propertyCursor.propertyKey() ).thenReturn( propertyKeyId );
        when( propertyCursor.propertyValue() ).thenReturn( NO_VALUE );

        // when
        operations.nodeSetProperty( 123, propertyKeyId, value );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        order.verify( txState ).nodeDoAddProperty( 123, propertyKeyId, value );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeSettingPropertyOnNode() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );
        int relatedLabelId = 50;
        int unrelatedLabelId = 51;
        int propertyKeyId = 8;
        int unrelatedPropertyKeyId = 88;
        Value value = Values.of( 9 );
        when( propertyCursor.next() ).thenReturn( true );
        when( propertyCursor.propertyKey() ).thenReturn( propertyKeyId );
        when( propertyCursor.propertyValue() ).thenReturn( NO_VALUE );
        when( storeReadLayer.constraintsGetAll() ).thenReturn(
                Iterators.iterator( ConstraintDescriptorFactory.uniqueForLabel( relatedLabelId, propertyKeyId ),
                                    ConstraintDescriptorFactory.uniqueForLabel( unrelatedLabelId, unrelatedPropertyKeyId )) );

        // when
        operations.nodeSetProperty( 123, propertyKeyId, value );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, relatedLabelId );
        order.verify( locks, never() ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, unrelatedLabelId );
        order.verify( txState ).nodeDoAddProperty( 123, propertyKeyId, value );
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeSettingPropertyOnRelationship() throws Exception
    {
        // given
        when( relationshipCursor.next() ).thenReturn( true );
        int propertyKeyId = 8;
        Value value = Values.of( 9 );
        when( propertyCursor.next() ).thenReturn( true );
        when( propertyCursor.propertyKey() ).thenReturn( propertyKeyId );
        when( propertyCursor.propertyValue() ).thenReturn( NO_VALUE );

        // when
        operations.relationshipSetProperty( 123, propertyKeyId, value );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 123 );
        order.verify( txState ).relationshipDoReplaceProperty( 123, propertyKeyId, NO_VALUE, value );
    }

    @Test
    public void shouldNotAcquireEntityWriteLockBeforeSettingPropertyOnJustCreatedNode() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );
        when( transaction.hasTxStateWithChanges() ).thenReturn( true );
        txState.nodeDoCreate( 123 );
        int propertyKeyId = 8;
        Value value = Values.of( 9 );

        // when
        operations.nodeSetProperty( 123, propertyKeyId, value );

        // then
        verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        verify( txState ).nodeDoAddProperty( 123, propertyKeyId, value );
    }

    @Test
    public void shouldNotAcquireEntityWriteLockBeforeSettingPropertyOnJustCreatedRelationship() throws Exception
    {
        // given
        when( relationshipCursor.next() ).thenReturn( true );
        when( transaction.hasTxStateWithChanges() ).thenReturn( true );
        txState.relationshipDoCreate( 123, 42, 43, 45 );
        int propertyKeyId = 8;
        Value value = Values.of( 9 );

        // when
        operations.relationshipSetProperty( 123, propertyKeyId, value );

        // then
        verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 123 );
        verify( txState ).relationshipDoReplaceProperty( 123, propertyKeyId, NO_VALUE, value );
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeDeletingNode()
            throws AutoIndexingKernelException
    {
        // GIVEN
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );
        when( allStoreHolder.nodeExistsInStore( 123 ) ).thenReturn( true );

        // WHEN
        operations.nodeDelete(  123 );

        //THEN
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        order.verify( txState ).nodeDoDelete( 123 );
    }

    @Test
    public void shouldNotAcquireEntityWriteLockBeforeDeletingJustCreatedNode() throws Exception
    {
        // THEN
        txState.nodeDoCreate( 123 );
        when( transaction.hasTxStateWithChanges() ).thenReturn( true );

        // WHEN
        operations.nodeDelete( 123 );

        //THEN
        verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        verify( txState ).nodeDoDelete( 123 );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeGettingConstraintsByLabelAndProperty()
    {
        // WHEN
        allStoreHolder.constraintsGetForSchema( descriptor );

        // THEN
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( storeReadLayer ).constraintsGetForSchema( descriptor );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeGettingConstraintsByLabel()
    {
        // WHEN
        allStoreHolder.constraintsGetForLabel( 42 );

        // THEN
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, 42 );
        order.verify( storeReadLayer ).constraintsGetForLabel( 42 );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeCheckingExistenceConstraints()
    {
        // WHEN
        allStoreHolder.constraintExists( ConstraintDescriptorFactory.uniqueForSchema( descriptor ) );

        // THEN
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, 123 );
        order.verify( storeReadLayer ).constraintExists( any() );
    }

    @Test
    public void shouldAcquireSchemaReadLockLazilyBeforeGettingAllConstraints()
    {
        // given
        int labelId = 1;
        int relTypeId = 2;
        UniquenessConstraintDescriptor uniquenessConstraint = uniqueForLabel( labelId, 2, 3, 3 );
        RelExistenceConstraintDescriptor existenceConstraint = existsForRelType( relTypeId, 3, 4, 5 );
        when( storeReadLayer.constraintsGetAll() )
                .thenReturn( Iterators.iterator( uniquenessConstraint, existenceConstraint ) );

        // when
        Iterator<ConstraintDescriptor> result = allStoreHolder.constraintsGetAll( );
        Iterators.count( result );

        // then
        assertThat( asList( result ), empty() );
        order.verify( storeReadLayer ).constraintsGetAll();
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP_TYPE, relTypeId );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeRemovingIndexRule() throws Exception
    {
        // given
        SchemaIndexDescriptor index = SchemaIndexDescriptorFactory.forLabel( 0, 0 );
        when( storeReadLayer.indexGetForSchema( any() )).thenReturn( index );

        // when
        operations.indexDrop( DefaultIndexReference.fromDescriptor( index ) );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, 0 );
        order.verify( txState ).indexDoDrop( index );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeCreatingUniquenessConstraint() throws Exception
    {
        // given
        when( constraintIndexCreator.createUniquenessConstraintIndex( transaction, descriptor, null ) ).thenReturn( 42L );
        when( storeReadLayer.constraintsGetForSchema(  descriptor.schema() ) ).thenReturn( Collections.emptyIterator() );

        // when
        operations.uniquePropertyConstraintCreate( descriptor );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( txState ).constraintDoAdd( ConstraintDescriptorFactory.uniqueForSchema( descriptor ), 42L );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeDroppingConstraint() throws Exception
    {
        // given
        UniquenessConstraintDescriptor constraint = uniqueForSchema( descriptor );
        when( storeReadLayer.constraintExists( constraint ) ).thenReturn( true );

        // when
        operations.constraintDrop( constraint );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( txState ).constraintDoDrop( constraint );
    }

    @Test
    public void detachDeleteNodeWithoutRelationshipsExclusivelyLockNode() throws KernelException
    {
        long nodeId = 1L;
        returnRelationships( transaction, false, new TestRelationshipChain( nodeId ) );
        when( transaction.ambientNodeCursor() ).thenReturn( new StubNodeCursor( false ).withNode( nodeId ) );
        when( nodeCursor.next() ).thenReturn( true );
        LabelSet labels = mock( LabelSet.class );
        when( labels.all() ).thenReturn( EMPTY_LONG_ARRAY );
        when( nodeCursor.labels() ).thenReturn( labels );

        operations.nodeDetachDelete( nodeId );

        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
        order.verify( locks, never() ).releaseExclusive( ResourceTypes.NODE, nodeId );
        order.verify( txState ).nodeDoDelete( nodeId );
    }

    @Test
    public void detachDeleteNodeExclusivelyLockNodes() throws KernelException
    {
        long nodeId = 1L;
        returnRelationships( transaction, false,
                new TestRelationshipChain( nodeId ).outgoing( 1, 2L, 42 ) );
        when( transaction.ambientNodeCursor() ).thenReturn( new StubNodeCursor( false ).withNode( nodeId ) );
        LabelSet labels = mock( LabelSet.class );
        when( labels.all() ).thenReturn( EMPTY_LONG_ARRAY );
        when( nodeCursor.labels() ).thenReturn( labels );
        when( nodeCursor.next() ).thenReturn( true );

        operations.nodeDetachDelete( nodeId );

        order.verify( locks ).acquireExclusive(
                LockTracer.NONE, ResourceTypes.NODE, nodeId, 2L );
        order.verify( locks, never() ).releaseExclusive( ResourceTypes.NODE, nodeId );
        order.verify( locks, never() ).releaseExclusive( ResourceTypes.NODE, 2L );
        order.verify( txState ).nodeDoDelete( nodeId );
    }

    @Test
    public void shouldAcquiredSharedLabelLocksWhenDeletingNode() throws AutoIndexingKernelException
    {
        // given
        long nodeId = 1L;
        long labelId1 = 1;
        long labelId2 = 2;
        when( nodeCursor.next() ).thenReturn( true );
        LabelSet labels = mock( LabelSet.class );
        when( labels.all() ).thenReturn( new long[]{labelId1, labelId2} );
        when( nodeCursor.labels() ).thenReturn( labels );

        // when
        operations.nodeDelete( nodeId );

        // then
        InOrder order = inOrder( locks );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId1, labelId2 );
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldAcquiredSharedLabelLocksWhenDetachDeletingNode() throws KernelException
    {
        // given
        long nodeId = 1L;
        long labelId1 = 1;
        long labelId2 = 2;

        returnRelationships( transaction, false, new TestRelationshipChain( nodeId ) );
        when( transaction.ambientNodeCursor() ).thenReturn( new StubNodeCursor( false ).withNode( nodeId ) );
        when( nodeCursor.next() ).thenReturn( true );
        LabelSet labels = mock( LabelSet.class );
        when( labels.all() ).thenReturn( new long[]{labelId1, labelId2} );
        when( nodeCursor.labels() ).thenReturn( labels );

        // when
        operations.nodeDetachDelete( nodeId );

        // then
        InOrder order = inOrder( locks );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId1, labelId2 );
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldAcquiredSharedLabelLocksWhenRemovingNodeLabel() throws EntityNotFoundException
    {
        // given
        long nodeId = 1L;
        int labelId = 1;
        when( nodeCursor.next() ).thenReturn( true );
        LabelSet labels = mock( LabelSet.class );
        when( labels.contains( labelId ) ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( labels );

        // when
        operations.nodeRemoveLabel( nodeId, labelId );

        // then
        InOrder order = inOrder( locks );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldAcquiredSharedLabelLocksWhenRemovingNodeProperty() throws AutoIndexingKernelException, EntityNotFoundException
    {
        // given
        long nodeId = 1L;
        long labelId1 = 1;
        long labelId2 = 1;
        int propertyKeyId = 5;
        when( nodeCursor.next() ).thenReturn( true );
        LabelSet labels = mock( LabelSet.class );
        when( labels.all() ).thenReturn( new long[]{labelId1, labelId2} );
        when( nodeCursor.labels() ).thenReturn( labels );
        when( propertyCursor.next() ).thenReturn( true );
        when( propertyCursor.propertyKey() ).thenReturn( propertyKeyId );
        when( propertyCursor.propertyValue() ).thenReturn( Values.of( "abc" ) );

        // when
        operations.nodeRemoveProperty( nodeId, propertyKeyId );

        // then
        InOrder order = inOrder( locks );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId1, labelId2 );
        order.verifyNoMoreInteractions();
    }

    private void setStoreRelationship( long relationshipId, long sourceNode, long targetNode, int relationshipLabel )
    {
        when( relationshipCursor.next() ).thenReturn( true );
        when( relationshipCursor.relationshipReference() ).thenReturn( relationshipId );
        when( relationshipCursor.sourceNodeReference() ).thenReturn( sourceNode );
        when( relationshipCursor.targetNodeReference() ).thenReturn( targetNode );
        when( relationshipCursor.type() ).thenReturn( relationshipLabel );
    }
}
