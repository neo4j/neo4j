/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.StubPropertyCursor;
import org.neo4j.internal.kernel.api.helpers.StubRelationshipCursor;
import org.neo4j.internal.kernel.api.helpers.TestRelationshipChain;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.explicitindex.AutoIndexOperations;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.api.store.DefaultCapableIndexReference;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.helpers.collection.MapUtil.genericMap;
import static org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory.existsForRelType;
import static org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory.uniqueForLabel;
import static org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory.uniqueForSchema;
import static org.neo4j.kernel.impl.api.store.DefaultIndexReference.toDescriptor;
import static org.neo4j.kernel.impl.newapi.TwoPhaseNodeForRelationshipLockingTest.returnRelationships;
import static org.neo4j.values.storable.Values.NO_VALUE;

public class OperationsLockTest
{
    private KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
    private ReadOperations readOperations;
    private WriteOperations writeOperations;
    private final Locks.Client locks = mock( Locks.Client.class );
    private final Write write = mock( Write.class );
    private final Read read = mock( Read.class );
    private final SchemaRead schemaRead = mock( SchemaRead.class );
    private InOrder order;
    private StubNodeCursor nodeCursor;
    private StubPropertyCursor propertyCursor;
    private StubRelationshipCursor relationshipCursor;
    private TransactionState txState;
    private final LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 123, 456 );
    private StorageReader storageReader;
    private ConstraintIndexCreator constraintIndexCreator;

    @Before
    public void setUp() throws InvalidTransactionTypeKernelException
    {
        txState = Mockito.spy( new TxState() );
        when( transaction.getReasonIfTerminated() ).thenReturn( Optional.empty() );
        when( transaction.statementLocks() ).thenReturn( new SimpleStatementLocks( locks ) );
        when( transaction.dataWrite() ).thenReturn( write );
        when( transaction.dataRead() ).thenReturn( read );
        when( transaction.schemaRead() ).thenReturn( schemaRead );
        when( transaction.isOpen() ).thenReturn( true );
        when( transaction.lockTracer() ).thenReturn( LockTracer.NONE );
        when( transaction.txState() ).thenReturn( txState );
        when( transaction.securityContext() ).thenReturn( SecurityContext.AUTH_DISABLED );

        nodeCursor = new StubNodeCursor( false );
        propertyCursor = new StubPropertyCursor();
        relationshipCursor = new StubRelationshipCursor();
        AutoIndexing autoindexing = mock( AutoIndexing.class );
        AutoIndexOperations autoIndexOperations = mock( AutoIndexOperations.class );
        when( autoindexing.nodes() ).thenReturn( autoIndexOperations );
        when( autoindexing.relationships() ).thenReturn( autoIndexOperations );
        StorageEngine engine = mock( StorageEngine.class );
        storageReader = mock( StorageReader.class );
        when( storageReader.allocateNodeCursor() ).thenReturn( nodeCursor );
        when( storageReader.allocatePropertyCursor() ).thenReturn( propertyCursor );
        when( storageReader.allocateRelationshipScanCursor() ).thenReturn( relationshipCursor );
        when( storageReader.nodeExists( anyLong() ) ).thenReturn( true );
        when( storageReader.constraintsGetForLabel( anyInt() )).thenReturn( Collections.emptyIterator() );
        when( storageReader.constraintsGetAll() ).thenReturn( Collections.emptyIterator() );
        doAnswer( invocationOnMock ->
        {
            nodeCursor.single( invocationOnMock.getArgument( 0 ) );
            return null;
        } ).when( storageReader ).singleNode( anyLong(), any( NodeCursor.class ) );
        when( engine.newReader() ).thenReturn( storageReader );
        ExplicitIndexStore explicitIndexStore = mock( ExplicitIndexStore.class );
        constraintIndexCreator = mock( ConstraintIndexCreator.class );
        readOperations = new ReadOperations( storageReader, transaction, explicitIndexStore, mock( Procedures.class ),
                mock( SchemaState.class ) );
        writeOperations = new WriteOperations( mock( IndexTxStateUpdater.class ), storageReader, readOperations,
                transaction, new KernelToken( storageReader, transaction ), autoindexing,
                constraintIndexCreator, mock( ConstraintSemantics.class ), explicitIndexStore );
        writeOperations.initialize();
        when( transaction.ambientNodeCursor() ).thenReturn( nodeCursor );

        this.order = inOrder( locks, txState, storageReader );
    }

    @After
    public void tearDown()
    {
        writeOperations.release();
    }

    @Test
    public void shouldAcquireEntityWriteLockCreatingRelationship() throws Exception
    {
        // when
        long rId = writeOperations.relationshipCreate( 1, 2, 3 );

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
            writeOperations.relationshipCreate( lowId, relationshipLabel, highId );

            // THEN
            InOrder lockingOrder = inOrder( locks );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, lowId );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, highId );
            lockingOrder.verifyNoMoreInteractions();
            reset( locks );
        }

        {
            // WHEN
            writeOperations.relationshipCreate( highId, relationshipLabel, lowId );

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
            relationshipCursor.withRelationshipChain( new TestRelationshipChain( lowId ).outgoing( relationshipId, highId, relationshipLabel ) );

            // WHEN
            writeOperations.relationshipDelete( relationshipId );

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
            relationshipCursor.clearStore();
            relationshipCursor.withRelationshipChain( new TestRelationshipChain( highId ).outgoing( relationshipId, lowId, relationshipLabel ) );

            // WHEN
            writeOperations.relationshipDelete( relationshipId );

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
        nodeCursor.withNode( 123 );

        // when
        writeOperations.nodeAddLabel( 123L, 456 );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123L );
        order.verify( txState ).nodeDoAddLabel( 456, 123L );
    }

    @Test
    public void shouldNotAcquireEntityWriteLockBeforeAddingLabelToJustCreatedNode() throws Exception
    {
        // given
        nodeCursor.withNode( 123 );
        when( transaction.hasTxStateWithChanges() ).thenReturn( true );

        // when
        txState.nodeDoCreate( 123 );
        writeOperations.nodeAddLabel( 123, 456 );

        // then
        verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeAddingLabelToNode() throws Exception
    {
        // given
        nodeCursor.withNode( 123 );

        // when
        int labelId = 456;
        writeOperations.nodeAddLabel( 123, labelId );

        // then
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( txState ).nodeDoAddLabel( labelId, 123 );
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeSettingPropertyOnNode() throws Exception
    {
        // given
        int propertyKeyId = 8;
        Value value = Values.of( 9 );
        nodeCursor.withNode( 123, new long[0] );

        // when
        writeOperations.nodeSetProperty( 123, propertyKeyId, value );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        order.verify( txState ).nodeDoAddProperty( 123, propertyKeyId, value );
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeSettingPropertyOnRelationship() throws Exception
    {
        // given
        relationshipCursor.withRelationshipChain( new TestRelationshipChain( 0 ).outgoing( 123, 1, 2 ) );
        int propertyKeyId = 8;
        Value value = Values.of( 9 );

        // when
        writeOperations.relationshipSetProperty( 123, propertyKeyId, value );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 123 );
        order.verify( txState ).relationshipDoReplaceProperty( 123, propertyKeyId, NO_VALUE, value );
    }

    @Test
    public void shouldNotAcquireEntityWriteLockBeforeSettingPropertyOnJustCreatedNode() throws Exception
    {
        // given
        nodeCursor.withNode( 123 );
        when( transaction.hasTxStateWithChanges() ).thenReturn( true );
        txState.nodeDoCreate( 123 );
        int propertyKeyId = 8;
        Value value = Values.of( 9 );

        // when
        writeOperations.nodeSetProperty( 123, propertyKeyId, value );

        // then
        verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        verify( txState ).nodeDoAddProperty( 123, propertyKeyId, value );
    }

    @Test
    public void shouldNotAcquireEntityWriteLockBeforeSettingPropertyOnJustCreatedRelationship() throws Exception
    {
        // given
        relationshipCursor.withRelationshipChain( new TestRelationshipChain( 0 ).outgoing( 123, 1, 2 ) );
        when( transaction.hasTxStateWithChanges() ).thenReturn( true );
        txState.relationshipDoCreate( 123, 42, 43, 45 );
        int propertyKeyId = 8;
        Value value = Values.of( 9 );

        // when
        writeOperations.relationshipSetProperty( 123, propertyKeyId, value );

        // then
        verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 123 );
        verify( txState ).relationshipDoReplaceProperty( 123, propertyKeyId, NO_VALUE, value );
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeDeletingNode()
            throws AutoIndexingKernelException
    {
        // GIVEN
        nodeCursor.withNode( 123 );
        when( storageReader.nodeExists( 123 ) ).thenReturn( true );

        // WHEN
        writeOperations.nodeDelete(  123 );

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
        writeOperations.nodeDelete( 123 );

        //THEN
        verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        verify( txState ).nodeDoDelete( 123 );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeGettingConstraintsByLabelAndProperty()
    {
        // WHEN
        readOperations.constraintsGetForSchema( descriptor );

        // THEN
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( storageReader ).constraintsGetForSchema( descriptor );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeGettingConstraintsByLabel()
    {
        // WHEN
        readOperations.constraintsGetForLabel( 42 );

        // THEN
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, 42 );
        order.verify( storageReader ).constraintsGetForLabel( 42 );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeCheckingExistenceConstraints()
    {
        // WHEN
        readOperations.constraintExists( ConstraintDescriptorFactory.uniqueForSchema( descriptor ) );

        // THEN
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, 123 );
        order.verify( storageReader ).constraintExists( any() );
    }

    @Test
    public void shouldAcquireSchemaReadLockLazilyBeforeGettingAllConstraints()
    {
        // given
        int labelId = 1;
        int relTypeId = 2;
        UniquenessConstraintDescriptor uniquenessConstraint = uniqueForLabel( labelId, 2, 3, 3 );
        RelExistenceConstraintDescriptor existenceConstraint = existsForRelType( relTypeId, 3, 4, 5 );
        when( storageReader.constraintsGetAll() )
                .thenReturn( Iterators.iterator( uniquenessConstraint, existenceConstraint ) );

        // when
        Iterator<ConstraintDescriptor> result = readOperations.constraintsGetAll();
        Iterators.count( result );

        // then
        assertThat( asList( result ), empty() );
        order.verify( storageReader ).constraintsGetAll();
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP_TYPE, relTypeId );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeRemovingIndexRule() throws Exception
    {
        // given
        DefaultCapableIndexReference index =
                new DefaultCapableIndexReference( false, IndexCapability.NO_CAPABILITY, new IndexProvider.Descriptor( "a", "b" ), 0, 0 );
        when( schemaRead.index( 0, 0 ) ).thenReturn( index );
        when( storageReader.index( 0, 0 ) ).thenReturn( index );

        // when
        writeOperations.indexDrop( index );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, 0 );
        order.verify( txState ).indexDoDrop( toDescriptor( index ) );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeCreatingUniquenessConstraint() throws Exception
    {
        // given
        when( constraintIndexCreator.createUniquenessConstraintIndex( transaction, descriptor ) ).thenReturn( 42L );
        when( storageReader.constraintsGetForSchema(  descriptor.schema() ) ).thenReturn( Collections.emptyIterator() );
        when( storageReader.index( descriptor.getLabelId(), descriptor.getPropertyIds() ) ).thenReturn( CapableIndexReference.NO_INDEX );

        // when
        writeOperations.uniquePropertyConstraintCreate( descriptor );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( txState ).constraintDoAdd( ConstraintDescriptorFactory.uniqueForSchema( descriptor ), 42L );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeDroppingConstraint() throws Exception
    {
        // given
        UniquenessConstraintDescriptor constraint = uniqueForSchema( descriptor );
        when( storageReader.constraintExists( constraint ) ).thenReturn( true );

        // when
        writeOperations.constraintDrop( constraint );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( txState ).constraintDoDrop( constraint );
    }

    @Test
    public void detachDeleteNodeWithoutRelationshipsExclusivelyLockNode() throws KernelException
    {
        // given
        long nodeId = 1L;
        returnRelationships( transaction, false, new TestRelationshipChain( nodeId ) );
        nodeCursor.withNode( nodeId );

        // when
        writeOperations.nodeDetachDelete( nodeId );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
        order.verify( locks, never() ).releaseExclusive( ResourceTypes.NODE, nodeId );
        order.verify( txState ).nodeDoDelete( nodeId );
    }

    @Test
    public void detachDeleteNodeExclusivelyLockNodes() throws KernelException
    {
        // given
        long nodeId = 1L;
        TestRelationshipChain relationshipChain = new TestRelationshipChain( nodeId ).outgoing( 1, 2L, 42 );
        returnRelationships( transaction, false, relationshipChain );
        nodeCursor.withNode( nodeId );
        relationshipCursor.withRelationshipChain( relationshipChain );

        // when
        writeOperations.nodeDetachDelete( nodeId );

        // then
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
        nodeCursor.withNode( nodeId, labelId1, labelId2 );

        // when
        writeOperations.nodeDelete( nodeId );

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
        nodeCursor.withNode( nodeId, labelId1, labelId2 );

        // when
        writeOperations.nodeDetachDelete( nodeId );

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
        nodeCursor.withNode( nodeId, labelId );

        // when
        writeOperations.nodeRemoveLabel( nodeId, labelId );

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
        nodeCursor.withNode( nodeId, new long[]{labelId1, labelId2}, genericMap( propertyKeyId, Values.of( "abc" ) ) );

        // when
        writeOperations.nodeRemoveProperty( nodeId, propertyKeyId );

        // then
        InOrder order = inOrder( locks );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId1, labelId2 );
        order.verifyNoMoreInteractions();
    }
}
