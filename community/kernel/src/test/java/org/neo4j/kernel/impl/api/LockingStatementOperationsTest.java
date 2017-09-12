/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api;

import org.junit.Test;
import org.mockito.InOrder;

import java.util.Iterator;
import java.util.Optional;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.TwoPhaseNodeForRelationshipLockingTest.RelationshipData;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.emptyIterator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory.existsForRelType;
import static org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory.uniqueForLabel;
import static org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory.uniqueForSchema;
import static org.neo4j.kernel.impl.api.TwoPhaseNodeForRelationshipLockingTest.returnRelationships;

public class LockingStatementOperationsTest
{
    private final LockingStatementOperations lockingOps;
    private final EntityReadOperations entityReadOps;
    private final EntityWriteOperations entityWriteOps;
    private final SchemaReadOperations schemaReadOps;
    private final SchemaWriteOperations schemaWriteOps;
    private final Locks.Client locks = mock( Locks.Client.class );
    private final InOrder order;
    private final KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
    private final TxState txState = new TxState();
    private final KernelStatement state = new KernelStatement( transaction, new SimpleTxStateHolder( txState ),
            mock( StorageStatement.class ), new Procedures(), new CanWrite(), LockTracer.NONE, null );
    private final SchemaStateOperations schemaStateOps;

    private final LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 123, 456 );

    public LockingStatementOperationsTest()
    {
        when( transaction.getReasonIfTerminated() ).thenReturn( Optional.empty() );
        entityReadOps = mock( EntityReadOperations.class );
        entityWriteOps = mock( EntityWriteOperations.class );
        schemaReadOps = mock( SchemaReadOperations.class );
        schemaWriteOps = mock( SchemaWriteOperations.class );
        schemaStateOps = mock( SchemaStateOperations.class );
        order = inOrder( locks, entityWriteOps, schemaReadOps, schemaWriteOps, schemaStateOps );
        lockingOps = new LockingStatementOperations(
                entityReadOps, entityWriteOps, schemaReadOps, schemaWriteOps, schemaStateOps
        );
        state.initialize( new SimpleStatementLocks( locks ), PageCursorTracer.NULL );
        state.acquire();
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeAddingLabelToNode() throws Exception
    {
        // when
        lockingOps.nodeAddLabel( state, 123, 456 );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        order.verify( entityWriteOps ).nodeAddLabel( state, 123, 456 );
    }

    @Test
    public void shouldNotAcquireEntityWriteLockBeforeAddingLabelToJustCreatedNode() throws Exception
    {
        // when
        txState.nodeDoCreate( 123 );
        lockingOps.nodeAddLabel( state, 123, 456 );

        // then
        order.verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        order.verify( entityWriteOps ).nodeAddLabel( state, 123, 456 );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeAddingLabelToNode() throws Exception
    {
        // when
        int labelId = 456;
        lockingOps.nodeAddLabel( state, 123, labelId );

        // then
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( entityWriteOps ).nodeAddLabel( state, 123, labelId );
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeSettingPropertyOnNode() throws Exception
    {
        // given
        int propertyKeyId = 8;
        Value value = Values.of( 9 );

        // when
        lockingOps.nodeSetProperty( state, 123, propertyKeyId, value );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        order.verify( entityWriteOps ).nodeSetProperty( state, 123, propertyKeyId, value );
    }

    @Test
    public void shouldNotAcquireEntityWriteLockBeforeSettingPropertyOnJustCreatedNode() throws Exception
    {
        // given
        txState.nodeDoCreate( 123 );
        int propertyKeyId = 8;
        Value value = Values.of( 9 );

        // when
        lockingOps.nodeSetProperty( state, 123, propertyKeyId, value );

        // then
        order.verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        order.verify( entityWriteOps ).nodeSetProperty( state, 123, propertyKeyId, value );
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeDeletingNode()
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        // WHEN
        lockingOps.nodeDelete( state, 123 );

        //THEN
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        order.verify( entityWriteOps ).nodeDelete( state, 123 );
    }

    @Test
    public void shouldNotAcquireEntityWriteLockBeforeDeletingJustCreatedNode() throws Exception
    {
        // WHEN
        txState.nodeDoCreate( 123 );
        lockingOps.nodeDelete( state, 123 );

        //THEN
        order.verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        order.verify( entityWriteOps ).nodeDelete( state, 123 );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeAddingIndexRule() throws Exception
    {
        // given
        LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 123, 456 );
        IndexDescriptor index = IndexDescriptorFactory.forLabel( 123, 456 );
        when( schemaWriteOps.indexCreate( state, descriptor ) ).thenReturn( index );

        // when
        IndexDescriptor result = lockingOps.indexCreate( state, descriptor );

        // then
        assertSame( index, result );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( schemaWriteOps ).indexCreate( state, descriptor );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeRemovingIndexRule() throws Exception
    {
        // given
        IndexDescriptor index = IndexDescriptorFactory.forLabel( 0, 0 );

        // when
        lockingOps.indexDrop( state, index );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, index.schema().getLabelId() );
        order.verify( schemaWriteOps ).indexDrop( state, index );
    }

    @Test
    public void acquireReadLockBeforeGettingIndexRules() throws Exception
    {
        // given
        int labelId = 1;
        IndexDescriptor labelDescriptor = IndexDescriptorFactory.forLabel( labelId, 2, 3 );

        Iterator<IndexDescriptor> rules = Iterators.iterator( labelDescriptor );
        when( schemaReadOps.indexesGetAll( state ) ).thenReturn( rules );

        // when
        Iterator<IndexDescriptor> result = lockingOps.indexesGetAll( state );
        Iterators.count( result );

        // then
        order.verify( schemaReadOps ).indexesGetAll( state );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeCreatingUniquenessConstraint() throws Exception
    {
        // given
        UniquenessConstraintDescriptor constraint = uniqueForSchema( descriptor );
        when( schemaWriteOps.uniquePropertyConstraintCreate( state, descriptor ) ).thenReturn( constraint );

        // when
        UniquenessConstraintDescriptor result = lockingOps.uniquePropertyConstraintCreate( state, descriptor );

        // then
        assertSame( constraint, result );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( schemaWriteOps ).uniquePropertyConstraintCreate( state, descriptor );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeDroppingConstraint() throws Exception
    {
        // given
        UniquenessConstraintDescriptor constraint = uniqueForSchema( descriptor );

        // when
        lockingOps.constraintDrop( state, constraint );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( schemaWriteOps ).constraintDrop( state, constraint );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeGettingConstraintsByLabelAndProperty() throws Exception
    {
        // given
        when( schemaReadOps.constraintsGetForSchema( state, descriptor ) ).thenReturn( emptyIterator() );

        // when
        Iterator<ConstraintDescriptor> result = lockingOps.constraintsGetForSchema( state, descriptor );

        // then
        assertThat( asList( result ), empty() );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( schemaReadOps ).constraintsGetForSchema( state, descriptor );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeGettingConstraintsByLabel() throws Exception
    {
        // given
        int labelId = 123;
        when( schemaReadOps.constraintsGetForLabel( state, labelId ) ).thenReturn( emptyIterator() );

        // when
        Iterator<ConstraintDescriptor> result = lockingOps.constraintsGetForLabel( state, labelId );

        // then
        assertThat( asList( result ), empty() );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( schemaReadOps ).constraintsGetForLabel( state, labelId );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeGettingAllConstraints() throws Exception
    {
        // given
        int labelId = 1;
        int relTypeId = 2;
        UniquenessConstraintDescriptor uniquenessConstraint = uniqueForLabel( labelId, 2, 3, 3 );
        RelExistenceConstraintDescriptor existenceConstraint = existsForRelType( relTypeId, 3, 4, 5 );

        when( schemaReadOps.constraintsGetAll( state ) )
                .thenReturn( Iterators.iterator( uniquenessConstraint, existenceConstraint ) );

        // when
        Iterator<ConstraintDescriptor> result = lockingOps.constraintsGetAll( state );
        Iterators.count( result );

        // then
        assertThat( asList( result ), empty() );
        order.verify( schemaReadOps ).constraintsGetAll( state );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP_TYPE, relTypeId );
    }

    @Test
    public void shouldAcquireEntityWriteLockCreatingRelationship() throws Exception
    {
        // when
        lockingOps.relationshipCreate( state, 1, 2, 3 );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 2 );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 3 );
        order.verify( entityWriteOps ).relationshipCreate( state, 1, 2, 3 );
    }

    @Test
    public void shouldAcquireNodeLocksWhenCreatingRelationshipInOrderOfAscendingId() throws Exception
    {
        // GIVEN
        long lowId = 3;
        long highId = 5;

        {
            // WHEN
            lockingOps.relationshipCreate( state, 0, lowId, highId );

            // THEN
            InOrder lockingOrder = inOrder( locks );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, lowId );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, highId );
            lockingOrder.verifyNoMoreInteractions();
            reset( locks );
        }

        {
            // WHEN
            lockingOps.relationshipCreate( state, 0, highId, lowId );

            // THEN
            InOrder lockingOrder = inOrder( locks );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, lowId );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, highId );
            lockingOrder.verifyNoMoreInteractions();
        }
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldAcquireNodeLocksWhenDeletingRelationshipInOrderOfAscendingId() throws Exception
    {
        // GIVEN
        final long relationshipId = 10;
        final long lowId = 3;
        final long highId = 5;

        {
            // and GIVEN
            doAnswer( invocation ->
            {
                RelationshipVisitor<RuntimeException> visitor =
                        (RelationshipVisitor<RuntimeException>) invocation.getArguments()[2];
                visitor.visit( relationshipId, 0, lowId, highId );
                return null;
            } ).when( entityReadOps ).relationshipVisit( any( KernelStatement.class ), anyLong(),
                    any( RelationshipVisitor.class ) );

            // WHEN
            lockingOps.relationshipDelete( state, relationshipId );

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
            doAnswer( invocation ->
            {
                RelationshipVisitor<RuntimeException> visitor =
                        (RelationshipVisitor<RuntimeException>) invocation.getArguments()[2];
                visitor.visit( relationshipId, 0, highId, lowId );
                return null;
            } ).when( entityReadOps ).relationshipVisit( any( KernelStatement.class ), anyLong(),
                    any( RelationshipVisitor.class ) );

            // WHEN
            lockingOps.relationshipDelete( state, relationshipId );

            // THEN
            InOrder lockingOrder = inOrder( locks );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, lowId );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, highId );
            lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, relationshipId );
            lockingOrder.verifyNoMoreInteractions();
        }
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeSettingPropertyOnRelationship() throws Exception
    {
        // given
        int propertyKeyId = 8;
        Value value = Values.of( 9 );

        // when
        lockingOps.relationshipSetProperty( state, 123, propertyKeyId, value );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 123 );
        order.verify( entityWriteOps ).relationshipSetProperty( state, 123, propertyKeyId, value );
    }

    @Test
    public void shouldNotAcquireEntityWriteLockBeforeSettingPropertyOnJustCreatedRelationship() throws Exception
    {
        // given
        txState.relationshipDoCreate( 123, 1, 2, 3 );
        int propertyKeyId = 8;
        Value value = Values.of( 9 );

        // when
        lockingOps.relationshipSetProperty( state, 123, propertyKeyId, value );

        // then
        order.verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 123 );
        order.verify( entityWriteOps ).relationshipSetProperty( state, 123, propertyKeyId, value );
    }

    @Test
    public void detachDeleteNodeWithoutRelationshipsExclusivelyLockNode() throws KernelException
    {
        long nodeId = 1L;
        returnRelationships( entityReadOps, state, nodeId, false );

        lockingOps.nodeDetachDelete( state, nodeId );

        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
        order.verify( locks, times( 0 ) ).releaseExclusive( ResourceTypes.NODE, nodeId );
        order.verify( entityWriteOps ).nodeDetachDelete( state, nodeId );
    }

    @Test
    public void detachDeleteNodeExclusivelyLockNodes() throws KernelException
    {
        long nodeId = 1L;
        RelationshipData relationship = new RelationshipData( 1, nodeId, 2L );
        returnRelationships( entityReadOps, state, nodeId, false, relationship );

        lockingOps.nodeDetachDelete( state, nodeId );

        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, relationship.startNodeId );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, relationship.endNodeId );
        order.verify( locks, times( 0 ) ).releaseExclusive( ResourceTypes.NODE, relationship.startNodeId );
        order.verify( locks, times( 0 ) ).releaseExclusive( ResourceTypes.NODE, relationship.endNodeId );
        order.verify( entityWriteOps ).nodeDetachDelete( state, nodeId );
    }

    private static class SimpleTxStateHolder implements TxStateHolder
    {
        private final TxState txState;

        private SimpleTxStateHolder( TxState txState )
        {
            this.txState = txState;
        }

        @Override
        public TransactionState txState()
        {
            return txState;
        }

        @Override
        public ExplicitIndexTransactionState explicitIndexTxState()
        {
            return null;
        }

        @Override
        public boolean hasTxStateWithChanges()
        {
            return txState.hasChanges();
        }
    }
}
