/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.Iterator;

import org.neo4j.function.Function;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;

import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.neo4j.function.Functions.constant;
import static org.neo4j.kernel.impl.locking.ResourceTypes.schemaResource;

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
    private final KernelStatement state = new KernelStatement( transaction, null, null, null, locks, null, null );
    private final SchemaStateOperations schemaStateOps;

    public LockingStatementOperationsTest()
    {
        entityReadOps = mock( EntityReadOperations.class );
        entityWriteOps = mock( EntityWriteOperations.class );
        schemaReadOps = mock( SchemaReadOperations.class );
        schemaWriteOps = mock( SchemaWriteOperations.class );
        schemaStateOps = mock( SchemaStateOperations.class );
        order = inOrder( locks, entityWriteOps, schemaReadOps, schemaWriteOps, schemaStateOps );
        lockingOps = new LockingStatementOperations(
                entityReadOps, entityWriteOps, schemaReadOps, schemaWriteOps, schemaStateOps
        );
    }

    @Test
    public void shouldAcquireEntityWriteLockCreatingRelationship() throws Exception
    {
        // when
        lockingOps.relationshipCreate( state, 1, 2, 3 );

        // then
        order.verify( locks ).acquireExclusive( ResourceTypes.NODE, 2 );
        order.verify( locks ).acquireExclusive( ResourceTypes.NODE, 3 );
        order.verify( entityWriteOps ).relationshipCreate( state, 1, 2, 3 );
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeAddingLabelToNode() throws Exception
    {
        // when
        lockingOps.nodeAddLabel( state, 123, 456 );

        // then
        order.verify( locks ).acquireExclusive( ResourceTypes.NODE, 123 );
        order.verify( entityWriteOps ).nodeAddLabel( state, 123, 456 );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeAddingLabelToNode() throws Exception
    {
        // when
        lockingOps.nodeAddLabel( state, 123, 456 );

        // then
        order.verify( locks ).acquireShared( ResourceTypes.SCHEMA, schemaResource() );
        order.verify( entityWriteOps ).nodeAddLabel( state, 123, 456 );
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeSettingPropertyOnNode() throws Exception
    {
        // given
        DefinedProperty property = Property.property( 8, 9 );

        // when
        lockingOps.nodeSetProperty( state, 123, property );

        // then
        order.verify( locks ).acquireExclusive( ResourceTypes.NODE, 123 );
        order.verify( entityWriteOps ).nodeSetProperty( state, 123, property );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeSettingPropertyOnNode() throws Exception
    {
        // given
        DefinedProperty property = Property.property( 8, 9 );

        // when
        lockingOps.nodeSetProperty( state, 123, property );

        // then
        order.verify( locks ).acquireShared( ResourceTypes.SCHEMA, schemaResource() );
        order.verify( entityWriteOps ).nodeSetProperty( state, 123, property );
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeDeletingNode() throws EntityNotFoundException
    {
        // WHEN
        lockingOps.nodeDelete( state, 123 );

        //THEN
        order.verify( locks ).acquireExclusive( ResourceTypes.NODE, 123 );
        order.verify( entityWriteOps ).nodeDelete( state, 123 );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeAddingIndexRule() throws Exception
    {
        // given
        IndexDescriptor rule = mock( IndexDescriptor.class );
        when( schemaWriteOps.indexCreate( state, 123, 456 ) ).thenReturn( rule );

        // when
        IndexDescriptor result = lockingOps.indexCreate( state, 123, 456 );

        // then
        assertSame( rule, result );
        order.verify( locks ).acquireExclusive( ResourceTypes.SCHEMA, schemaResource() );
        order.verify( schemaWriteOps ).indexCreate( state, 123, 456 );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeRemovingIndexRule() throws Exception
    {
        // given
        IndexDescriptor rule = new IndexDescriptor( 0, 0 );

        // when
        lockingOps.indexDrop( state, rule );

        // then
        order.verify( locks ).acquireExclusive( ResourceTypes.SCHEMA, schemaResource() );
        order.verify( schemaWriteOps ).indexDrop( state, rule );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeGettingIndexRules() throws Exception
    {
        // given
        Iterator<IndexDescriptor> rules = Collections.emptyIterator();
        when( schemaReadOps.indexesGetAll( state ) ).thenReturn( rules );

        // when
        Iterator<IndexDescriptor> result = lockingOps.indexesGetAll( state );

        // then
        assertSame( rules, result );
        order.verify( locks ).acquireShared( ResourceTypes.SCHEMA, schemaResource() );
        order.verify( schemaReadOps ).indexesGetAll( state );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeCreatingUniquenessConstraint() throws Exception
    {
        // given
        UniquenessConstraint constraint = new UniquenessConstraint( 0, 0 );
        when( schemaWriteOps.uniquePropertyConstraintCreate( state, 123, 456 ) ).thenReturn( constraint );

        // when
        PropertyConstraint result = lockingOps.uniquePropertyConstraintCreate( state, 123, 456 );

        // then
        assertSame( constraint, result );
        order.verify( locks ).acquireExclusive( ResourceTypes.SCHEMA, schemaResource() );
        order.verify( schemaWriteOps ).uniquePropertyConstraintCreate( state, 123, 456 );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeDroppingConstraint() throws Exception
    {
        // given
        UniquenessConstraint constraint = new UniquenessConstraint( 1, 2 );

        // when
        lockingOps.constraintDrop( state, constraint );

        // then
        order.verify( locks ).acquireExclusive( ResourceTypes.SCHEMA, schemaResource() );
        order.verify( schemaWriteOps ).constraintDrop( state, constraint );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeGettingConstraintsByLabelAndProperty() throws Exception
    {
        // given
        Iterator<NodePropertyConstraint> constraints = Collections.emptyIterator();
        when( schemaReadOps.constraintsGetForLabelAndPropertyKey( state, 123, 456 ) ).thenReturn( constraints );

        // when
        Iterator<NodePropertyConstraint> result = lockingOps.constraintsGetForLabelAndPropertyKey( state, 123, 456 );

        // then
        assertSame( constraints, result );
        order.verify( locks ).acquireShared( ResourceTypes.SCHEMA, schemaResource() );
        order.verify( schemaReadOps ).constraintsGetForLabelAndPropertyKey( state, 123, 456 );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeGettingConstraintsByLabel() throws Exception
    {
        // given
        Iterator<NodePropertyConstraint> constraints = Collections.emptyIterator();
        when( schemaReadOps.constraintsGetForLabel( state, 123 ) ).thenReturn( constraints );

        // when
        Iterator<NodePropertyConstraint> result = lockingOps.constraintsGetForLabel( state, 123 );

        // then
        assertSame( constraints, result );
        order.verify( locks ).acquireShared( ResourceTypes.SCHEMA, schemaResource() );
        order.verify( schemaReadOps ).constraintsGetForLabel( state, 123 );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeGettingAllConstraints() throws Exception
    {
        // given
        Iterator<PropertyConstraint> constraints = Collections.emptyIterator();
        when( schemaReadOps.constraintsGetAll( state ) ).thenReturn( constraints );

        // when
        Iterator<PropertyConstraint> result = lockingOps.constraintsGetAll( state );

        // then
        assertSame( constraints, result );
        order.verify( locks ).acquireShared( ResourceTypes.SCHEMA, schemaResource() );
        order.verify( schemaReadOps ).constraintsGetAll( state );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeUpdatingSchemaState() throws Exception
    {
        // given
        Function<Object,Object> creator = constant( null );

        // when
        lockingOps.schemaStateGetOrCreate( state, null, creator );

        // then
        order.verify( locks ).acquireShared( ResourceTypes.SCHEMA, schemaResource() );
        order.verify( schemaStateOps ).schemaStateGetOrCreate( state, null, creator );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeCheckingSchemaState() throws Exception
    {
        // when
        lockingOps.schemaStateContains( state, null );

        // then
        order.verify( locks ).acquireShared( ResourceTypes.SCHEMA, schemaResource() );
        order.verify( schemaStateOps ).schemaStateContains( state, null );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeFlushingSchemaState() throws Exception
    {
        // when
        lockingOps.schemaStateFlush( state );

        // then
        order.verify( locks ).acquireShared( ResourceTypes.SCHEMA, schemaResource() );
        order.verify( schemaStateOps ).schemaStateFlush( state );
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
            lockingOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, lowId );
            lockingOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, highId );
            lockingOrder.verifyNoMoreInteractions();
            reset( locks );
        }

        {
            // WHEN
            lockingOps.relationshipCreate( state, 0, highId, lowId );

            // THEN
            InOrder lockingOrder = inOrder( locks );
            lockingOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, lowId );
            lockingOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, highId );
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
            doAnswer( new Answer<Void>()
            {
                @Override
                public Void answer( InvocationOnMock invocation ) throws Throwable
                {
                    RelationshipVisitor<RuntimeException> visitor =
                            (RelationshipVisitor<RuntimeException>) invocation.getArguments()[2];
                    visitor.visit( relationshipId, 0, lowId, highId );
                    return null;
                }
            } ).when( entityReadOps ).relationshipVisit( any( KernelStatement.class ), anyLong(),
                    any( RelationshipVisitor.class ) );

            // WHEN
            lockingOps.relationshipDelete( state, relationshipId );

            // THEN
            InOrder lockingOrder = inOrder( locks );
            lockingOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, lowId );
            lockingOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, highId );
            lockingOrder.verify( locks ).acquireExclusive( ResourceTypes.RELATIONSHIP, relationshipId );
            lockingOrder.verifyNoMoreInteractions();
            reset( locks );
        }

        {
            // and GIVEN
            doAnswer( new Answer<Void>()
            {
                @Override
                public Void answer( InvocationOnMock invocation ) throws Throwable
                {
                    RelationshipVisitor<RuntimeException> visitor =
                            (RelationshipVisitor<RuntimeException>) invocation.getArguments()[2];
                    visitor.visit( relationshipId, 0, highId, lowId );
                    return null;
                }
            } ).when( entityReadOps ).relationshipVisit( any( KernelStatement.class ), anyLong(),
                    any( RelationshipVisitor.class ) );

            // WHEN
            lockingOps.relationshipDelete( state, relationshipId );

            // THEN
            InOrder lockingOrder = inOrder( locks );
            lockingOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, lowId );
            lockingOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, highId );
            lockingOrder.verify( locks ).acquireExclusive( ResourceTypes.RELATIONSHIP, relationshipId );
            lockingOrder.verifyNoMoreInteractions();
        }
    }
}
