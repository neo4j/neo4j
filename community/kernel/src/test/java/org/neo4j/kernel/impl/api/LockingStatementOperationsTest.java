/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.operations.EntityWriteOperations;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.operations.SchemaReadOperations;
import org.neo4j.kernel.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.transaction.LockType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class LockingStatementOperationsTest
{
    @Test
    public void shouldGrabWriteLocksBeforeDeleting() throws Exception
    {
        // GIVEN
        EntityWriteOperations inner = mock( EntityWriteOperations.class );
        NodeProxy.NodeLookup lookup = mock( NodeProxy.NodeLookup.class );
        NodeImpl node = mock( NodeImpl.class );
        int nodeId = 0;

        when( lookup.lookup( anyLong(), any( LockType.class ) ) ).thenReturn( node );

        LockingStatementOperations statementContext =
                new LockingStatementOperations( inner, null, null, null );

        // WHEN
        statementContext.nodeDelete( state, nodeId );

        //THEN
        verify( lockHolder ).acquireNodeWriteLock( nodeId );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeAddingIndexRule() throws Exception
    {
        // given
        SchemaWriteOperations delegate = mock( SchemaWriteOperations.class );
        IndexDescriptor rule = mock( IndexDescriptor.class );
        when( delegate.indexCreate( state, 123, 456 ) ).thenReturn( rule );

        LockingStatementOperations context = new LockingStatementOperations( null, null, delegate, null );

        // when
        IndexDescriptor result = context.indexCreate( state, 123, 456 );

        // then
        assertSame( rule, result );
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaWriteLock();
        order.verify( delegate ).indexCreate( state, 123, 456 );
        verifyNoMoreInteractions( lockHolder, delegate );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeRemovingIndexRule() throws Exception
    {
        // given
        SchemaWriteOperations delegate = mock( SchemaWriteOperations.class );
        IndexDescriptor rule = mock( IndexDescriptor.class );

        LockingStatementOperations context = new LockingStatementOperations( null, null, delegate, null );

        // when
        context.indexDrop( state, rule );

        // then
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaWriteLock();
        order.verify( delegate ).indexDrop( state, rule );
        verifyNoMoreInteractions( lockHolder, delegate );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeRetrievingIndexRule() throws Exception
    {
        // given
        SchemaReadOperations delegate = mock( SchemaReadOperations.class );
        @SuppressWarnings("unchecked")
        Iterator<IndexDescriptor> rules = mock( Iterator.class );
        when( delegate.indexesGetAll( state ) ).thenReturn( rules );

        LockingStatementOperations context = new LockingStatementOperations( null, delegate, null, null );

        // when
        Iterator<IndexDescriptor> result = context.indexesGetAll( state );

        // then
        assertSame( rules, result );
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaReadLock();
        order.verify( delegate ).indexesGetAll( state );
        verifyNoMoreInteractions( lockHolder, delegate );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeAddingUniquenessConstraint() throws Exception
    {
        // given
        SchemaWriteOperations delegate = mock( SchemaWriteOperations.class );
        UniquenessConstraint constraint = mock( UniquenessConstraint.class );
        when( delegate.uniquenessConstraintCreate( state, 123, 456 ) ).thenReturn( constraint );

        LockingStatementOperations context = new LockingStatementOperations( null, null, delegate, null );

        // when
        UniquenessConstraint result = context.uniquenessConstraintCreate( state, 123, 456 );

        // then
        assertEquals( constraint, result );
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaWriteLock();
        order.verify( delegate ).uniquenessConstraintCreate( state, 123, 456 );
        verifyNoMoreInteractions( lockHolder, delegate );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeDroppingConstraint() throws Exception
    {
        // given
        SchemaWriteOperations delegate = mock( SchemaWriteOperations.class );
        UniquenessConstraint constraint = mock( UniquenessConstraint.class );

        LockingStatementOperations context = new LockingStatementOperations( null, null, delegate, null );

        // when
        context.constraintDrop( state, constraint );

        // then
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaWriteLock();
        order.verify( delegate ).constraintDrop( state, constraint );
        verifyNoMoreInteractions( lockHolder, delegate );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeRetrievingConstraintsByLabelAndProperty() throws Exception
    {
        // given
        SchemaReadOperations delegate = mock( SchemaReadOperations.class );
        @SuppressWarnings("unchecked")
        Iterator<UniquenessConstraint> constraints = mock( Iterator.class );
        when( delegate.constraintsGetForLabelAndPropertyKey( state, 123, 456 ) ).thenReturn( constraints );

        LockingStatementOperations context = new LockingStatementOperations( null, delegate, null, null );

        // when
        Iterator<UniquenessConstraint> result = context.constraintsGetForLabelAndPropertyKey( state, 123, 456 );

        // then
        assertEquals( constraints, result );
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaReadLock();
        order.verify( delegate ).constraintsGetForLabelAndPropertyKey( state, 123, 456 );
        verifyNoMoreInteractions( lockHolder, delegate );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeRetrievingConstraintsByLabel() throws Exception
    {
        // given
        SchemaReadOperations delegate = mock( SchemaReadOperations.class );
        @SuppressWarnings("unchecked")
        Iterator<UniquenessConstraint> constraints = mock( Iterator.class );
        when( delegate.constraintsGetForLabel( state, 123 ) ).thenReturn( constraints );

        LockingStatementOperations context = new LockingStatementOperations( null, delegate, null, null );

        // when
        Iterator<UniquenessConstraint> result = context.constraintsGetForLabel( state, 123 );

        // then
        assertEquals( constraints, result );
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaReadLock();
        order.verify( delegate ).constraintsGetForLabel( state, 123 );
        verifyNoMoreInteractions( lockHolder, delegate );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeRetrievingAllConstraintsl() throws Exception
    {
        // given
        SchemaReadOperations delegate = mock( SchemaReadOperations.class );
        @SuppressWarnings("unchecked")
        Iterator<UniquenessConstraint> constraints = mock( Iterator.class );
        when( delegate.constraintsGetAll( state ) ).thenReturn( constraints );

        LockingStatementOperations context = new LockingStatementOperations( null, delegate, null, null );

        // when
        Iterator<UniquenessConstraint> result = context.constraintsGetAll( state );

        // then
        assertEquals( constraints, result );
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaReadLock();
        order.verify( delegate ).constraintsGetAll( state );
        verifyNoMoreInteractions( lockHolder, delegate );
    }

    private final LockHolder lockHolder = mock( LockHolder.class );
    private final StatementState state = mock( StatementState.class );
    
    @Before
    public void before() throws Exception
    {
        when( state.locks() ).thenReturn( lockHolder );
    }
}
