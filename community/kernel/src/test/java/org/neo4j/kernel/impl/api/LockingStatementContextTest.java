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

import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
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

public class LockingStatementContextTest
{
    @Test
    public void shouldGrabWriteLocksBeforeDeleting() throws Exception
    {
        // GIVEN
        StatementContext inner = mock( StatementContext.class );
        LockHolder lockHolder = mock( LockHolder.class );
        NodeProxy.NodeLookup lookup = mock( NodeProxy.NodeLookup.class );
        NodeImpl node = mock( NodeImpl.class );
        int nodeId = 0;

        when( lookup.lookup( anyLong(), any( LockType.class ) ) ).thenReturn( node );

        LockingStatementContext statementContext = new LockingStatementContext( inner, lockHolder );

        // WHEN
        statementContext.nodeDelete( nodeId );

        //THEN
//        verify( inner ).deleteNode( null, nodeId );
        verify( lockHolder ).acquireNodeWriteLock( nodeId );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeAddingIndexRule() throws Exception
    {
        // given
        StatementContext delegate = mock( StatementContext.class );
        LockHolder lockHolder = mock( LockHolder.class );
        IndexDescriptor rule = mock( IndexDescriptor.class );
        when( delegate.indexCreate( 123, 456 ) ).thenReturn( rule );

        LockingStatementContext context = new LockingStatementContext( delegate, lockHolder );

        // when
        IndexDescriptor result = context.indexCreate( 123, 456 );

        // then
        assertSame( rule, result );
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaWriteLock();
        order.verify( delegate ).indexCreate( 123, 456 );
        verifyNoMoreInteractions( lockHolder, delegate );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeRemovingIndexRule() throws Exception
    {
        // given
        StatementContext delegate = mock( StatementContext.class );
        LockHolder lockHolder = mock( LockHolder.class );
        IndexDescriptor rule = mock( IndexDescriptor.class );

        LockingStatementContext context = new LockingStatementContext( delegate, lockHolder );

        // when
        context.indexDrop( rule );

        // then
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaWriteLock();
        order.verify( delegate ).indexDrop( rule );
        verifyNoMoreInteractions( lockHolder, delegate );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeRetrievingIndexRule() throws Exception
    {
        // given
        StatementContext delegate = mock( StatementContext.class );
        LockHolder lockHolder = mock( LockHolder.class );
        @SuppressWarnings("unchecked")
        Iterator<IndexDescriptor> rules = mock( Iterator.class );
        when( delegate.indexesGetAll() ).thenReturn( rules );

        LockingStatementContext context = new LockingStatementContext( delegate, lockHolder );

        // when
        Iterator<IndexDescriptor> result = context.indexesGetAll();

        // then
        assertSame( rules, result );
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaReadLock();
        order.verify( delegate ).indexesGetAll();
        verifyNoMoreInteractions( lockHolder, delegate );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeAddingUniquenessConstraint() throws Exception
    {
        // given
        StatementContext delegate = mock( StatementContext.class );
        LockHolder lockHolder = mock( LockHolder.class );
        UniquenessConstraint constraint = mock( UniquenessConstraint.class );
        when( delegate.uniquenessConstraintCreate( 123, 456 ) ).thenReturn( constraint );

        LockingStatementContext context = new LockingStatementContext( delegate, lockHolder );

        // when
        UniquenessConstraint result = context.uniquenessConstraintCreate( 123, 456 );

        // then
        assertEquals( constraint, result );
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaWriteLock();
        order.verify( delegate ).uniquenessConstraintCreate( 123, 456 );
        verifyNoMoreInteractions( lockHolder, delegate );
    }

    @Test
    public void shouldAcquireSchemaWriteLockBeforeDroppingConstraint() throws Exception
    {
        // given
        StatementContext delegate = mock( StatementContext.class );
        LockHolder lockHolder = mock( LockHolder.class );
        UniquenessConstraint constraint = mock( UniquenessConstraint.class );

        LockingStatementContext context = new LockingStatementContext( delegate, lockHolder );

        // when
        context.constraintDrop( constraint );

        // then
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaWriteLock();
        order.verify( delegate ).constraintDrop( constraint );
        verifyNoMoreInteractions( lockHolder, delegate );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeRetrievingConstraintsByLabelAndProperty() throws Exception
    {
        // given
        StatementContext delegate = mock( StatementContext.class );
        LockHolder lockHolder = mock( LockHolder.class );
        @SuppressWarnings("unchecked")
        Iterator<UniquenessConstraint> constraints = mock( Iterator.class );
        when( delegate.constraintsGetForLabelAndPropertyKey( 123, 456 ) ).thenReturn( constraints );

        LockingStatementContext context = new LockingStatementContext( delegate, lockHolder );

        // when
        Iterator<UniquenessConstraint> result = context.constraintsGetForLabelAndPropertyKey( 123, 456 );

        // then
        assertEquals( constraints, result );
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaReadLock();
        order.verify( delegate ).constraintsGetForLabelAndPropertyKey( 123, 456 );
        verifyNoMoreInteractions( lockHolder, delegate );
        
        // cleanup
        context.close();
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeRetrievingConstraintsByLabel() throws Exception
    {
        // given
        StatementContext delegate = mock( StatementContext.class );
        LockHolder lockHolder = mock( LockHolder.class );
        @SuppressWarnings("unchecked")
        Iterator<UniquenessConstraint> constraints = mock( Iterator.class );
        when( delegate.constraintsGetForLabel( 123 ) ).thenReturn( constraints );

        LockingStatementContext context = new LockingStatementContext( delegate, lockHolder );

        // when
        Iterator<UniquenessConstraint> result = context.constraintsGetForLabel( 123 );

        // then
        assertEquals( constraints, result );
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaReadLock();
        order.verify( delegate ).constraintsGetForLabel( 123 );
        verifyNoMoreInteractions( lockHolder, delegate );
        
        // cleanup
        context.close();
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeRetrievingAllConstraintsl() throws Exception
    {
        // given
        StatementContext delegate = mock( StatementContext.class );
        LockHolder lockHolder = mock( LockHolder.class );
        @SuppressWarnings("unchecked")
        Iterator<UniquenessConstraint> constraints = mock( Iterator.class );
        when( delegate.constraintsGetAll() ).thenReturn( constraints );

        LockingStatementContext context = new LockingStatementContext( delegate, lockHolder );

        // when
        Iterator<UniquenessConstraint> result = context.constraintsGetAll();

        // then
        assertEquals( constraints, result );
        InOrder order = inOrder( lockHolder, delegate );
        order.verify( lockHolder ).acquireSchemaReadLock();
        order.verify( delegate ).constraintsGetAll();
        verifyNoMoreInteractions( lockHolder, delegate );
        
        // cleanup
        context.close();
    }
}
