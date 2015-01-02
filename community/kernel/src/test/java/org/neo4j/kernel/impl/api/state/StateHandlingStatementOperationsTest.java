/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.state;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.LegacyPropertyTrackers;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.state.TxState.IdGeneration;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.util.DiffSets;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.impl.api.StatementOperationsTestHelper.mockedState;

public class StateHandlingStatementOperationsTest
{
    // Note: Most of the behavior of this class is tested in separate classes,
    // based on the category of state being
    // tested. This contains general tests or things that are common to all
    // types of state.

    StoreReadLayer inner = mock( StoreReadLayer.class );

    @Test
    public void shouldNeverDelegateWrites() throws Exception
    {
        KernelStatement state = mockedState();
        StateHandlingStatementOperations ctx = newTxStateOps( inner );

        // When
        ctx.indexCreate( state, 0, 0 );
        ctx.nodeAddLabel( state, 0, 0 );
        ctx.indexDrop( state, new IndexDescriptor( 0, 0 ) );
        ctx.nodeRemoveLabel( state, 0, 0 );

        // These are kind of in between.. property key ids are created in
        // micro-transactions, so these methods
        // circumvent the normal state of affairs. We may want to rub the
        // genius-bumps over this at some point.
        // ctx.getOrCreateLabelId("0");
        // ctx.getOrCreatePropertyKeyId("0");

        verify( inner, times( 2 ) ).nodeHasLabel( state, 0, 0 );
        verifyNoMoreInteractions( inner );
    }

    @Test
    public void shouldNotAddConstraintAlreadyExistsInTheStore() throws Exception
    {
        // given
        UniquenessConstraint constraint = new UniquenessConstraint( 10, 66 );
        TxState txState = mock( TxState.class );
        when( txState.nodesWithLabelChanged( anyInt() ) ).thenReturn( DiffSets.<Long>emptyDiffSets() );
        when( txState.nodesWithChangedProperty( anyInt() ) ).thenReturn( Collections.<Long, Object>emptyMap() );
        KernelStatement state = mockedState( txState );
        when( inner.constraintsGetForLabelAndPropertyKey( state, 10, 66 ) )
            .thenAnswer( asAnswer( asList( constraint ) ) );
        StateHandlingStatementOperations context = newTxStateOps( inner );

        // when
        context.uniquenessConstraintCreate( state, 10, 66 );

        // then
        verify( txState ).constraintIndexDoUnRemove( any( IndexDescriptor.class ) );
    }

    @Test
    public void shouldGetConstraintsByLabelAndProperty() throws Exception
    {
        // given
        UniquenessConstraint constraint = new UniquenessConstraint( 10, 66 );
        TxState txState = new TxStateImpl( mock( OldTxStateBridge.class ), mock( PersistenceManager.class ),
                mock( IdGeneration.class ) );
        KernelStatement state = mockedState( txState );
        when( inner.constraintsGetForLabelAndPropertyKey( state, 10, 66 ) )
            .thenAnswer( asAnswer( Collections.emptyList() ) );
        StateHandlingStatementOperations context = newTxStateOps( inner );
        context.uniquenessConstraintCreate( state, 10, 66 );

        // when
        Set<UniquenessConstraint> result = asSet(
                asIterable( context.constraintsGetForLabelAndPropertyKey( state, 10, 66 ) ) );

        // then
        assertEquals( asSet( constraint ), result );
    }

    @Test
    public void shouldGetConstraintsByLabel() throws Exception
    {
        // given
        UniquenessConstraint constraint1 = new UniquenessConstraint( 11, 66 );
        UniquenessConstraint constraint2 = new UniquenessConstraint( 11, 99 );

        TxState txState = new TxStateImpl( mock( OldTxStateBridge.class ), mock( PersistenceManager.class ),
                mock( IdGeneration.class ) );
        KernelStatement state = mockedState( txState );
        when( inner.constraintsGetForLabelAndPropertyKey( state, 10, 66 ) )
            .thenAnswer( asAnswer( Collections.emptyList() ) );
        when( inner.constraintsGetForLabelAndPropertyKey( state, 11, 99 ) )
            .thenAnswer( asAnswer( Collections.emptyList() ) );
        when( inner.constraintsGetForLabel( state, 10 ) )
            .thenAnswer( asAnswer( Collections.emptyList() ) );
        when( inner.constraintsGetForLabel( state, 11 ) )
            .thenAnswer( asAnswer( asIterable( constraint1 ) ) );
        StateHandlingStatementOperations context = newTxStateOps( inner );
        context.uniquenessConstraintCreate( state, 10, 66 );
        context.uniquenessConstraintCreate( state, 11, 99 );

        // when
        Set<UniquenessConstraint> result = asSet( asIterable( context.constraintsGetForLabel( state, 11 ) ) );

        // then
        assertEquals( asSet( constraint1, constraint2 ), result );
    }

    @Test
    public void shouldGetAllConstraints() throws Exception
    {
        // given
        UniquenessConstraint constraint1 = new UniquenessConstraint( 10, 66 );
        UniquenessConstraint constraint2 = new UniquenessConstraint( 11, 99 );

        TxState txState = new TxStateImpl( mock( OldTxStateBridge.class ), mock( PersistenceManager.class ),
                mock( IdGeneration.class ) );
        KernelStatement state = mockedState( txState );
        when( inner.constraintsGetForLabelAndPropertyKey( state, 10, 66 ) )
            .thenAnswer( asAnswer( Collections.emptyList() ) );
        when( inner.constraintsGetForLabelAndPropertyKey( state, 11, 99 ) )
            .thenAnswer( asAnswer( Collections.emptyList() ) );
        when( inner.constraintsGetAll( state ) ).thenAnswer( asAnswer( asIterable( constraint2 ) ) );
        StateHandlingStatementOperations context = newTxStateOps( inner );
        context.uniquenessConstraintCreate( state, 10, 66 );

        // when
        Set<UniquenessConstraint> result = asSet( asIterable( context.constraintsGetAll( state ) ) );

        // then
        assertEquals( asSet( constraint1, constraint2 ), result );
    }

    private static <T> Answer<Iterator<T>> asAnswer( final Iterable<T> values )
    {
        return new Answer<Iterator<T>>()
        {
            @Override
            public Iterator<T> answer( InvocationOnMock invocation ) throws Throwable
            {
                return values.iterator();
            }
        };
    }

    private StateHandlingStatementOperations newTxStateOps( StoreReadLayer delegate )
    {
        return new StateHandlingStatementOperations( delegate,
                mock( LegacyPropertyTrackers.class ), mock( ConstraintIndexCreator.class ) );
    }
}
