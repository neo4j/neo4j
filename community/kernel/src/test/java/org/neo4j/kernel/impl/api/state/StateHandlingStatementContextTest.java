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
package org.neo4j.kernel.impl.api.state;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.CompositeStatementContext;
import org.neo4j.kernel.impl.api.StateHandlingStatementContext;
import org.neo4j.kernel.impl.api.constraints.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.state.TxState.IdGeneration;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class StateHandlingStatementContextTest
{
    // Note: Most of the behavior of this class is tested in separate classes,
    // based on the category of state being
    // tested. This contains general tests or things that are common to all
    // types of state.

    @Test
    public void shouldNeverDelegateWrites() throws Exception
    {
        // Given
        StatementContext hatesWritesCtx = new CompositeStatementContext()
        {
            @Override
            protected void beforeWriteOperation()
            {
                throw new RuntimeException( "RAWR SO ANGRY, HOW DID YOU GET THIS NUMBER DONT EVER CALL ME AGAIN" );
            }

            @Override
            public boolean isLabelSetOnNode( long labelId, long nodeId )
            {
                return false;
            }
        };

        StateHandlingStatementContext ctx = new StateHandlingStatementContext( hatesWritesCtx,
                mock( SchemaStateOperations.class ), mock( TxState.class ), mock( ConstraintIndexCreator.class ) );

        // When
        ctx.addIndex( 0l, 0l );
        ctx.addLabelToNode( 0l, 0l );
        ctx.dropIndex( new IndexDescriptor( 0l, 0l ) );
        ctx.removeLabelFromNode( 0l, 0l );

        // These are kind of in between.. property key ids are created in
        // micro-transactions, so these methods
        // circumvent the normal state of affairs. We may want to rub the
        // genius-bumps over this at some point.
        // ctx.getOrCreateLabelId("0");
        // ctx.getOrCreatePropertyKeyId("0");

        // Then no exception should have been thrown
    }

    @Test
    public void shouldNotAddConstraintAlreadyExistsInTheStore() throws Exception
    {
        // given
        UniquenessConstraint constraint = new UniquenessConstraint( 10, 66 );
        StatementContext delegate = mock( StatementContext.class );
        when( delegate.getConstraints( 10, 66 ) ).thenAnswer( asAnswer( asList( constraint ) ) );
        TxState state = mock( TxState.class );
        StateHandlingStatementContext context = new StateHandlingStatementContext( delegate,
                mock( SchemaStateOperations.class ), state, mock( ConstraintIndexCreator.class ) );

        // when
        context.addUniquenessConstraint( 10, 66 );

        // then
        verify( state ).unRemoveConstraint( any( UniquenessConstraint.class ) );
        verifyNoMoreInteractions( state );
    }

    @Test
    public void shouldGetConstraintsByLabelAndProperty() throws Exception
    {
        // given
        UniquenessConstraint constraint = new UniquenessConstraint( 10, 66 );

        StatementContext delegate = mock( StatementContext.class );
        when( delegate.getConstraints( 10, 66 ) ).thenAnswer( asAnswer( Collections.emptyList() ) );
        TxState state = new TxState( mock( OldTxStateBridge.class ), mock( PersistenceManager.class ),
                mock( IdGeneration.class ) );
        StateHandlingStatementContext context = new StateHandlingStatementContext( delegate,
                mock( SchemaStateOperations.class ), state, mock( ConstraintIndexCreator.class ) );
        context.addUniquenessConstraint( 10, 66 );

        // when
        Set<UniquenessConstraint> result = asSet( asIterable( context.getConstraints( 10, 66 ) ) );

        // then
        assertEquals( asSet( constraint ), result );
    }

    @Test
    public void shouldGetConstraintsByLabel() throws Exception
    {
        // given
        UniquenessConstraint constraint1 = new UniquenessConstraint( 11, 66 );
        UniquenessConstraint constraint2 = new UniquenessConstraint( 11, 99 );

        StatementContext delegate = mock( StatementContext.class );
        when( delegate.getConstraints( 10, 66 ) ).thenAnswer( asAnswer( Collections.emptyList() ) );
        when( delegate.getConstraints( 11, 99 ) ).thenAnswer( asAnswer( Collections.emptyList() ) );
        when( delegate.getConstraints( 10 ) ).thenAnswer( asAnswer( Collections.emptyList() ) );
        when( delegate.getConstraints( 11 ) ).thenAnswer( asAnswer( asIterable( constraint1 ) ) );
        TxState state = new TxState( mock( OldTxStateBridge.class ), mock( PersistenceManager.class ),
                mock( IdGeneration.class ) );
        StateHandlingStatementContext context = new StateHandlingStatementContext( delegate,
                mock( SchemaStateOperations.class ), state, mock( ConstraintIndexCreator.class ) );
        context.addUniquenessConstraint( 10, 66 );
        context.addUniquenessConstraint( 11, 99 );

        // when
        Set<UniquenessConstraint> result = asSet( asIterable( context.getConstraints( 11 ) ) );

        // then
        assertEquals( asSet( constraint1, constraint2 ), result );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldGetAllConstraints() throws Exception
    {
        // given
        UniquenessConstraint constraint1 = new UniquenessConstraint( 10, 66 );
        UniquenessConstraint constraint2 = new UniquenessConstraint( 11, 99 );

        StatementContext delegate = mock( StatementContext.class );
        when( delegate.getConstraints( 10, 66 ) ).thenAnswer( asAnswer( Collections.emptyList() ) );
        when( delegate.getConstraints( 11, 99 ) ).thenAnswer( asAnswer( Collections.emptyList() ) );
        when( delegate.getConstraints() ).thenAnswer( asAnswer( asIterable( constraint2 ) ) );
        TxState state = new TxState( mock( OldTxStateBridge.class ), mock( PersistenceManager.class ),
                mock( IdGeneration.class ) );
        StateHandlingStatementContext context = new StateHandlingStatementContext( delegate,
                mock( SchemaStateOperations.class ), state, mock( ConstraintIndexCreator.class ) );
        context.addUniquenessConstraint( 10, 66 );

        // when
        Set<UniquenessConstraint> result = asSet( asIterable( context.getConstraints() ) );

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
}
