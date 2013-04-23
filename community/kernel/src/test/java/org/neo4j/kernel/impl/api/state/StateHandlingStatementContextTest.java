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

import java.util.Iterator;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.operations.SchemaOperations;
import org.neo4j.kernel.impl.api.CompositeStatementContext;
import org.neo4j.kernel.impl.api.StateHandlingStatementContext;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;

public class StateHandlingStatementContextTest
{
    // Note: Most of the behavior of this class is tested in separate classes, based on the category of state being
    // tested. This contains general tests or things that are common to all types of state.

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
                mock( SchemaOperations.class ), mock( TxState.class ) );

        // When
        ctx.addIndexRule( 0l, 0l );
        ctx.addLabelToNode( 0l, 0l );
        ctx.dropIndexRule( new IndexRule( 0l, 0l, PROVIDER_DESCRIPTOR, 0l ) );
        ctx.removeLabelFromNode( 0l, 0l );

        // These are kind of in between.. property key ids are created in micro-transactions, so these methods
        // circumvent the normal state of affairs. We may want to rub the genius-bumps over this at some point.
        //   ctx.getOrCreateLabelId("0");
        //   ctx.getOrCreatePropertyKeyId("0");

        // Then no exception should have been thrown
    }

    @Test
    public void shouldNotAddConstraintAlreadyExistsInTheStore() throws Exception
    {
        // given
        StatementContext delegate = mock( StatementContext.class );
        when( delegate.getConstraints( 10, 66 ) )
                .thenAnswer( asAnswer( asList( new UniquenessConstraint( 10, 66 ) ) ) );
        TxState state = mock( TxState.class );
        StateHandlingStatementContext context = new StateHandlingStatementContext(
                delegate, mock( SchemaOperations.class ), state );

        // when
        context.addUniquenessConstraint( 10, 66 );

        // then
        verify( state ).addConstraint( new UniquenessConstraint( 10, 66 ), false );
        verifyNoMoreInteractions( state );
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
