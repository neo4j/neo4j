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

import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asIterator;

import java.util.Iterator;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class ConstraintEvaluatingStatementContextTest
{
    @Test
    public void shouldDisallowReAddingExistingSchemaRules() throws Exception
    {
        // GIVEN
        long id = 3, label = 0, propertyKey = 7;
        IndexRule rule = new IndexRule( id, label, propertyKey );
        StatementContext inner = Mockito.mock(StatementContext.class);
        ConstraintEvaluatingStatementContext ctx = new ConstraintEvaluatingStatementContext( inner );
        when( inner.getIndexRules( rule.getLabel() ) ).thenAnswer( withIterator( rule ) );

        // WHEN
        try
        {
            ctx.addIndexRule( label, propertyKey );
            fail( "Should have thrown exception." );
        }
        catch ( ConstraintViolationKernelException e )
        {
        }

        // THEN
        verify( inner, never() ).addIndexRule( anyLong(), anyLong() );
    }

    private static <T> Answer<Iterator<T>> withIterator( final T... content )
    {
        return new Answer<Iterator<T>>()
        {
            @Override
            public Iterator<T> answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                return asIterator( content );
            }
        };
    }

    @Test(expected = ConstraintViolationKernelException.class)
    public void shouldFailInvalidLabelNames() throws Exception
    {
        // Given
        ConstraintEvaluatingStatementContext ctx = new ConstraintEvaluatingStatementContext( null );

        // When
        ctx.getOrCreateLabelId( "" );
    }

    @Test(expected = ConstraintViolationKernelException.class)
    public void shouldFailOnNullLabel() throws Exception
    {
        // Given
        ConstraintEvaluatingStatementContext ctx = new ConstraintEvaluatingStatementContext( null );

        // When
        ctx.getOrCreateLabelId( null );
    }

}
