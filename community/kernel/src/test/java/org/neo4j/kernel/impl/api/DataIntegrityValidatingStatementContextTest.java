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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.exceptions.schema.AddIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IndexBelongsToConstraintException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchIndexException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asIterator;

public class DataIntegrityValidatingStatementContextTest
{
    @Test
    public void shouldDisallowReAddingIndex() throws Exception
    {
        // GIVEN
        long label = 0, propertyKey = 7;
        IndexDescriptor rule = new IndexDescriptor( label, propertyKey );
        StatementContext inner = mock( StatementContext.class );
        DataIntegrityValidatingStatementContext ctx = new DataIntegrityValidatingStatementContext( inner );
        when( inner.indexesGetForLabel( rule.getLabelId() ) ).thenAnswer( withIterator( rule ) );

        // WHEN
        try
        {
            ctx.indexCreate( label, propertyKey );
            fail( "Should have thrown exception." );
        }
        catch ( AddIndexFailureException e )
        {
            assertThat(e.getCause(), instanceOf( AlreadyIndexedException.class) );
        }

        // THEN
        verify( inner, never() ).indexCreate( anyLong(), anyLong() );
    }

    @Test
    public void shouldDisallowAddingIndexWhenConstraintIndexExists() throws Exception
    {
        // GIVEN
        long label = 0, propertyKey = 7;
        IndexDescriptor rule = new IndexDescriptor( label, propertyKey );
        StatementContext inner = mock( StatementContext.class );
        DataIntegrityValidatingStatementContext ctx = new DataIntegrityValidatingStatementContext( inner );
        when( inner.indexesGetForLabel( rule.getLabelId() ) ).thenAnswer( withIterator(  ) );
        when( inner.uniqueIndexesGetForLabel( rule.getLabelId() ) ).thenAnswer( withIterator( rule ) );

        // WHEN
        try
        {
            ctx.indexCreate( label, propertyKey );
            fail( "Should have thrown exception." );
        }
        catch ( AddIndexFailureException e )
        {
            assertThat(e.getCause(), instanceOf( AlreadyConstrainedException.class) );
        }

        // THEN
        verify( inner, never() ).indexCreate( anyLong(), anyLong() );
    }

    @Test
    public void shouldDisallowReAddingConstraintIndex() throws Exception
    {
        // GIVEN
        long label = 0, propertyKey = 7;
        IndexDescriptor rule = new IndexDescriptor( label, propertyKey );
        StatementContext inner = mock( StatementContext.class );
        DataIntegrityValidatingStatementContext ctx = new DataIntegrityValidatingStatementContext( inner );
        when( inner.indexesGetForLabel( rule.getLabelId() ) ).thenAnswer( withIterator(  ) );
        when( inner.uniqueIndexesGetForLabel( rule.getLabelId() ) ).thenAnswer( withIterator( rule ) );

        // WHEN
        try
        {
            ctx.uniqueIndexCreate( label, propertyKey );
            fail( "Should have thrown exception." );
        }
        catch ( AddIndexFailureException e )
        {
            assertThat(e.getCause(), instanceOf( AlreadyConstrainedException.class) );
        }

        // THEN
        verify( inner, never() ).indexCreate( anyLong(), anyLong() );
    }

    @Test
    public void shouldDisallowDroppingIndexThatDoesNotExist() throws Exception
    {
        // GIVEN
        long label = 0, propertyKey = 7;
        IndexDescriptor indexDescriptor = new IndexDescriptor( label, propertyKey );
        StatementContext inner = mock( StatementContext.class );
        DataIntegrityValidatingStatementContext ctx = new DataIntegrityValidatingStatementContext( inner );
        when( inner.uniqueIndexesGetForLabel( indexDescriptor.getLabelId() ) ).thenAnswer( withIterator(  ) );
        when( inner.indexesGetForLabel( indexDescriptor.getLabelId() ) ).thenAnswer( withIterator( ) );

        // WHEN
        try
        {
            ctx.indexDrop( indexDescriptor );
            fail( "Should have thrown exception." );
        }
        catch ( DropIndexFailureException e )
        {
            assertThat(e.getCause(), instanceOf( NoSuchIndexException.class) );
        }

        // THEN
        verify( inner, never() ).indexCreate( anyLong(), anyLong() );
    }

    @Test
    public void shouldDisallowDroppingIndexWhenConstraintIndexExists() throws Exception
    {
        // GIVEN
        long label = 0, propertyKey = 7;
        IndexDescriptor indexDescriptor = new IndexDescriptor( label, propertyKey );
        StatementContext inner = mock( StatementContext.class );
        DataIntegrityValidatingStatementContext ctx = new DataIntegrityValidatingStatementContext( inner );
        when( inner.uniqueIndexesGetForLabel( indexDescriptor.getLabelId() ) ).thenAnswer( withIterator( indexDescriptor ) );
        when( inner.indexesGetForLabel( indexDescriptor.getLabelId() ) ).thenAnswer( withIterator() );

        // WHEN
        try
        {
            ctx.indexDrop( new IndexDescriptor( label, propertyKey ) );
            fail( "Should have thrown exception." );
        }
        catch ( DropIndexFailureException e )
        {
            assertThat(e.getCause(), instanceOf( IndexBelongsToConstraintException.class) );
        }

        // THEN
        verify( inner, never() ).indexCreate( anyLong(), anyLong() );
    }

    @Test
    public void shouldDisallowDroppingConstraintIndexThatDoesNotExists() throws Exception
    {
        // GIVEN
        long label = 0, propertyKey = 7;
        IndexDescriptor indexDescriptor = new IndexDescriptor( label, propertyKey );
        StatementContext inner = mock( StatementContext.class );
        DataIntegrityValidatingStatementContext ctx = new DataIntegrityValidatingStatementContext( inner );
        when( inner.uniqueIndexesGetForLabel( indexDescriptor.getLabelId() ) ).thenAnswer( withIterator( indexDescriptor ) );
        when( inner.indexesGetForLabel( indexDescriptor.getLabelId() ) ).thenAnswer( withIterator() );

        // WHEN
        try
        {
            ctx.indexDrop( new IndexDescriptor( label, propertyKey ) );
            fail( "Should have thrown exception." );
        }
        catch ( DropIndexFailureException e )
        {
            assertThat(e.getCause(), instanceOf( IndexBelongsToConstraintException.class) );
        }

        // THEN
        verify( inner, never() ).indexCreate( anyLong(), anyLong() );
    }

    @Test
    public void shouldDisallowDroppingConstraintIndexThatIsReallyJustRegularIndex() throws Exception
    {
        // GIVEN
        long label = 0, propertyKey = 7;
        IndexDescriptor indexDescriptor = new IndexDescriptor( label, propertyKey );
        StatementContext inner = mock( StatementContext.class );
        DataIntegrityValidatingStatementContext ctx = new DataIntegrityValidatingStatementContext( inner );
        when( inner.uniqueIndexesGetForLabel( indexDescriptor.getLabelId() ) ).thenAnswer( withIterator( indexDescriptor ) );
        when( inner.indexesGetForLabel( indexDescriptor.getLabelId() ) ).thenAnswer( withIterator() );

        // WHEN
        try
        {
            ctx.indexDrop( new IndexDescriptor( label, propertyKey ) );
            fail( "Should have thrown exception." );
        }
        catch ( DropIndexFailureException e )
        {
            assertThat(e.getCause(), instanceOf( IndexBelongsToConstraintException.class) );
        }

        // THEN
        verify( inner, never() ).indexCreate( anyLong(), anyLong() );
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

    @Test(expected = SchemaKernelException.class)
    public void shouldFailInvalidLabelNames() throws Exception
    {
        // Given
        DataIntegrityValidatingStatementContext ctx = new DataIntegrityValidatingStatementContext( null );

        // When
        ctx.labelGetOrCreateForName( "" );
    }

    @Test(expected = SchemaKernelException.class)
    public void shouldFailOnNullLabel() throws Exception
    {
        // Given
        DataIntegrityValidatingStatementContext ctx = new DataIntegrityValidatingStatementContext( null );

        // When
        ctx.labelGetOrCreateForName( null );
    }

}
