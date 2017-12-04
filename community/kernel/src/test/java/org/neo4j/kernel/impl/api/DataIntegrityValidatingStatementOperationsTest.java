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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.stubbing.Answer;

import java.util.Iterator;

import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.IndexBelongsToConstraintException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchIndexException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInCompositeSchemaException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.iterator;

public class DataIntegrityValidatingStatementOperationsTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 0, 7 );
    IndexDescriptor index = IndexDescriptorFactory.forLabel( 0, 7 );
    IndexDescriptor uniqueIndex = IndexDescriptorFactory.uniqueForLabel( 0, 7 );
    private SchemaReadOperations innerRead;
    private SchemaWriteOperations innerWrite;
    private KeyWriteOperations innerKeyWrite;
    private DataIntegrityValidatingStatementOperations ops;

    @Before
    public void setup()
    {
        innerKeyWrite = mock( KeyWriteOperations.class );
        innerRead = mock( SchemaReadOperations.class );
        innerWrite = mock( SchemaWriteOperations.class );
        ops = new DataIntegrityValidatingStatementOperations( innerKeyWrite, innerRead, innerWrite );
    }

    @Test
    public void shouldDisallowReAddingIndex() throws Exception
    {
        // GIVEN
        when( innerRead.indexGetForSchema( state, descriptor ) ).thenReturn( index );

        // WHEN
        try
        {
            ops.indexCreate( state, descriptor );
            fail( "Should have thrown exception." );
        }
        catch ( AlreadyIndexedException e )
        {
            // ok
        }

        // THEN
        verify( innerWrite, never() ).indexCreate( eq( state ), any() );
    }

    @Test
    public void shouldDisallowAddingIndexWhenConstraintIndexExists() throws Exception
    {
        // GIVEN
        when( innerRead.indexGetForSchema( state, descriptor ) ).thenReturn( uniqueIndex );

        // WHEN
        try
        {
            ops.indexCreate( state, descriptor );
            fail( "Should have thrown exception." );
        }
        catch ( AlreadyConstrainedException e )
        {
            // ok
        }

        // THEN
        verify( innerWrite, never() ).indexCreate( eq( state ), any() );
    }

    @Test
    public void shouldDisallowDroppingIndexThatDoesNotExist() throws Exception
    {
        // GIVEN
        when( innerRead.indexGetForSchema( state, descriptor ) ).thenReturn( null );

        // WHEN
        try
        {
            ops.indexDrop( state, index );
            fail( "Should have thrown exception." );
        }
        catch ( DropIndexFailureException e )
        {
            assertThat(e.getCause(), instanceOf( NoSuchIndexException.class) );
        }

        // THEN
        verify( innerWrite, never() ).indexCreate( eq( state ), any() );
    }

    @Test
    public void shouldDisallowDroppingIndexWhenConstraintIndexExists() throws Exception
    {
        // GIVEN
        when( innerRead.indexGetForSchema( state, descriptor ) ).thenReturn( uniqueIndex );

        // WHEN
        try
        {
            ops.indexDrop( state, index );
            fail( "Should have thrown exception." );
        }
        catch ( DropIndexFailureException e )
        {
            assertThat(e.getCause(), instanceOf( IndexBelongsToConstraintException.class) );
        }

        // THEN
        verify( innerWrite, never() ).indexCreate( eq( state ), any() );
    }

    @Test
    public void shouldDisallowDroppingConstraintIndexThatDoesNotExists() throws Exception
    {
        // GIVEN
        when( innerRead.indexGetForSchema( state, descriptor ) ).thenReturn( uniqueIndex );

        // WHEN
        try
        {
            ops.indexDrop( state, index );
            fail( "Should have thrown exception." );
        }
        catch ( DropIndexFailureException e )
        {
            assertThat(e.getCause(), instanceOf( IndexBelongsToConstraintException.class) );
        }

        // THEN
        verify( innerWrite, never() ).indexCreate( eq( state ), any() );
    }

    @Test
    public void shouldDisallowDroppingConstraintIndexThatIsReallyJustRegularIndex() throws Exception
    {
        // GIVEN
        when( innerRead.indexGetForSchema( state, descriptor ) ).thenReturn( uniqueIndex );

        // WHEN
        try
        {
            ops.indexDrop( state, index );
            fail( "Should have thrown exception." );
        }
        catch ( DropIndexFailureException e )
        {
            assertThat(e.getCause(), instanceOf( IndexBelongsToConstraintException.class) );
        }

        // THEN
        verify( innerWrite, never() ).indexCreate( eq( state ), any() );
    }

    @Test
    public void shouldDisallowNullOrEmptyPropertyKey() throws Exception
    {
        try
        {
            ops.propertyKeyGetOrCreateForName( state, null );
            fail( "Should not be able to create null property key" );
        }
        catch ( IllegalTokenNameException e )
        {   // good
        }

        try
        {
            ops.propertyKeyGetOrCreateForName( state, "" );
            fail( "Should not be able to create empty property key" );
        }
        catch ( IllegalTokenNameException e )
        {   // good
        }
    }

    @Test
    public void shouldDisallowNullOrEmptyLabelName() throws Exception
    {
        try
        {
            ops.labelGetOrCreateForName( state, null );
            fail( "Should not be able to create null label" );
        }
        catch ( IllegalTokenNameException e )
        {   // good
        }

        try
        {
            ops.labelGetOrCreateForName( state, "" );
            fail( "Should not be able to create empty label" );
        }
        catch ( IllegalTokenNameException e )
        {   // good
        }
    }

    @Test( expected = SchemaKernelException.class )
    public void shouldFailInvalidLabelNames() throws Exception
    {
        ops.labelGetOrCreateForName( state, "" );
    }

    @Test( expected = SchemaKernelException.class )
    public void shouldFailOnNullLabel() throws Exception
    {
        ops.labelGetOrCreateForName( state, null );
    }

    @Test( expected = RepeatedPropertyInCompositeSchemaException.class )
    public void shouldFailIndexCreateOnRepeatedPropertyId() throws Exception
    {
        ops.indexCreate( state, SchemaDescriptorFactory.forLabel( 0, 1, 1 ) );
    }

    @Test( expected = RepeatedPropertyInCompositeSchemaException.class )
    public void shouldFailNodeExistenceCreateOnRepeatedPropertyId() throws Exception
    {
        ops.nodePropertyExistenceConstraintCreate( state, SchemaDescriptorFactory.forLabel( 0, 1, 1 ) );
    }

    @Test( expected = RepeatedPropertyInCompositeSchemaException.class )
    public void shouldFailRelExistenceCreateOnRepeatedPropertyId() throws Exception
    {
        ops.relationshipPropertyExistenceConstraintCreate( state, SchemaDescriptorFactory.forRelType( 0, 1, 1 ) );
    }

    @Test( expected = RepeatedPropertyInCompositeSchemaException.class )
    public void shouldFailUniquenessCreateOnRepeatedPropertyId() throws Exception
    {
        ops.uniquePropertyConstraintCreate( state, SchemaDescriptorFactory.forLabel( 0, 1, 1 ) );
    }

    @SafeVarargs
    private static <T> Answer<Iterator<T>> withIterator( final T... content )
    {
        return invocationOnMock -> iterator( content );
    }

    private final KernelStatement state = StatementOperationsTestHelper.mockedState();
}
