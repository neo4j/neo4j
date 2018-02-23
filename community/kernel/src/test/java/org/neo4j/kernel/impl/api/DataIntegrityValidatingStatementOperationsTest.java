/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.ExpectedException;

import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IndexBelongsToConstraintException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchIndexException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInCompositeSchemaException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.rules.ExpectedException.none;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forRelType;
import static org.neo4j.kernel.api.schema.index.IndexDescriptorFactory.uniqueForLabel;
import static org.neo4j.kernel.impl.api.StatementOperationsTestHelper.mockedState;

@EnableRuleMigrationSupport
public class DataIntegrityValidatingStatementOperationsTest
{
    @Rule
    public ExpectedException exception = none();

    private LabelSchemaDescriptor descriptor = forLabel( 0, 7 );
    private IndexDescriptor index = IndexDescriptorFactory.forLabel( 0, 7 );
    private IndexDescriptor uniqueIndex = uniqueForLabel( 0, 7 );
    private SchemaReadOperations innerRead;
    private SchemaWriteOperations innerWrite;
    private KeyWriteOperations innerKeyWrite;
    private DataIntegrityValidatingStatementOperations ops;

    @BeforeEach
    void setup()
    {
        innerKeyWrite = mock( KeyWriteOperations.class );
        innerRead = mock( SchemaReadOperations.class );
        innerWrite = mock( SchemaWriteOperations.class );
        ops = new DataIntegrityValidatingStatementOperations( innerKeyWrite, innerRead, innerWrite );
    }

    @Test
    void shouldDisallowReAddingIndex() throws Exception
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
    void shouldDisallowAddingIndexWhenConstraintIndexExists() throws Exception
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
    void shouldDisallowDroppingIndexThatDoesNotExist() throws Exception
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
            assertThat( e.getCause(), instanceOf( NoSuchIndexException.class ) );
        }

        // THEN
        verify( innerWrite, never() ).indexCreate( eq( state ), any() );
    }

    @Test
    void shouldDisallowDroppingIndexWhenConstraintIndexExists() throws Exception
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
            assertThat( e.getCause(), instanceOf( IndexBelongsToConstraintException.class ) );
        }

        // THEN
        verify( innerWrite, never() ).indexCreate( eq( state ), any() );
    }

    @Test
    void shouldDisallowDroppingConstraintIndexThatDoesNotExists() throws Exception
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
            assertThat( e.getCause(), instanceOf( IndexBelongsToConstraintException.class ) );
        }

        // THEN
        verify( innerWrite, never() ).indexCreate( eq( state ), any() );
    }

    @Test
    void shouldDisallowDroppingConstraintIndexThatIsReallyJustRegularIndex() throws Exception
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
            assertThat( e.getCause(), instanceOf( IndexBelongsToConstraintException.class ) );
        }

        // THEN
        verify( innerWrite, never() ).indexCreate( eq( state ), any() );
    }

    @Test
    void shouldDisallowNullOrEmptyPropertyKey()
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
    void shouldDisallowNullOrEmptyLabelName() throws Exception
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

    @Test
    void shouldFailInvalidLabelNames()
    {
        assertThrows( SchemaKernelException.class, () -> ops.labelGetOrCreateForName( state, "" ) );
    }

    @Test
    void shouldFailOnNullLabel()
    {
        assertThrows( SchemaKernelException.class, () -> ops.labelGetOrCreateForName( state, null ) );
    }

    @Test
    void shouldFailIndexCreateOnRepeatedPropertyId()
    {
        assertThrows( RepeatedPropertyInCompositeSchemaException.class, () -> ops.indexCreate( state, forLabel( 0, 1, 1 ) ) );
    }

    @Test
    void shouldFailNodeExistenceCreateOnRepeatedPropertyId()
    {
        assertThrows( RepeatedPropertyInCompositeSchemaException.class,
                () -> ops.nodePropertyExistenceConstraintCreate( state, forLabel( 0, 1, 1 ) ) );
    }

    @Test
    void shouldFailRelExistenceCreateOnRepeatedPropertyId()
    {
        assertThrows( RepeatedPropertyInCompositeSchemaException.class,
                () -> ops.relationshipPropertyExistenceConstraintCreate( state, forRelType( 0, 1, 1 ) ) );
    }

    @Test
    void shouldFailUniquenessCreateOnRepeatedPropertyId()
    {
        assertThrows( RepeatedPropertyInCompositeSchemaException.class,
                () -> ops.uniquePropertyConstraintCreate( state, forLabel( 0, 1, 1 ) ) );
    }

    private final KernelStatement state = mockedState();
}
