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
package org.neo4j.kernel.builtinprocs;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.index.InternalIndexState.FAILED;
import static org.neo4j.kernel.api.index.InternalIndexState.ONLINE;
import static org.neo4j.kernel.api.index.InternalIndexState.POPULATING;
import static org.neo4j.storageengine.api.schema.SchemaRule.Kind.INDEX_RULE;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class AwaitIndexProcedureTest
{
    private static final int TIMEOUT = 10;
    private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;
    private final ReadOperations operations = mock( ReadOperations.class );
    private final IndexProcedures procedure = new IndexProcedures( new StubKernelTransaction( operations ), null );
    private final LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 123, 456 );
    private final LabelSchemaDescriptor anyDescriptor = SchemaDescriptorFactory.forLabel( 0, 0 );
    private final IndexDescriptor anyIndex = IndexDescriptorFactory.forSchema( anyDescriptor );

    @Test
    public void shouldThrowAnExceptionIfTheLabelDoesntExist() throws ProcedureException
    {
        when( operations.labelGetForName( "NonExistentLabel" ) ).thenReturn( -1 );

        try
        {
            procedure.awaitIndex( ":NonExistentLabel(prop)", TIMEOUT, TIME_UNIT );
            fail( "Expected an exception" );
        }
        catch ( ProcedureException e )
        {
            assertThat( e.status(), is( Status.Schema.LabelAccessFailed ) );
        }
    }

    @Test
    public void shouldThrowAnExceptionIfThePropertyKeyDoesntExist() throws ProcedureException
    {
        when( operations.propertyKeyGetForName( "nonExistentProperty" ) ).thenReturn( -1 );

        try
        {
            procedure.awaitIndex( ":Label(nonExistentProperty)", TIMEOUT, TIME_UNIT );
            fail( "Expected an exception" );
        }
        catch ( ProcedureException e )
        {
            assertThat( e.status(), is( Status.Schema.PropertyKeyAccessFailed ) );
        }
    }

    @Test
    public void shouldLookUpTheIndexByLabelIdAndPropertyKeyId()
            throws ProcedureException, SchemaRuleNotFoundException, IndexNotFoundKernelException
    {
        when( operations.labelGetForName( anyString() ) ).thenReturn( descriptor.getLabelId() );
        when( operations.propertyKeyGetForName( anyString() ) ).thenReturn( descriptor.getPropertyId() );
        when( operations.indexGetForSchema( anyObject() ) ).thenReturn( anyIndex );
        when( operations.indexGetState( any( IndexDescriptor.class ) ) ).thenReturn( ONLINE );

        procedure.awaitIndex( ":Person(name)", TIMEOUT, TIME_UNIT );

        verify( operations ).indexGetForSchema( descriptor );
    }

    @Test
    public void shouldThrowAnExceptionIfTheIndexHasFailed()
            throws SchemaRuleNotFoundException, IndexNotFoundKernelException

    {
        when( operations.labelGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.propertyKeyGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.indexGetForSchema( anyObject() ) ).thenReturn( anyIndex );
        when( operations.indexGetState( any( IndexDescriptor.class ) ) ).thenReturn( FAILED );

        try
        {
            procedure.awaitIndex( ":Person(name)", TIMEOUT, TIME_UNIT );
            fail( "Expected an exception" );
        }
        catch ( ProcedureException e )
        {
            assertThat( e.status(), is( Status.Schema.IndexCreationFailed ) );
        }
    }

    @Test
    public void shouldThrowAnExceptionIfTheIndexDoesNotExist()
            throws SchemaRuleNotFoundException, IndexNotFoundKernelException

    {
        when( operations.propertyKeyGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.labelGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.indexGetForSchema( any() ) ).thenThrow(
                new SchemaRuleNotFoundException( INDEX_RULE, SchemaDescriptorFactory.forLabel( 0, 0 ) ) );

        try
        {
            procedure.awaitIndex( ":Person(name)", TIMEOUT, TIME_UNIT );
            fail( "Expected an exception" );
        }
        catch ( ProcedureException e )
        {
            assertThat( e.status(), is( Status.Schema.IndexNotFound ) );
        }
    }

    @Test
    public void shouldBlockUntilTheIndexIsOnline() throws SchemaRuleNotFoundException, IndexNotFoundKernelException,
            InterruptedException
    {
        when( operations.labelGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.propertyKeyGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.indexGetForSchema( anyObject() ) ).thenReturn( anyIndex );

        AtomicReference<InternalIndexState> state = new AtomicReference<>( POPULATING );
        when( operations.indexGetState( any( IndexDescriptor.class ) ) ).then( invocationOnMock -> state.get() );

        AtomicBoolean done = new AtomicBoolean( false );
        new Thread( () ->
        {
            try
            {
                procedure.awaitIndex( ":Person(name)", TIMEOUT, TimeUnit.MILLISECONDS );
            }
            catch ( ProcedureException e )
            {
                throw new RuntimeException( e );
            }
            done.set( true );
        } ).start();

        assertThat( done.get(), is( false ) );

        state.set( ONLINE );
        assertEventually( "Procedure did not return after index was online",
                done::get, is( true ), TIMEOUT, TimeUnit.SECONDS );
    }

    @Test
    public void shouldTimeoutIfTheIndexTakesTooLongToComeOnline()
            throws InterruptedException, SchemaRuleNotFoundException, IndexNotFoundKernelException
    {
        when( operations.labelGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.propertyKeyGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.indexGetForSchema( anyObject() ) ).thenReturn( anyIndex );
        when( operations.indexGetState( any( IndexDescriptor.class ) ) ).thenReturn( POPULATING );

        AtomicReference<ProcedureException> exception = new AtomicReference<>();
        new Thread( () ->
        {
            try
            {
                procedure.awaitIndex( ":Person(name)", TIMEOUT, TimeUnit.MILLISECONDS );
            }
            catch ( ProcedureException e )
            {
                exception.set( e );
            }
        } ).start();

        assertEventually( "Procedure did not time out", exception::get, not( nullValue() ), TIMEOUT, TIME_UNIT );
        //noinspection ThrowableResultOfMethodCallIgnored
        assertThat( exception.get().status(), is( Status.Procedure.ProcedureTimedOut ) );
    }
}
