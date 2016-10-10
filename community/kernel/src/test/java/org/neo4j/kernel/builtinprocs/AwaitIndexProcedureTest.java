/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexSchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.index.InternalIndexState.FAILED;
import static org.neo4j.kernel.api.index.InternalIndexState.ONLINE;
import static org.neo4j.kernel.api.index.InternalIndexState.POPULATING;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class AwaitIndexProcedureTest
{
    private static final int timeout = 40;
    private static final TimeUnit timeoutUnits = TimeUnit.MILLISECONDS;
    private final ReadOperations operations = mock( ReadOperations.class );
    private final IndexProcedures procedure = new IndexProcedures( new StubKernelTransaction( operations ), null );

    @Test
    public void shouldThrowAnExceptionIfTheLabelDoesntExist() throws ProcedureException
    {
        when( operations.labelGetForName( "NonExistentLabel" ) ).thenReturn( -1 );

        try
        {
            procedure.awaitIndex( ":NonExistentLabel(prop)", timeout, timeoutUnits );
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
            procedure.awaitIndex( ":Label(nonExistentProperty)", timeout, timeoutUnits );
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
        when( operations.labelGetForName( anyString() ) ).thenReturn( 123 );
        when( operations.propertyKeyGetForName( anyString() ) ).thenReturn( 456 );
        when( operations.indexGetForLabelAndPropertyKey( anyInt(), anyInt() ) )
                .thenReturn( new IndexDescriptor( 0, 0 ) );
        when( operations.indexGetState( any( IndexDescriptor.class ) ) ).thenReturn( ONLINE );

        procedure.awaitIndex( ":Person(name)", timeout, timeoutUnits );

        verify( operations ).indexGetForLabelAndPropertyKey( 123, 456 );
    }

    @Test
    public void shouldThrowAnExceptionIfTheIndexHasFailed()
            throws SchemaRuleNotFoundException, IndexNotFoundKernelException

    {
        when( operations.labelGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.propertyKeyGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.indexGetForLabelAndPropertyKey( anyInt(), anyInt() ) )
                .thenReturn( new IndexDescriptor( 0, 0 ) );
        when( operations.indexGetState( any( IndexDescriptor.class ) ) ).thenReturn( FAILED );

        try
        {
            procedure.awaitIndex( ":Person(name)", timeout, timeoutUnits );
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
        when( operations.labelGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.propertyKeyGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.indexGetForLabelAndPropertyKey( anyInt(), anyInt() ) )
                .thenThrow( new IndexSchemaRuleNotFoundException( -1, -1 ) );

        try
        {
            procedure.awaitIndex( ":Person(name)", timeout, timeoutUnits );
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
        when( operations.indexGetForLabelAndPropertyKey( anyInt(), anyInt() ) )
                .thenReturn( new IndexDescriptor( 0, 0 ) );

        AtomicReference<InternalIndexState> state = new AtomicReference<>( POPULATING );
        when( operations.indexGetState( any( IndexDescriptor.class ) ) ).then( new Answer<InternalIndexState>()
        {
            @Override
            public InternalIndexState answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                return state.get();
            }
        } );

        AtomicBoolean done = new AtomicBoolean( false );
        new Thread( () ->
        {
            try
            {
                procedure.awaitIndex( ":Person(name)", timeout, timeoutUnits );
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
                done::get, is( true ), 10, TimeUnit.SECONDS );
    }

    @Test
    public void shouldTimeoutIfTheIndexTakesTooLongToComeOnline()
            throws InterruptedException, SchemaRuleNotFoundException, IndexNotFoundKernelException
    {
        when( operations.labelGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.propertyKeyGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.indexGetForLabelAndPropertyKey( anyInt(), anyInt() ) )
                .thenReturn( new IndexDescriptor( 0, 0 ) );
        when( operations.indexGetState( any( IndexDescriptor.class ) ) ).thenReturn( POPULATING );

        AtomicReference<ProcedureException> exception = new AtomicReference<>();
        new Thread( () ->
        {
            try
            {
                procedure.awaitIndex( ":Person(name)", timeout, timeoutUnits );
            }
            catch ( ProcedureException e )
            {
                exception.set( e );
            }
        } ).start();

        assertEventually( "Procedure did not time out", exception::get, not( nullValue() ), 10, TimeUnit.SECONDS );
        //noinspection ThrowableResultOfMethodCallIgnored
        assertThat( exception.get().status(), is( Status.Procedure.ProcedureTimedOut ) );
    }
}
