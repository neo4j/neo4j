/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.procedure.builtin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.test.assertion.Assert.assertEventually;

class AwaitIndexProcedureTest
{
    private static final int TIMEOUT = 10;
    private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;
    private KernelTransaction transaction;
    private TokenRead tokenRead;
    private SchemaRead schemaRead;
    private IndexProcedures procedure;
    private LabelSchemaDescriptor descriptor;
    private LabelSchemaDescriptor anyDescriptor;
    private IndexDescriptor anyIndex ;

    @BeforeEach
    void setup()
    {
        transaction = mock( KernelTransaction.class );
        tokenRead = mock( TokenRead.class );
        schemaRead = mock( SchemaRead.class );
        procedure = new IndexProcedures( transaction, null );
        descriptor = SchemaDescriptor.forLabel( 123, 456 );
        anyDescriptor = SchemaDescriptor.forLabel( 0, 0 );
        anyIndex = forSchema( anyDescriptor ).withName( "index" ).materialise( 13 );
        when( transaction.tokenRead() ).thenReturn( tokenRead );
        when( transaction.schemaRead() ).thenReturn( schemaRead );
    }

    @Test
    void shouldThrowAnExceptionIfTheLabelDoesntExist()
    {
        when( tokenRead.nodeLabel( "NonExistentLabel" ) ).thenReturn( -1 );

        ProcedureException exception =
                assertThrows( ProcedureException.class, () -> procedure.awaitIndexByPattern( ":NonExistentLabel(prop)", TIMEOUT, TIME_UNIT ) );
        assertThat( exception.status(), is( Status.Schema.LabelAccessFailed ) );
    }

    @Test
    void shouldThrowAnExceptionIfThePropertyKeyDoesntExist()
    {
        when( tokenRead.propertyKey( "nonExistentProperty" ) ).thenReturn( -1 );

        ProcedureException exception =
                assertThrows( ProcedureException.class, () -> procedure.awaitIndexByPattern( ":Label(nonExistentProperty)", TIMEOUT, TIME_UNIT ) );
        assertThat( exception.status(), is( Status.Schema.PropertyKeyAccessFailed ) );
    }

    @Test
    void shouldLookUpTheIndexByLabelIdAndPropertyKeyId() throws ProcedureException, IndexNotFoundKernelException
    {
        when( tokenRead.nodeLabel( anyString() ) ).thenReturn( descriptor.getLabelId() );
        when( tokenRead.propertyKey( anyString() ) ).thenReturn( descriptor.getPropertyId() );
        when( schemaRead.index( anyInt(), any() ) ).thenReturn( anyIndex );
        when( schemaRead.indexGetState( any( IndexDescriptor.class ) ) ).thenReturn( ONLINE );

        procedure.awaitIndexByPattern( ":Person(name)", TIMEOUT, TIME_UNIT );

        verify( schemaRead ).index( descriptor.getLabelId(), descriptor.getPropertyId() );
    }

    @Test
    void shouldLookUpTheIndexByIndexName() throws ProcedureException, IndexNotFoundKernelException
    {
        when( tokenRead.nodeLabel( anyString() ) ).thenReturn( descriptor.getLabelId() );
        when( tokenRead.propertyKey( anyString() ) ).thenReturn( descriptor.getPropertyId() );
        when( schemaRead.indexGetForName( "my index" ) ).thenReturn( anyIndex );
        when( schemaRead.indexGetState( any( IndexDescriptor.class ) ) ).thenReturn( ONLINE );

        procedure.awaitIndexByName( "`my index`", TIMEOUT, TIME_UNIT );

        verify( schemaRead ).indexGetForName( "my index" );
    }

    @Test
    void shouldThrowAnExceptionIfTheIndexHasFailed() throws IndexNotFoundKernelException
    {
        when( tokenRead.nodeLabel( anyString() ) ).thenReturn( 0 );
        when( tokenRead.propertyKey( anyString() ) ).thenReturn( 0 );
        when( schemaRead.index( anyInt(), any() ) ).thenReturn( anyIndex );
        when( schemaRead.indexGetState( any( IndexDescriptor.class ) ) ).thenReturn( FAILED );
        when( schemaRead.indexGetFailure( any( IndexDescriptor.class ) ) ).thenReturn( Exceptions.stringify( new Exception( "Kilroy was here" ) ) );

        ProcedureException exception = assertThrows( ProcedureException.class, () -> procedure.awaitIndexByPattern( ":Person(name)", TIMEOUT, TIME_UNIT ) );
        assertThat( exception.status(), is( Status.Schema.IndexCreationFailed ) );
        assertThat( exception.getMessage(), containsString( "Kilroy was here" ) );
        assertThat( exception.getMessage(), containsString( ":Person(name)" ) );
    }

    @Test
    void shouldThrowAnExceptionIfTheIndexDoesNotExist()
    {
        when( tokenRead.propertyKey( anyString() ) ).thenReturn( 0 );
        when( tokenRead.nodeLabel( anyString() ) ).thenReturn( 0 );
        when( schemaRead.index( anyInt(), any() ) ).thenReturn( IndexDescriptor.NO_INDEX );

        ProcedureException exception =
                assertThrows( ProcedureException.class, () -> procedure.awaitIndexByPattern( ":Person(name)", TIMEOUT, TIME_UNIT ) );
        assertThat( exception.status(), is( Status.Schema.IndexNotFound ) );
    }

    @Test
    void shouldThrowAnExceptionIfGivenAnIndexName()
    {
        assertThrows( IllegalArgumentException.class, () -> procedure.awaitIndexByPattern( "`some index`", TIMEOUT, TIME_UNIT ) );
    }

    @Test
    void shouldThrowAnExceptionIfTheIndexWithGivenNameDoesNotExist()
    {
        when( tokenRead.propertyKey( anyString() ) ).thenReturn( 0 );
        when( tokenRead.nodeLabel( anyString() ) ).thenReturn( 0 );
        when( schemaRead.indexGetForName( "some index" ) ).thenReturn( IndexDescriptor.NO_INDEX );

        ProcedureException exception = assertThrows( ProcedureException.class, () -> procedure.awaitIndexByName( "`some index`", TIMEOUT, TIME_UNIT ) );
        assertThat( exception.status(), is( Status.Schema.IndexNotFound ) );
    }

    @Test
    void shouldBlockUntilTheIndexIsOnline() throws IndexNotFoundKernelException, InterruptedException
    {
        when( tokenRead.nodeLabel( anyString() ) ).thenReturn( 0 );
        when( tokenRead.propertyKey( anyString() ) ).thenReturn( 0 );
        when( schemaRead.index( anyInt(), any() ) ).thenReturn( anyIndex );

        AtomicReference<InternalIndexState> state = new AtomicReference<>( POPULATING );
        when( schemaRead.indexGetState( any( IndexDescriptor.class ) ) ).then( invocationOnMock -> state.get() );

        AtomicBoolean done = new AtomicBoolean( false );
        new Thread( () ->
        {
            try
            {
                procedure.awaitIndexByPattern( ":Person(name)", TIMEOUT, TIME_UNIT );
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
                done::get, is( true ), TIMEOUT, TIME_UNIT );
    }

    @Test
    void shouldTimeoutIfTheIndexTakesTooLongToComeOnline() throws InterruptedException, IndexNotFoundKernelException
    {
        when( tokenRead.nodeLabel( anyString() ) ).thenReturn( 0 );
        when( tokenRead.propertyKey( anyString() ) ).thenReturn( 0 );
        when( schemaRead.index( anyInt(), anyInt() ) ).thenReturn( anyIndex );
        when( schemaRead.indexGetState( any( IndexDescriptor.class ) ) ).thenReturn( POPULATING );

        AtomicReference<ProcedureException> exception = new AtomicReference<>();
        new Thread( () ->
        {
            try
            {
                // We wait here, because we expect timeout
                procedure.awaitIndexByPattern( ":Person(name)", 0, TIME_UNIT );
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
