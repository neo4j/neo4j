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
package org.neo4j.kernel.api.proc;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.impl.proc.Procedures;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import static org.neo4j.kernel.api.proc.Neo4jTypes.NTAny;
import static org.neo4j.kernel.api.proc.Procedure.Key.key;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;
import static org.neo4j.helpers.collection.IteratorUtil.asList;

public class ProceduresTest
{
    @Rule public ExpectedException exception = ExpectedException.none();

    private final Procedures procs = new Procedures();
    private final ProcedureSignature signature = procedureSignature( "org", "myproc" ).build();
    private final Procedure procedure = new Procedure.BasicProcedure(signature)
    {
        @Override
        public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input )
        {
            return RawIterator.<Object[], ProcedureException>of( input );
        }
    };

    @Test
    public void shouldGetRegisteredProcedure() throws Throwable
    {
        // When
        procs.register( procedure );

        // Then
        assertThat( procs.get( signature.name() ), equalTo( signature ) );
    }

    @Test
    public void shouldCallRegisteredProcedure() throws Throwable
    {
        // Given
        procs.register( procedure );

        // When
        RawIterator<Object[], ProcedureException> result = procs.call( new Procedure.BasicContext()
        {
        }, signature.name(), new Object[]{1337} );

        // Then
        assertThat( asList( result ), contains( equalTo( new Object[]{1337} ) ) );
    }

    @Test
    public void shouldNotAllowCallingNonExistantProcedure() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "There is no procedure with the name `org.myproc` registered for this " +
                                 "database instance. Please ensure you've spelled the " +
                                 "procedure name correctly and that the procedure is properly deployed." );

        // When
        procs.call( new Procedure.BasicContext()
        {
        }, signature.name(), new Object[]{1337} );
    }

    @Test
    public void shouldNotAllowRegisteringConflicingName() throws Throwable
    {
        // Given
        procs.register( procedure );

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Unable to register procedure, because the name `org.myproc` is already in use." );

        // When
        procs.register( procedure );
    }

    @Test
    public void shouldNotAllowDuplicateFieldNamesInInput() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Procedure `asd(a :: ANY?, a :: ANY?) :: ()` cannot be " +
                                 "registered, because it contains a duplicated input field, 'a'. " +
                                 "You need to rename or remove one of the duplicate fields." );

        // When
        procs.register( procedureWithSignature( procedureSignature( "asd" ).in( "a", NTAny ).in( "a", NTAny ).build() ) );
    }

    @Test
    public void shouldNotAllowDuplicateFieldNamesInOutput() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Procedure `asd() :: (a :: ANY?, a :: ANY?)` cannot be registered, " +
                                 "because it contains a duplicated output field, 'a'. " +
                                 "You need to rename or remove one of the duplicate fields." );

        // When
        procs.register( procedureWithSignature( procedureSignature( "asd" ).out( "a", NTAny ).out( "a", NTAny ).build() ) );
    }

    @Test
    public void shouldSignalNonExistantProcedure() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "There is no procedure with the name `org.myproc` registered for this " +
                                 "database instance. Please ensure you've spelled the " +
                                 "procedure name correctly and that the procedure is properly deployed." );

        // When
        procs.get( signature.name() );
    }

    @Test
    public void shouldMakeContextAvailable() throws Throwable
    {
        // Given
        Procedure.Key<String> someKey = key("someKey", String.class);

        procs.register( new Procedure.BasicProcedure(signature)
        {
            @Override
            public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
            {
                return RawIterator.<Object[], ProcedureException>of( new Object[]{ctx.get( someKey )} );
            }
        } );

        Procedure.BasicContext ctx = new Procedure.BasicContext();
        ctx.put( someKey, "hello, world" );

        // When
        RawIterator<Object[], ProcedureException> result = procs.call( ctx, signature.name(), new Object[0] );

        // Then
        assertThat( asList( result ), contains( equalTo( new Object[]{ "hello, world" } ) ) );
    }

    private Procedure.BasicProcedure procedureWithSignature( final ProcedureSignature signature )
    {
        return new Procedure.BasicProcedure(signature)
        {
            @Override
            public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
            {
                return null;
            }
        };
    }
}
