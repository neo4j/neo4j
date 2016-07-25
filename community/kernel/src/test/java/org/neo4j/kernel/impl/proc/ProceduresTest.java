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
package org.neo4j.kernel.impl.proc;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.proc.CallableProcedure.Key.key;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTAny;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class ProceduresTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final Procedures procs = new Procedures();
    private final ProcedureSignature signature = procedureSignature( "org", "myproc" ).build();
    private final CallableProcedure procedure = procedure( signature );

    @Test
    public void shouldGetRegisteredProcedure() throws Throwable
    {
        // When
        procs.register( procedure );

        // Then
        assertThat( procs.get( signature.name() ), equalTo( signature ) );
    }

    @Test
    public void shouldGetAllRegisteredProcedures() throws Throwable
    {
        // When
        procs.register( procedure( procedureSignature( "org", "myproc1" ).build() ) );
        procs.register( procedure( procedureSignature( "org", "myproc2" ).build() ) );
        procs.register( procedure( procedureSignature( "org", "myproc3" ).build() ) );

        // Then
        List<ProcedureSignature> signatures = Iterables.asList( procs.getAll() );
        assertThat( signatures, containsInAnyOrder(
                procedureSignature( "org", "myproc1" ).build(),
                procedureSignature( "org", "myproc2" ).build(),
                procedureSignature( "org", "myproc3" ).build() ) );
    }

    @Test
    public void shouldCallRegisteredProcedure() throws Throwable
    {
        // Given
        procs.register( procedure );

        // When
        RawIterator<Object[], ProcedureException> result = procs.call( new CallableProcedure.BasicContext()
        {
        }, signature.name(), new Object[]{1337} );

        // Then
        assertThat( asList( result ), contains( equalTo( new Object[]{1337} ) ) );
    }

    @Test
    public void shouldNotAllowCallingNonExistingProcedure() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "There is no procedure with the name `org.myproc` registered for this " +
                                 "database instance. Please ensure you've spelled the " +
                                 "procedure name correctly and that the procedure is properly deployed." );

        // When
        procs.call( new CallableProcedure.BasicContext()
        {
        }, signature.name(), new Object[]{1337} );
    }

    @Test
    public void shouldNotAllowRegisteringConflictingName() throws Throwable
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
    public void shouldSignalNonExistingProcedure() throws Throwable
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
        CallableProcedure.Key<String> someKey = key("someKey", String.class);

        procs.register( new CallableProcedure.BasicProcedure(signature)
        {
            @Override
            public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
            {
                return RawIterator.<Object[], ProcedureException>of( new Object[]{ctx.get( someKey )} );
            }
        } );

        CallableProcedure.BasicContext ctx = new CallableProcedure.BasicContext();
        ctx.put( someKey, "hello, world" );

        // When
        RawIterator<Object[], ProcedureException> result = procs.call( ctx, signature.name(), new Object[0] );

        // Then
        assertThat( asList( result ), contains( equalTo( new Object[]{ "hello, world" } ) ) );
    }

    @Test
    public void shouldFailCompileProcedureWithReadConflict() throws Throwable
    {
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Conflicting procedure annotation, cannot use PerformsWrites and mode" );
        procs.register( ProcedureWithReadConflictAnnotation.class );
    }

    @Test
    public void shouldFailCompileProcedureWithWriteConflict() throws Throwable
    {
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Conflicting procedure annotation, cannot use PerformsWrites and mode" );
        procs.register( ProcedureWithWriteConflictAnnotation.class );
    }

    @Test
    public void shouldFailCompileProcedureWithSchemaConflict() throws Throwable
    {
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Conflicting procedure annotation, cannot use PerformsWrites and mode" );
        procs.register( ProcedureWithSchemaConflictAnnotation.class );
    }

    @Test
    public void shouldFailCompileProcedureWithDBMSConflict() throws Throwable
    {
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Conflicting procedure annotation, cannot use PerformsWrites and mode" );
        procs.register( ProcedureWithDBMSConflictAnnotation.class );
    }

    public static class ProcedureWithReadConflictAnnotation
    {
        @PerformsWrites
        @Procedure( mode = Procedure.Mode.READ )
        public void shouldCompile()
        {
        }
    }

    public static class ProcedureWithWriteConflictAnnotation
    {
        @PerformsWrites
        @Procedure( mode = Procedure.Mode.WRITE )
        public void shouldCompileToo()
        {
        }
    }

    public static class ProcedureWithDBMSConflictAnnotation
    {
        @PerformsWrites
        @Procedure( mode = Procedure.Mode.DBMS )
        public void shouldNotCompile()
        {
        }
    }

    public static class ProcedureWithSchemaConflictAnnotation
    {
        @PerformsWrites
        @Procedure( mode = Procedure.Mode.SCHEMA )
        public void shouldNotCompile()
        {
        }
    }

    private CallableProcedure.BasicProcedure procedureWithSignature( final ProcedureSignature signature )
    {
        return new CallableProcedure.BasicProcedure(signature)
        {
            @Override
            public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
            {
                return null;
            }
        };
    }

    private CallableProcedure procedure( ProcedureSignature signature )
    {
        return new CallableProcedure.BasicProcedure( signature )
        {
            @Override
            public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input )
            {
                return RawIterator.<Object[], ProcedureException>of( input );
            }
        };
    }
}
