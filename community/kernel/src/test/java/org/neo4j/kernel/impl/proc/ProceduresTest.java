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
package org.neo4j.kernel.impl.proc;

import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.collection.RawIterator;
import org.neo4j.common.DependencyResolver;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.StubResourceManager;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.ValueMapper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.api.proc.BasicContext.buildContext;

class ProceduresTest
{
    private final GlobalProcedures procs = new GlobalProcedures();
    private final ProcedureSignature signature = procedureSignature( "org", "myproc" ).out( "name", NTString ).build();
    private final CallableProcedure procedure = procedure( signature );
    private final ResourceTracker resourceTracker = new StubResourceManager();
    private final DependencyResolver dependencyResolver = new Dependencies();
    private final ValueMapper<Object> valueMapper = new DefaultValueMapper( mock( EmbeddedProxySPI.class ) );

    @Test
    void shouldGetRegisteredProcedure() throws Throwable
    {
        // When
        procs.register( procedure );

        // Then
        assertThat( procs.procedure( signature.name() ).signature(), equalTo( signature ) );
    }

    @Test
    void shouldGetAllRegisteredProcedures() throws Throwable
    {
        // When
        procs.register( procedure( procedureSignature( "org", "myproc1" ).out( "age", NTInteger ).build() ) );
        procs.register( procedure( procedureSignature( "org", "myproc2" ).out( "age", NTInteger ).build() ) );
        procs.register( procedure( procedureSignature( "org", "myproc3" ).out( "age", NTInteger ).build() ) );

        // Then
        List<ProcedureSignature> signatures = Iterables.asList( procs.getAllProcedures() );
        assertThat( signatures, containsInAnyOrder(
                procedureSignature( "org", "myproc1" ).out( "age", NTInteger ).build(),
                procedureSignature( "org", "myproc2" ).out( "age", NTInteger ).build(),
                procedureSignature( "org", "myproc3" ).out( "age", NTInteger ).build() ) );
    }

    @Test
    void shouldCallRegisteredProcedure() throws Throwable
    {
        // Given
        procs.register( procedure );

        // When
        RawIterator<Object[], ProcedureException> result =
                procs.callProcedure( buildContext( dependencyResolver, valueMapper ).context(), signature.name(), new Object[]{1337}, resourceTracker );

        // Then
        assertThat( asList( result ), contains( equalTo( new Object[]{1337} ) ) );
    }

    @Test
    void shouldNotAllowCallingNonExistingProcedure()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () ->
                procs.callProcedure( prepareContext(), signature.name(), new Object[]{1337}, resourceTracker ) );
        assertThat( exception.getMessage(), equalTo( "There is no procedure with the name `org.myproc` registered for this " +
                                                    "database instance. Please ensure you've spelled the " +
                                                    "procedure name correctly and that the procedure is properly deployed." ) );
    }

    @Test
    void shouldNotAllowRegisteringConflictingName() throws Throwable
    {
        // Given
        procs.register( procedure );

        ProcedureException exception = assertThrows( ProcedureException.class, () -> procs.register( procedure ) );
        assertThat( exception.getMessage(), equalTo( "Unable to register procedure, because the name `org.myproc` is already in use." ) );
    }

    @Test
    void shouldNotAllowDuplicateFieldNamesInInput()
    {
        ProcedureException exception = assertThrows( ProcedureException.class,
                () -> procs.register( procedureWithSignature( procedureSignature( "asd" ).in( "a", NTAny ).in( "a", NTAny ).build() ) ) );
        assertThat( exception.getMessage(), equalTo( "Procedure `asd(a :: ANY?, a :: ANY?) :: ()` cannot be " +
                                                    "registered, because it contains a duplicated input field, 'a'. " +
                                                    "You need to rename or remove one of the duplicate fields." ) );
    }

    @Test
    void shouldNotAllowDuplicateFieldNamesInOutput()
    {
        ProcedureException exception = assertThrows( ProcedureException.class,
                () -> procs.register( procedureWithSignature( procedureSignature( "asd" ).out( "a", NTAny ).out( "a", NTAny ).build() ) ) );
        assertThat( exception.getMessage(), equalTo( "Procedure `asd() :: (a :: ANY?, a :: ANY?)` cannot be registered, " +
                                                    "because it contains a duplicated output field, 'a'. " +
                                                    "You need to rename or remove one of the duplicate fields." ) );
    }

    @Test
    void shouldSignalNonExistingProcedure()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> procs.procedure( signature.name() ) );
        assertThat( exception.getMessage(), equalTo( "There is no procedure with the name `org.myproc` registered for this " +
                                                    "database instance. Please ensure you've spelled the " +
                                                    "procedure name correctly and that the procedure is properly deployed." ) );
    }

    @Test
    void shouldMakeContextAvailable() throws Throwable
    {
        // Given

        procs.register( new CallableProcedure.BasicProcedure( signature )
        {
            @Override
            public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input, ResourceTracker resourceTracker ) throws ProcedureException
            {
                return RawIterator.<Object[], ProcedureException>of( new Object[]{ctx.get( Context.THREAD )} );
            }
        } );

        Context ctx = prepareContext();

        // When
        RawIterator<Object[], ProcedureException> result = procs.callProcedure( ctx, signature.name(), new Object[0], resourceTracker );

        // Then
        assertThat( asList( result ), contains( equalTo( new Object[]{ Thread.currentThread() } ) ) );
    }

    @Test
    void shouldFailCompileProcedureWithReadConflict()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> procs.registerProcedure( ProcedureWithReadConflictAnnotation.class ) );
        assertThat( exception.getMessage(), equalTo( "Conflicting procedure annotation, cannot use PerformsWrites and mode" ) );
    }

    @Test
    void shouldFailCompileProcedureWithWriteConflict()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> procs.registerProcedure( ProcedureWithWriteConflictAnnotation.class ) );
        assertThat( exception.getMessage(), equalTo( "Conflicting procedure annotation, cannot use PerformsWrites and mode" ) );
    }

    @Test
    void shouldFailCompileProcedureWithSchemaConflict()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> procs.registerProcedure( ProcedureWithSchemaConflictAnnotation.class ) );
        assertThat( exception.getMessage(), equalTo( "Conflicting procedure annotation, cannot use PerformsWrites and mode" ) );
    }

    @Test
    void shouldFailCompileProcedureWithDBMSConflict()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> procs.registerProcedure( ProcedureWithDBMSConflictAnnotation.class ) );
        assertThat( exception.getMessage(), equalTo( "Conflicting procedure annotation, cannot use PerformsWrites and mode" ) );
    }

    private Context prepareContext()
    {
        return buildContext( dependencyResolver, valueMapper ).context();
    }

    public static class ProcedureWithReadConflictAnnotation
    {
        @PerformsWrites
        @Procedure( mode = Mode.READ )
        public void shouldCompile()
        {
        }
    }

    public static class ProcedureWithWriteConflictAnnotation
    {
        @PerformsWrites
        @Procedure( mode = Mode.WRITE )
        public void shouldCompileToo()
        {
        }
    }

    public static class ProcedureWithDBMSConflictAnnotation
    {
        @PerformsWrites
        @Procedure( mode = Mode.DBMS )
        public void shouldNotCompile()
        {
        }
    }

    public static class ProcedureWithSchemaConflictAnnotation
    {
        @PerformsWrites
        @Procedure( mode = Mode.SCHEMA )
        public void shouldNotCompile()
        {
        }
    }

    private CallableProcedure.BasicProcedure procedureWithSignature( final ProcedureSignature signature )
    {
        return new CallableProcedure.BasicProcedure( signature )
        {
            @Override
            public RawIterator<Object[], ProcedureException> apply(
                    Context ctx, Object[] input, ResourceTracker resourceTracker )
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
            public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input, ResourceTracker resourceTracker )
            {
                return RawIterator.<Object[], ProcedureException>of( input );
            }
        };
    }
}
