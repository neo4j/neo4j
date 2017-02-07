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
package org.neo4j.kernel.impl.proc;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings( "WeakerAccess" )
public class ResourceInjectionTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private Log log = mock(Log.class);

    @Test
    public void shouldCompileAndRunProcedure() throws Throwable
    {
        // Given
        CallableProcedure proc = compile( ProcedureWithInjectedAPI.class, true ).get( 0 );

        // Then
        List<Object[]> out = Iterators.asList( proc.apply( new BasicContext(), new Object[0] ) );

        // Then
        assertThat( out.get( 0 ), equalTo( (new Object[]{"Bonnie"}) ) );
        assertThat( out.get( 1 ), equalTo( (new Object[]{"Clyde"}) ) );
    }

    @Test
    public void shouldFailNicelyWhenUnknownAPI() throws Throwable
    {
        //When
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Unable to set up injection for procedure `procedureWithUnknownAPI`, " +
                "the field `api` has type `class org.neo4j.kernel.impl.proc.ResourceInjectionTest$UnknownAPI` " +
                "which is not a known injectable component." );

        // Then
        compile( procedureWithUnknownAPI.class, true );

    }

    @Test
    public void shouldCompileAndRunUnsafeProcedureUnsafeMode() throws Throwable
    {
        // Given
        CallableProcedure proc = compile( procedureWithUnsafeAPI.class, true ).get( 0 );

        // Then
        List<Object[]> out = Iterators.asList( proc.apply( new BasicContext(), new Object[0] ) );

        // Then
        assertThat( out.get( 0 ), equalTo( (new Object[]{"Morpheus"}) ) );
        assertThat( out.get( 1 ), equalTo( (new Object[]{"Trinity"}) ) );
        assertThat( out.get( 2 ), equalTo( (new Object[]{"Neo"}) ) );
        assertThat( out.get( 3 ), equalTo( (new Object[]{"Emil"}) ) );
    }

    @Test
    public void shouldFailNicelyWhenUnsafeAPISafeMode() throws Throwable
    {
        //When
        List<CallableProcedure> procList = compile( procedureWithUnsafeAPI.class, false );
        verify( log )
                .warn( "Unable to set up injection for procedure `procedureWithUnsafeAPI`, the field `api` has type" +
                        " `class org.neo4j.kernel.impl.proc.ResourceInjectionTest$MyUnsafeAPI`" +
                        " which is not a known injectable component." );

        assertThat( procList.size(), equalTo( 1 ) );
        try
        {
            procList.get( 0 ).apply( new BasicContext(), new Object[0] );
            fail();
        }
        catch ( ProcedureException e )
        {
            assertThat( e.getMessage(), containsString(
                    "org.neo4j.kernel.impl.proc.listCoolPeopleis not " +
                            "available due to not having unrestricted access rights, check configuration." ) );
        }

    }

    public static class MyOutputRecord
    {
        public String name;

        public MyOutputRecord( String name )
        {
            this.name = name;
        }
    }

    public static class MyAwesomeAPI
    {

        List<String> listCoolPeople()
        {
            return asList( "Bonnie", "Clyde" );
        }
    }

    public static class UnknownAPI
    {

        List<String> listCoolPeople()
        {
            return singletonList( "booh!" );
        }
    }

    public static class MyUnsafeAPI
    {
        List<String> listCoolPeople()
        {
            return asList( "Morpheus", "Trinity", "Neo", "Emil" );
        }
    }

    public static class ProcedureWithInjectedAPI
    {
        @Context
        public MyAwesomeAPI api;

        @Procedure
        public Stream<MyOutputRecord> listCoolPeople()
        {
            return api.listCoolPeople().stream().map( MyOutputRecord::new );
        }
    }

    public static class procedureWithUnknownAPI
    {
        @Context
        public UnknownAPI api;

        @Procedure
        public Stream<MyOutputRecord> listCoolPeople()
        {
            return api.listCoolPeople().stream().map( MyOutputRecord::new );
        }
    }

    public static class procedureWithUnsafeAPI
    {
        @Context
        public MyUnsafeAPI api;

        @Procedure
        public Stream<MyOutputRecord> listCoolPeople()
        {
            return api.listCoolPeople().stream().map( MyOutputRecord::new );
        }
    }

    private List<CallableProcedure> compile( Class<?> clazz, boolean safe ) throws KernelException
    {
        ComponentRegistry safeComponents = new ComponentRegistry();
        ComponentRegistry allComponents = new ComponentRegistry();
        safeComponents.register( MyAwesomeAPI.class, (ctx) -> new MyAwesomeAPI() );
        allComponents.register( MyAwesomeAPI.class, (ctx) -> new MyAwesomeAPI() );
        allComponents.register( MyUnsafeAPI.class, (ctx) -> new MyUnsafeAPI() );

        return new ReflectiveProcedureCompiler( new TypeMappers(), safeComponents, allComponents, log,
                ProcedureConfig.DEFAULT ).compileProcedure( clazz, Optional.empty(), safe );
    }
}
