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

import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.procedure.UserFunction;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.allOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings( "WeakerAccess" )
public class ResourceInjectionTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ReflectiveProcedureCompiler compiler;

    private Log log = mock(Log.class);

    public static String notAvailableMessage( String procName )
    {
        return argThat( notAvailableMessageMatcher( procName ) );
    }

    private static Matcher<String> notAvailableMessageMatcher( String procName )
    {
        return allOf( containsString( procName ), containsString( "unavailable" ) );
    }

    @Before
    public void setUp()
    {
        ComponentRegistry safeComponents = new ComponentRegistry();
        ComponentRegistry allComponents = new ComponentRegistry();
        safeComponents.register( MyAwesomeAPI.class, ctx -> new MyAwesomeAPI() );
        allComponents.register( MyAwesomeAPI.class, ctx -> new MyAwesomeAPI() );
        allComponents.register( MyUnsafeAPI.class, ctx -> new MyUnsafeAPI() );

        compiler = new ReflectiveProcedureCompiler( new TypeMappers(), safeComponents, allComponents, log,
                ProcedureConfig.DEFAULT );
    }

    @Test
    public void shouldCompileAndRunProcedure() throws Throwable
    {
        // Given
        CallableProcedure proc =
                compiler.compileProcedure( ProcedureWithInjectedAPI.class, Optional.empty(), true ).get( 0 );

        // Then
        List<Object[]> out = Iterators.asList( proc.apply( new BasicContext(), new Object[0] ) );

        // Then
        assertThat( out.get( 0 ), equalTo( new Object[]{"Bonnie"} ) );
        assertThat( out.get( 1 ), equalTo( new Object[]{"Clyde"} ) );
    }

    @Test
    public void shouldFailNicelyWhenUnknownAPI() throws Throwable
    {
        //When
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Unable to set up injection for procedure `ProcedureWithUnknownAPI`, " +
                "the field `api` has type `class org.neo4j.kernel.impl.proc.ResourceInjectionTest$UnknownAPI` " +
                "which is not a known injectable component." );

        // Then
        compiler.compileProcedure( ProcedureWithUnknownAPI.class, Optional.empty(), true );
    }

    @Test
    public void shouldCompileAndRunUnsafeProcedureUnsafeMode() throws Throwable
    {
        // Given
        CallableProcedure proc =
                compiler.compileProcedure( ProcedureWithUnsafeAPI.class, Optional.empty(), true ).get( 0 );

        // Then
        List<Object[]> out = Iterators.asList( proc.apply( new BasicContext(), new Object[0] ) );

        // Then
        assertThat( out.get( 0 ), equalTo( new Object[]{"Morpheus"} ) );
        assertThat( out.get( 1 ), equalTo( new Object[]{"Trinity"} ) );
        assertThat( out.get( 2 ), equalTo( new Object[]{"Neo"} ) );
        assertThat( out.get( 3 ), equalTo( new Object[]{"Emil"} ) );
    }

    @Test
    public void shouldFailNicelyWhenUnsafeAPISafeMode() throws Throwable
    {
        //When
        List<CallableProcedure> procList =
                compiler.compileProcedure( ProcedureWithUnsafeAPI.class, Optional.empty(), false );
        verify( log ).warn( notAvailableMessage( "org.neo4j.kernel.impl.proc.listCoolPeople" ) );

        assertThat( procList.size(), equalTo( 1 ) );
        try
        {
            procList.get( 0 ).apply( new BasicContext(), new Object[0] );
            fail();
        }
        catch ( ProcedureException e )
        {
            assertThat( e.getMessage(), notAvailableMessageMatcher( "org.neo4j.kernel.impl.proc.listCoolPeople" ) );
        }
    }

    @Test
    public void shouldCompileAndRunUserFunctions() throws Throwable
    {
        // Given
        CallableUserFunction proc =
                compiler.compileFunction( FunctionWithInjectedAPI.class).get( 0 );

        // When
        Object out = proc.apply( new BasicContext(), new Object[0] );

        // Then
        assertThat( out, equalTo( "[Bonnie, Clyde]" ) );
    }

    @Test
    public void shouldFailNicelyWhenFunctionUsesUnknownAPI() throws Throwable
    {
        //When
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Unable to set up injection for procedure `FunctionWithUnknownAPI`, " +
                "the field `api` has type `class org.neo4j.kernel.impl.proc.ResourceInjectionTest$UnknownAPI` " +
                "which is not a known injectable component." );

        // Then
        compiler.compileFunction( FunctionWithUnknownAPI.class );
    }

    @Test
    public void shouldFailNicelyWhenUnsafeAPISafeModeFunction() throws Throwable
    {
        //When
        List<CallableUserFunction> procList =
                compiler.compileFunction( FunctionWithUnsafeAPI.class);
        verify( log ).warn( notAvailableMessage( "org.neo4j.kernel.impl.proc.listCoolPeople" ) );

        assertThat( procList.size(), equalTo( 1 ) );
        try
        {
            procList.get( 0 ).apply( new BasicContext(), new Object[0] );
            fail();
        }
        catch ( ProcedureException e )
        {
            assertThat( e.getMessage(), notAvailableMessageMatcher( "org.neo4j.kernel.impl.proc.listCoolPeople" ) );
        }
    }

    @Test
    public void shouldCompileAndRunUserAggregationFunctions() throws Throwable
    {
        // Given
        CallableUserAggregationFunction proc =
                compiler.compileAggregationFunction( AggregationFunctionWithInjectedAPI.class).get( 0 );
        // When
        proc.create( new BasicContext() ).update( new Object[]{} );
        Object out = proc.create( new BasicContext() ).result();

        // Then
        assertThat( out, equalTo( "[Bonnie, Clyde]" ) );
    }

    @Test
    public void shouldFailNicelyWhenAggregationFunctionUsesUnknownAPI() throws Throwable
    {
        //When
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Unable to set up injection for procedure `AggregationFunctionWithUnknownAPI`, " +
                "the field `api` has type `class org.neo4j.kernel.impl.proc.ResourceInjectionTest$UnknownAPI` " +
                "which is not a known injectable component." );

        // Then
        compiler.compileAggregationFunction( AggregationFunctionWithUnknownAPI.class );
    }

    @Test
    public void shouldFailNicelyWhenUnsafeAPISafeModeAggregationFunction() throws Throwable
    {
        //When
        List<CallableUserAggregationFunction> procList =
                compiler.compileAggregationFunction( AggregationFunctionWithUnsafeAPI.class);
        verify( log ).warn( notAvailableMessage( "org.neo4j.kernel.impl.proc.listCoolPeople" ) );

        assertThat( procList.size(), equalTo( 1 ) );
        try
        {
            procList.get(0).create( new BasicContext() ).update( new Object[]{} );
            Object out = procList.get(0).create( new BasicContext() ).result();
            fail();
        }
        catch ( ProcedureException e )
        {
            assertThat( e.getMessage(), notAvailableMessageMatcher( "org.neo4j.kernel.impl.proc.listCoolPeople" ) );
        }
    }

    @Test
    public void shouldFailNicelyWhenAllUsesUnsafeAPI() throws Throwable
    {
        //When
        compiler.compileFunction( FunctionsAndProcedureUnsafe.class );
        compiler.compileProcedure( FunctionsAndProcedureUnsafe.class, Optional.empty(), false );
        compiler.compileAggregationFunction( FunctionsAndProcedureUnsafe.class );
        // Then

        verify( log ).warn( notAvailableMessage( "org.neo4j.kernel.impl.proc.safeUserFunctionInUnsafeAPIClass" ) );
        verify( log ).warn( notAvailableMessage( "org.neo4j.kernel.impl.proc.listCoolPeopleProcedure" ) );
        // With extra ' ' space at the end to distinguish from procedure form:
        verify( log ).warn( notAvailableMessage( "org.neo4j.kernel.impl.proc.listCoolPeople " ) );
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

    public static class FunctionWithInjectedAPI
    {
        @Context
        public MyAwesomeAPI api;

        @UserFunction
        public String listCoolPeople()
        {
            return api.listCoolPeople().toString();
        }
    }

    public static class AggregationFunctionWithInjectedAPI
    {
        @Context
        public MyAwesomeAPI api;

        @UserAggregationFunction
        public VoidOutput listCoolPeople()
        {
            return new VoidOutput( api );
        }

        public static class VoidOutput
        {
            private MyAwesomeAPI api;

            public VoidOutput( MyAwesomeAPI api )
            {
                this.api = api;
            }

            @UserAggregationUpdate
            public void update()
            {
            }

            @UserAggregationResult
            public String result()
            {
                return  api.listCoolPeople().toString();
            }
        }
    }

    public static class ProcedureWithUnknownAPI
    {
        @Context
        public UnknownAPI api;

        @Procedure
        public Stream<MyOutputRecord> listCoolPeople()
        {
            return api.listCoolPeople().stream().map( MyOutputRecord::new );
        }
    }

    public static class FunctionWithUnknownAPI
    {
        @Context
        public UnknownAPI api;

        @UserFunction
        public String listCoolPeople()
        {
            return api.listCoolPeople().toString();
        }
    }

    public static class AggregationFunctionWithUnknownAPI
    {
        @Context
        public UnknownAPI api;

        @UserAggregationFunction
        public VoidOutput listCoolPeople()
        {
            return new VoidOutput( api );
        }

        public static class VoidOutput
        {
            private UnknownAPI api;

            public VoidOutput( UnknownAPI api )
            {
                this.api = api;
            }

            @UserAggregationUpdate
            public void update()
            {
            }

            @UserAggregationResult
            public String result()
            {
                return  api.listCoolPeople().toString();
            }
        }
    }

    public static class ProcedureWithUnsafeAPI
    {
        @Context
        public MyUnsafeAPI api;

        @Procedure
        public Stream<MyOutputRecord> listCoolPeople()
        {
            return api.listCoolPeople().stream().map( MyOutputRecord::new );
        }
    }

    public static class FunctionWithUnsafeAPI
    {
        @Context
        public MyUnsafeAPI api;

        @UserFunction
        public String listCoolPeople()
        {
            return api.listCoolPeople().toString();
        }
    }
    public static class AggregationFunctionWithUnsafeAPI
    {
        @Context
        public MyUnsafeAPI api;

        @UserAggregationFunction
        public VoidOutput listCoolPeople()
        {
            return new VoidOutput( api );
        }

        public static class VoidOutput
        {
            private MyUnsafeAPI api;

            public VoidOutput( MyUnsafeAPI api )
            {
                this.api = api;
            }

            @UserAggregationUpdate
            public void update()
            {
            }

            @UserAggregationResult
            public String result()
            {
                return  api.listCoolPeople().toString();
            }
        }
    }

    public static class FunctionsAndProcedureUnsafe
    {
        @Context
        public MyUnsafeAPI api;

        @UserAggregationFunction
        public VoidOutput listCoolPeople()
        {
            return new VoidOutput( api );
        }

        public static class VoidOutput
        {
            private MyUnsafeAPI api;

            public VoidOutput( MyUnsafeAPI api )
            {
                this.api = api;
            }

            @UserAggregationUpdate
            public void update()
            {
            }

            @UserAggregationResult
            public String result()
            {
                return api.listCoolPeople().toString();
            }
        }

        @Procedure
        public Stream<MyOutputRecord> listCoolPeopleProcedure()
        {
            return api.listCoolPeople().stream().map( MyOutputRecord::new );
        }

        @UserFunction
        public String safeUserFunctionInUnsafeAPIClass()
        {
            return "a safe function";
        }
    }
}
