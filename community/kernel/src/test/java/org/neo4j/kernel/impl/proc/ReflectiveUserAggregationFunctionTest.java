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

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.api.proc.UserFunctionSignature.functionSignature;

public class ReflectiveUserAggregationFunctionTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ReflectiveProcedureCompiler procedureCompiler;
    private ComponentRegistry components;

    @Before
    public void setUp() throws Exception
    {
        components = new ComponentRegistry();
        procedureCompiler = new ReflectiveProcedureCompiler( new TypeMappers(), components, components,
                NullLog.getInstance(), ProcedureConfig.DEFAULT );
    }

    @Test
    public void shouldCompileAggregationFunction() throws Throwable
    {
        // When
        List<CallableUserAggregationFunction> function = compile( SingleAggregationFunction.class );

        // Then
        assertEquals( 1, function.size() );
        assertThat( function.get( 0 ).signature(), Matchers.equalTo(
                functionSignature( "org", "neo4j", "kernel", "impl", "proc", "collectCool" )
                        .in( "name", Neo4jTypes.NTString )
                        .out( Neo4jTypes.NTList( Neo4jTypes.NTAny ) )
                        .build() ) );
    }

    @Test
    public void shouldRunAggregationFunction() throws Throwable
    {
        // Given
        CallableUserAggregationFunction func = compile( SingleAggregationFunction.class ).get( 0 );

        // When
        CallableUserAggregationFunction.Aggregator aggregator = func.create( new BasicContext() );

        aggregator.update( new Object[]{"Harry"} );
        aggregator.update( new Object[]{"Bonnie"} );
        aggregator.update( new Object[]{"Sally"} );
        aggregator.update( new Object[]{"Clyde"} );

        // Then
        assertThat( aggregator.result(), equalTo( Arrays.asList( "Bonnie", "Clyde" ) ) );
    }

    @Test
    public void shouldInjectLogging() throws KernelException
    {
        // Given
        Log log = spy( Log.class );
        components.register( Log.class, ctx -> log );
        CallableUserAggregationFunction
                function = procedureCompiler.compileAggregationFunction( LoggingFunction.class ).get( 0 );

        // When
        CallableUserAggregationFunction.Aggregator aggregator = function.create( new BasicContext() );
        aggregator.update( new Object[]{} );
        aggregator.result();

        // Then
        verify( log ).debug( "1" );
        verify( log ).info( "2" );
        verify( log ).warn( "3" );
        verify( log ).error( "4" );
    }

    @Test
    public void shouldIgnoreClassesWithNoFunctions() throws Throwable
    {
        // When
        List<CallableUserAggregationFunction> functions = compile( PrivateConstructorButNoFunctions.class );

        // Then
        assertEquals( 0, functions.size() );
    }

    @Test
    public void shouldRunClassWithMultipleFunctionsDeclared() throws Throwable
    {
        // Given
        List<CallableUserAggregationFunction> compiled = compile( MultiFunction.class );
        CallableUserAggregationFunction f1 = compiled.get( 0 );
        CallableUserAggregationFunction f2 = compiled.get( 1 );

        // When
        CallableUserAggregationFunction.Aggregator f1Aggregator = f1.create( new BasicContext() );
        f1Aggregator.update( new Object[]{"Bonnie"} );
        f1Aggregator.update( new Object[]{"Clyde"} );
        CallableUserAggregationFunction.Aggregator f2Aggregator = f2.create( new BasicContext() );
        f2Aggregator.update( new Object[]{"Bonnie", 1337L} );
        f2Aggregator.update( new Object[]{"Bonnie", 42L} );

        // Then
        assertThat( f1Aggregator.result(), equalTo( Arrays.asList( "Bonnie", "Clyde" ) ) );
        assertThat( ((Map) f2Aggregator.result()).get( "Bonnie" ), equalTo( 1337L ) );
    }

    @Test
    public void shouldGiveHelpfulErrorOnConstructorThatRequiresArgument() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Unable to find a usable public no-argument constructor " +
                                 "in the class `WierdConstructorFunction`. Please add a " +
                                 "valid, public constructor, recompile the class and try again." );

        // When
        compile( WierdConstructorFunction.class );
    }

    @Test
    public void shouldGiveHelpfulErrorOnNoPublicConstructor() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Unable to find a usable public no-argument constructor " +
                                 "in the class `PrivateConstructorFunction`. Please add " +
                                 "a valid, public constructor, recompile the class and try again." );

        // When
        compile( PrivateConstructorFunction.class );
    }

    @Test
    public void shouldNotAllowVoidOutput() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Don't know how to map `void` to the Neo4j Type System." );

        // When
        compile( FunctionWithVoidOutput.class );
    }

    @Test
    public void shouldNotAllowNonVoidUpdate() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Update method 'update' in VoidOutput has type 'long' but must have return type 'void'." );

        // When
        compile( FunctionWithNonVoidUpdate.class );
    }

    @Test
    public void shouldNotAllowMissingAnnotations() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Class 'MissingAggregator' must contain methods annotated with " +
                "both '@UserAggregationResult' as well as '@UserAggregationUpdate'." );

        // When
        compile( FunctionWithMissingAnnotations.class );
    }

    @Test
    public void shouldNotAllowMultipleUpdateAnnotations() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Class 'MissingAggregator' contains multiple methods annotated " +
                "with '@UserAggregationUpdate'." );

        // When
        compile( FunctionWithDuplicateUpdateAnnotations.class );
    }

    @Test
    public void shouldNotAllowMultipleResultAnnotations() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Class 'MissingAggregator' contains multiple methods annotated " +
                "with '@UserAggregationResult'." );

        // When
        compile( FunctionWithDuplicateResultAnnotations.class );
    }

    @Test
    public void shouldNotAllowNonPublicMethod() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Aggregation method 'test' in NonPublicTestMethod must be public." );

        // When
        compile( NonPublicTestMethod.class );
    }

    @Test
    public void shouldNotAllowNonPublicUpdateMethod() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Aggregation update method 'update' in InnerAggregator must be public." );

        // When
        compile( NonPublicUpdateMethod.class );
    }

    @Test
    public void shouldNotAllowNonPublicResultMethod() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Aggregation result method 'result' in InnerAggregator must be public." );

        // When
        compile( NonPublicResultMethod.class );
    }

    @Test
    public void shouldGiveHelpfulErrorOnFunctionReturningInvalidType() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( String.format("Don't know how to map `char[]` to the Neo4j Type System.%n" +
                                 "Please refer to to the documentation for full details.%n" +
                                 "For your reference, known types are: [boolean, double, java.lang.Boolean, java.lang" +
                                 ".Double, java.lang.Long, java.lang.Number, java.lang.Object, java.lang.String, java" +
                                 ".util.List, java.util.Map, long]" ));

        // When
        compile( FunctionWithInvalidOutput.class ).get( 0 );
    }

    @Test
    public void shouldGiveHelpfulErrorOnContextAnnotatedStaticField() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( String.format("The field `gdb` in the class named `FunctionWithStaticContextAnnotatedField` is " +
                                 "annotated as a @Context field,%n" +
                                 "but it is static. @Context fields must be public, non-final and non-static,%n" +
                                 "because they are reset each time a procedure is invoked." ));

        // When
        compile( FunctionWithStaticContextAnnotatedField.class ).get( 0 );
    }

    @Test
    public void shouldAllowOverridingProcedureName() throws Throwable
    {
        // When
        CallableUserAggregationFunction method = compile( FunctionWithOverriddenName.class ).get( 0 );

        // Then
        assertEquals("org.mystuff.thisisActuallyTheName", method.signature().name().toString() );
    }

    @Test
    public void shouldNotAllowOverridingFunctionNameWithoutNamespace() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "It is not allowed to define functions in the root namespace please use a " +
                                 "namespace, e.g. `@UserFunction(\"org.example.com.singleName\")" );

        // When
        compile( FunctionWithSingleName.class ).get( 0 );
    }

    @Test
    public void shouldGiveHelpfulErrorOnNullMessageException() throws Throwable
    {
        // Given
        CallableUserAggregationFunction method = compile( FunctionThatThrowsNullMsgExceptionAtInvocation.class ).get( 0 );

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Failed to invoke function `org.neo4j.kernel.impl.proc.test`: " +
                                 "Caused by: java.lang.IndexOutOfBoundsException" );

        // When
        method.create( new BasicContext()).update( new Object[] {});
    }

    @Test
    public void shouldLoadWhiteListedFunction() throws Throwable
    {
        // Given
        procedureCompiler = new ReflectiveProcedureCompiler( new TypeMappers(), components, new ComponentRegistry(),
                NullLog.getInstance(), new ProcedureConfig( Config.defaults( GraphDatabaseSettings.procedure_whitelist,
                "org.neo4j.kernel.impl.proc.collectCool" ) ) );

        CallableUserAggregationFunction method = compile( SingleAggregationFunction.class ).get( 0 );

        // Expect
        CallableUserAggregationFunction.Aggregator created = method.create( new BasicContext() );
        created.update( new Object[]{"Bonnie"} );
        assertThat(created.result(), equalTo( Collections.singletonList( "Bonnie" ) ) );
    }

    @Test
    public void shouldNotLoadNoneWhiteListedFunction() throws Throwable
    {
        // Given
        Log log = spy(Log.class);
        procedureCompiler = new ReflectiveProcedureCompiler( new TypeMappers(), components, new ComponentRegistry(),
                log, new ProcedureConfig( Config.defaults( GraphDatabaseSettings.procedure_whitelist, "WrongName" ) ) );

        List<CallableUserAggregationFunction> method = compile( SingleAggregationFunction.class );
        verify( log ).warn( "The function 'org.neo4j.kernel.impl.proc.collectCool' is not on the whitelist and won't be loaded." );
        assertThat( method.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldNotLoadAnyFunctionIfConfigIsEmpty() throws Throwable
    {
        // Given
        Log log = spy(Log.class);
        procedureCompiler = new ReflectiveProcedureCompiler( new TypeMappers(), components, new ComponentRegistry(),
                log, new ProcedureConfig( Config.defaults( GraphDatabaseSettings.procedure_whitelist, "" ) ) );

        List<CallableUserAggregationFunction> method = compile( SingleAggregationFunction.class );
        verify( log ).warn( "The function 'org.neo4j.kernel.impl.proc.collectCool' is not on the whitelist and won't be loaded." );
        assertThat( method.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldSupportFunctionDeprecation() throws Throwable
    {
        // Given
        Log log = mock(Log.class);
        ReflectiveProcedureCompiler procedureCompiler = new ReflectiveProcedureCompiler( new TypeMappers(), components,
                new ComponentRegistry(), log, ProcedureConfig.DEFAULT );

        // When
        List<CallableUserAggregationFunction> funcs = procedureCompiler.compileAggregationFunction( FunctionWithDeprecation.class );

        // Then
        verify( log ).warn( "Use of @UserAggregationFunction(deprecatedBy) without @Deprecated in org.neo4j.kernel.impl.proc.badFunc" );
        verifyNoMoreInteractions( log );
        for ( CallableUserAggregationFunction func : funcs )
        {
            String name = func.signature().name().name();
            func.create( new BasicContext());
            switch ( name )
            {
            case "newFunc":
                assertFalse( "Should not be deprecated", func.signature().deprecated().isPresent() );
                break;
            case "oldFunc":
            case "badFunc":
                assertTrue( "Should be deprecated", func.signature().deprecated().isPresent() );
                assertThat( func.signature().deprecated().get(), equalTo( "newFunc" ) );
                break;
            default:
                fail( "Unexpected function: " + name );
            }
        }
    }

    public static class SingleAggregationFunction
    {
        @UserAggregationFunction
        public CoolPeopleAggregator collectCool()
        {
            return new CoolPeopleAggregator();
        }
    }

    public static class CoolPeopleAggregator
    {
        private List<String> coolPeople = new ArrayList<>();

        @UserAggregationUpdate
        public void update( @Name( "name" ) String name )
        {
            if ( name.equals( "Bonnie" ) || name.equals( "Clyde" ) )
            {
                coolPeople.add( name );
            }
        }

        @UserAggregationResult
        public List<String> result()
        {
            return coolPeople;
        }
    }

    public static class FunctionWithVoidOutput
    {
        @UserAggregationFunction
        public VoidOutput voidOutput()
        {
            return new VoidOutput();
        }

        public static class VoidOutput
        {
            @UserAggregationUpdate
            public void update()
            {
            }

            @UserAggregationResult
            public void result()
            {
            }
        }
    }

    public static class FunctionWithMissingAnnotations
    {
        @UserAggregationFunction
        public MissingAggregator test()
        {
            return new MissingAggregator();
        }

        public static class MissingAggregator
        {
            public void update()
            {
            }

            public String result()
            {
                return "test";
            }
        }
    }

    public static class FunctionWithDuplicateUpdateAnnotations
    {
        @UserAggregationFunction
        public MissingAggregator test()
        {
            return new MissingAggregator();
        }

        public static class MissingAggregator
        {
            @UserAggregationUpdate
            public void update1()
            {
            }

            @UserAggregationUpdate
            public void update2()
            {
            }

            @UserAggregationResult
            public String result()
            {
                return "test";
            }
        }
    }

    public static class FunctionWithDuplicateResultAnnotations
    {
        @UserAggregationFunction
        public MissingAggregator test()
        {
            return new MissingAggregator();
        }

        public static class MissingAggregator
        {
            @UserAggregationUpdate
            public void update()
            {
            }

            @UserAggregationResult
            public String result1()
            {
                return "test";
            }

            @UserAggregationResult
            public String result2()
            {
                return "test";
            }
        }
    }

    public static class FunctionWithNonVoidUpdate
    {
        @UserAggregationFunction
        public VoidOutput voidOutput()
        {
            return new VoidOutput();
        }

        public static class VoidOutput
        {
            @UserAggregationUpdate
            public long update()
            {
                return 42L;
            }

            @UserAggregationResult
            public long result()
            {
                return 42L;
            }
        }
    }

    public static class LoggingFunction
    {
        @Context
        public Log log;

        @UserAggregationFunction
        public LoggingAggregator log()
        {
            return new LoggingAggregator( );
        }

        public  class LoggingAggregator
        {
            @UserAggregationUpdate
            public void logAround()
            {
                log.debug( "1" );
                log.info( "2" );
                log.warn( "3" );
                log.error( "4" );
            }

            @UserAggregationResult
            public long result()
            {
                return 1337L;
            }
        }
    }

    public static class MapAggregator
    {
        private Map<String,Object> map = new HashMap<>();

        @UserAggregationUpdate
        public void update( @Name( "name" ) String name, @Name( "value" ) long value )
        {
            Long prev = (Long) map.getOrDefault( name, 0L );
            if ( value > prev )
            {
                map.put( name, value );
            }
        }

        @UserAggregationResult
        public Map<String,Object> result()
        {
            return map;
        }
    }

    public static class MultiFunction
    {
        @UserAggregationFunction
        public CoolPeopleAggregator collectCool()
        {
            return new CoolPeopleAggregator();
        }

        @UserAggregationFunction
        public MapAggregator collectMap()
        {
            return new MapAggregator();
        }
    }

    public static class WierdConstructorFunction
    {
        public WierdConstructorFunction( WierdConstructorFunction wat )
        {

        }

        @UserAggregationFunction
        public CoolPeopleAggregator collectCool()
        {
            return new CoolPeopleAggregator();
        }
    }

    public static class FunctionWithInvalidOutput
    {
        @UserAggregationFunction
        public InvalidAggregator test()
        {
            return new InvalidAggregator();
        }

        public static class InvalidAggregator
        {
            @UserAggregationUpdate
            public void update()
            {
                //dd nothing
            }

            @UserAggregationResult
            public char[] result()
            {
                return "Testing" .toCharArray();
            }
        }

    }

    public static class FunctionWithStaticContextAnnotatedField
    {
        @Context
        public static GraphDatabaseService gdb;

        @UserAggregationFunction
        public InvalidAggregator test()
        {
            return new InvalidAggregator();
        }

        public static class InvalidAggregator
        {

            @UserAggregationUpdate
            public void update()
            {
                //dd nothing
            }

            @UserAggregationResult
            public String result()
            {
                return "Testing";
            }
        }
    }

    public static class FunctionThatThrowsNullMsgExceptionAtInvocation
    {
        @UserAggregationFunction
        public ThrowingAggregator test()
        {
            return new ThrowingAggregator();
        }

        public static class ThrowingAggregator
        {
            @UserAggregationUpdate
            public void update()
            {
                throw new IndexOutOfBoundsException(  );
            }

            @UserAggregationResult
            public String result()
            {
                return "Testing";
            }
        }
    }

    public static class PrivateConstructorFunction
    {
        private PrivateConstructorFunction()
        {

        }

        @UserAggregationFunction
        public CoolPeopleAggregator collectCool()
        {
            return new CoolPeopleAggregator();
        }
    }

    public static class PrivateConstructorButNoFunctions
    {
        private PrivateConstructorButNoFunctions()
        {

        }

        public String thisIsNotAFunction()
        {
            return null;
        }
    }

    public static class FunctionWithOverriddenName
    {
        @UserAggregationFunction( "org.mystuff.thisisActuallyTheName" )
        public CoolPeopleAggregator collectCool()
        {
            return new CoolPeopleAggregator();
        }
    }

    public static class FunctionWithSingleName
    {
        @UserAggregationFunction( "singleName" )
        public CoolPeopleAggregator collectCool()
        {
            return new CoolPeopleAggregator();
        }
    }

    public static class FunctionWithDeprecation
    {
        @UserAggregationFunction()
        public CoolPeopleAggregator newFunc()
        {
            return new CoolPeopleAggregator();
        }

        @Deprecated
        @UserAggregationFunction( deprecatedBy = "newFunc" )
        public CoolPeopleAggregator oldFunc()
        {
            return new CoolPeopleAggregator();
        }

        @UserAggregationFunction( deprecatedBy = "newFunc" )
        public CoolPeopleAggregator badFunc()
        {
            return new CoolPeopleAggregator();
        }
    }

    public static class NonPublicTestMethod
    {
        @UserAggregationFunction
        InnerAggregator test()
        {
            return new InnerAggregator();
        }

        public static class InnerAggregator
        {
            @UserAggregationUpdate
            public void update()
            {
            }

            @UserAggregationResult
            public String result()
            {
                return "Testing";
            }
        }
    }

    public static class NonPublicUpdateMethod
    {
        @UserAggregationFunction
        public InnerAggregator test()
        {
            return new InnerAggregator();
        }

        public static class InnerAggregator
        {
            @UserAggregationUpdate
            void update()
            {
            }

            @UserAggregationResult
            public String result()
            {
                return "Testing";
            }
        }
    }

    public static class NonPublicResultMethod
    {
        @UserAggregationFunction
        public InnerAggregator test()
        {
            return new InnerAggregator();
        }

        public static class InnerAggregator
        {
            @UserAggregationUpdate
            public void update()
            {
            }

            @UserAggregationResult
            String result()
            {
                return "Testing";
            }
        }
    }

    private List<CallableUserAggregationFunction> compile( Class<?> clazz ) throws KernelException
    {
        return procedureCompiler.compileAggregationFunction( clazz );
    }
}
