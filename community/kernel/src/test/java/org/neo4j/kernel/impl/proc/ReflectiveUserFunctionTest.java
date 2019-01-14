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

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.internal.kernel.api.procs.UserFunctionSignature.functionSignature;

public class ReflectiveUserFunctionTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ReflectiveProcedureCompiler procedureCompiler;
    private ComponentRegistry components;

    @Before
    public void setUp()
    {
        components = new ComponentRegistry();
        procedureCompiler = new ReflectiveProcedureCompiler( new TypeMappers(), components, components,
                NullLog.getInstance(), ProcedureConfig.DEFAULT );
    }

    @Test
    public void shouldInjectLogging() throws KernelException
    {
        // Given
        Log log = spy( Log.class );
        components.register( Log.class, ctx -> log );
        CallableUserFunction function = procedureCompiler.compileFunction( LoggingFunction.class ).get( 0 );

        // When
        function.apply( new BasicContext(), new AnyValue[0] );

        // Then
        verify( log ).debug( "1" );
        verify( log ).info( "2" );
        verify( log ).warn( "3" );
        verify( log ).error( "4" );
    }

    @Test
    public void shouldCompileFunction() throws Throwable
    {
        // When
        List<CallableUserFunction> function = compile( SingleReadOnlyFunction.class );

        // Then
        assertEquals( 1, function.size() );
        assertThat( function.get( 0 ).signature(), Matchers.equalTo(
                functionSignature( "org", "neo4j", "kernel", "impl", "proc", "listCoolPeople" )
                        .out( Neo4jTypes.NTList( Neo4jTypes.NTAny ) )
                        .build() ) );
    }

    @Test
    public void shouldRunSimpleReadOnlyFunction() throws Throwable
    {
        // Given
        CallableUserFunction func = compile( SingleReadOnlyFunction.class ).get( 0 );

        // When
        Object out = func.apply( new BasicContext(), new AnyValue[0] );

        // Then
        assertThat(out, equalTo( ValueUtils.of( Arrays.asList("Bonnie", "Clyde") ) ) );
    }

    @Test
    public void shouldIgnoreClassesWithNoFunctions() throws Throwable
    {
        // When
        List<CallableUserFunction> functions = compile( PrivateConstructorButNoFunctions.class );

        // Then
        assertEquals( 0, functions.size() );
    }

    @Test
    public void shouldRunClassWithMultipleFunctionsDeclared() throws Throwable
    {
        // Given
        List<CallableUserFunction> compiled = compile( ReflectiveUserFunctionTest.MultiFunction.class );
        CallableUserFunction bananaPeople = compiled.get( 0 );
        CallableUserFunction coolPeople = compiled.get( 1 );

        // When
        Object coolOut = coolPeople.apply( new BasicContext(), new AnyValue[0] );
        Object bananaOut = bananaPeople.apply( new BasicContext(), new AnyValue[0] );

        // Then
        assertThat( coolOut , equalTo(ValueUtils.of( Arrays.asList("Bonnie", "Clyde") ) ) );

        assertThat( ((MapValue) bananaOut).get("foo"), equalTo( ValueUtils.of( Arrays.asList( "bar", "baz" ) ) ) );
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
    public void shouldGiveHelpfulErrorOnFunctionReturningInvalidType() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( String.format("Don't know how to map `char[]` to the Neo4j Type System.%n" +
                                 "Please refer to to the documentation for full details.%n" +
                                 "For your reference, known types are: [boolean, byte[], double, java.lang.Boolean, " +
                                 "java.lang.Double, java.lang.Long, java.lang.Number, java.lang.Object, " +
                                 "java.lang.String, java.time.LocalDate, java.time.LocalDateTime, " +
                                 "java.time.LocalTime, java.time.OffsetTime, java.time.ZonedDateTime, " +
                                 "java.time.temporal.TemporalAmount, java.util.List, java.util.Map, long]" ));

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
        CallableUserFunction proc = compile( FunctionWithOverriddenName.class ).get( 0 );

        // Then
        assertEquals("org.mystuff.thisisActuallyTheName", proc.signature().name().toString() );
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
        CallableUserFunction proc = compile( FunctionThatThrowsNullMsgExceptionAtInvocation.class ).get( 0 );

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Failed to invoke function `org.neo4j.kernel.impl.proc.throwsAtInvocation`: " +
                                 "Caused by: java.lang.IndexOutOfBoundsException" );

        // When
        proc.apply( new BasicContext(), new AnyValue[0] );
    }

    @Test
    public void shouldLoadWhiteListedFunction() throws Throwable
    {
        // Given
        procedureCompiler = new ReflectiveProcedureCompiler( new TypeMappers(), components, new ComponentRegistry(),
                NullLog.getInstance(), new ProcedureConfig( Config.defaults( GraphDatabaseSettings.procedure_whitelist,
                "org.neo4j.kernel.impl.proc.listCoolPeople" ) ) );

        CallableUserFunction method = compile( SingleReadOnlyFunction.class ).get( 0 );

        // Expect
        Object out = method.apply( new BasicContext(), new AnyValue[0] );
        assertThat(out, equalTo( ValueUtils.of( Arrays.asList("Bonnie", "Clyde") ) ) );
    }

    @Test
    public void shouldNotLoadNoneWhiteListedFunction() throws Throwable
    {
        // Given
        Log log = spy(Log.class);
        procedureCompiler = new ReflectiveProcedureCompiler( new TypeMappers(), components, new ComponentRegistry(),
                log, new ProcedureConfig( Config.defaults( GraphDatabaseSettings.procedure_whitelist, "WrongName" ) ) );

        List<CallableUserFunction> method = compile( SingleReadOnlyFunction.class );
        verify( log ).warn( "The function 'org.neo4j.kernel.impl.proc.listCoolPeople' is not on the whitelist and won't be loaded." );
        assertThat( method.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldNotLoadAnyFunctionIfConfigIsEmpty() throws Throwable
    {
        // Given
        Log log = spy(Log.class);
        procedureCompiler = new ReflectiveProcedureCompiler( new TypeMappers(), components, new ComponentRegistry(),
                log, new ProcedureConfig( Config.defaults( GraphDatabaseSettings.procedure_whitelist, "" ) ) );

        List<CallableUserFunction> method = compile( SingleReadOnlyFunction.class );
        verify( log ).warn( "The function 'org.neo4j.kernel.impl.proc.listCoolPeople' is not on the whitelist and won't be loaded." );
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
        List<CallableUserFunction> funcs = procedureCompiler.compileFunction( FunctionWithDeprecation.class );

        // Then
        verify( log ).warn( "Use of @UserFunction(deprecatedBy) without @Deprecated in org.neo4j.kernel.impl.proc.badFunc" );
        verifyNoMoreInteractions( log );
        for ( CallableUserFunction func : funcs )
        {
            String name = func.signature().name().name();
            func.apply( new BasicContext(), new AnyValue[0] );
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

    public static class LoggingFunction
    {
        @Context
        public Log log;

        @UserFunction
        public long logAround()
        {
            log.debug( "1" );
            log.info( "2" );
            log.warn( "3" );
            log.error( "4" );
            return -1L;
        }
    }

    public static class SingleReadOnlyFunction
    {
        @UserFunction
        public List<String> listCoolPeople()
        {
            return Arrays.asList("Bonnie", "Clyde");
        }
    }

    public static class FunctionWithVoidOutput
    {
        @UserFunction
        public void voidOutput()
        {
        }
    }

    public static class MultiFunction
    {
        @UserFunction
        public List<String> listCoolPeople()
        {
            return Arrays.asList("Bonnie", "Clyde");
        }

        @UserFunction
        public Map<String, Object> listBananaOwningPeople()
        {
            HashMap<String,Object> map = new HashMap<>();
            map.put("foo", Arrays.asList("bar", "baz"));
            return map;
        }
    }

    public static class WierdConstructorFunction
    {
        public WierdConstructorFunction( WierdConstructorFunction wat )
        {

        }

        @UserFunction
        public List<String> listCoolPeople()
        {
            return Arrays.asList("Bonnie", "Clyde");
        }
    }

    public static class FunctionWithInvalidOutput
    {
        @UserFunction
        public char[] test( )
        {
            return "Testing".toCharArray();
        }
    }

    public static class FunctionWithStaticContextAnnotatedField
    {
        @Context
        public static GraphDatabaseService gdb;

        @UserFunction
        public Object test( )
        {
            return null;
        }
    }

    public static class FunctionThatThrowsNullMsgExceptionAtInvocation
    {
        @UserFunction
        public String throwsAtInvocation( )
        {
            throw new IndexOutOfBoundsException();
        }
    }

    public static class PrivateConstructorFunction
    {
        private PrivateConstructorFunction()
        {

        }

        @UserFunction
        public List<String> listCoolPeople()
        {
            return Arrays.asList("Bonnie", "Clyde");
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
        @UserFunction( "org.mystuff.thisisActuallyTheName" )
        public Object somethingThatShouldntMatter()
        {
            return null;
        }

    }

    public static class FunctionWithSingleName
    {
        @UserFunction( "singleName" )
        public String blahDoesntMatterEither()
        {
            return null;
        }
    }

    public static class FunctionWithDeprecation
    {
        @UserFunction
        public Object newFunc()
        {
            return null;
        }

        @Deprecated
        @UserFunction( deprecatedBy = "newFunc" )
        public String oldFunc()
        {
            return null;
        }

        @UserFunction( deprecatedBy = "newFunc" )
        public Object badFunc()
        {
            return null;
        }
    }

    private List<CallableUserFunction> compile( Class<?> clazz ) throws KernelException
    {
        return procedureCompiler.compileFunction( clazz );
    }
}
