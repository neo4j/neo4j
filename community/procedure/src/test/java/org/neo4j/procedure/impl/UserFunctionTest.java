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
package org.neo4j.procedure.impl;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.virtual.MapValue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.internal.kernel.api.procs.UserFunctionSignature.functionSignature;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

@SuppressWarnings( {"WeakerAccess", "unused"} )
public class UserFunctionTest
{
    private ProcedureCompiler procedureCompiler;
    private ComponentRegistry components;
    private final DependencyResolver dependencyResolver = new Dependencies();
    private final ValueMapper<Object> valueMapper = new DefaultValueMapper( mock( InternalTransaction.class ) );

    @BeforeEach
    void setUp()
    {
        components = new ComponentRegistry();
        procedureCompiler = new ProcedureCompiler( new TypeCheckers(), components, components,
                NullLog.getInstance(), ProcedureConfig.DEFAULT );
    }

    @Test
    void shouldInjectLogging() throws KernelException
    {
        // Given
        Log log = spy( Log.class );
        components.register( Log.class, ctx -> log );
        CallableUserFunction function = procedureCompiler.compileFunction( LoggingFunction.class, false ).get( 0 );

        // When
        function.apply( prepareContext(), new AnyValue[0] );

        // Then
        verify( log ).debug( "1" );
        verify( log ).info( "2" );
        verify( log ).warn( "3" );
        verify( log ).error( "4" );
    }

    @Test
    void shouldCompileFunction() throws Throwable
    {
        // When
        List<CallableUserFunction> function = compile( SingleReadOnlyFunction.class );

        // Then
        assertEquals( 1, function.size() );
        assertThat( function.get( 0 ).signature(), Matchers.equalTo(
                functionSignature( "org", "neo4j", "procedure", "impl", "listCoolPeople" )
                        .out( Neo4jTypes.NTList( Neo4jTypes.NTAny ) )
                        .build() ) );
    }

    @Test
    void shouldRunSimpleReadOnlyFunction() throws Throwable
    {
        // Given
        CallableUserFunction func = compile( SingleReadOnlyFunction.class ).get( 0 );

        // When
        Object out = func.apply( prepareContext(), new AnyValue[0] );

        // Then
        assertThat(out, equalTo( ValueUtils.of( Arrays.asList("Bonnie", "Clyde") ) ) );
    }

    @Test
    void shouldIgnoreClassesWithNoFunctions() throws Throwable
    {
        // When
        List<CallableUserFunction> functions = compile( PrivateConstructorButNoFunctions.class );

        // Then
        assertEquals( 0, functions.size() );
    }

    @Test
    void shouldRunClassWithMultipleFunctionsDeclared() throws Throwable
    {
        // Given
        List<CallableUserFunction> compiled = compile( UserFunctionTest.MultiFunction.class );
        CallableUserFunction bananaPeople = compiled.get( 0 );
        CallableUserFunction coolPeople = compiled.get( 1 );

        // When
        Object coolOut = coolPeople.apply( prepareContext(), new AnyValue[0] );
        Object bananaOut = bananaPeople.apply( prepareContext(), new AnyValue[0] );

        // Then
        assertThat( coolOut , equalTo(ValueUtils.of( Arrays.asList("Bonnie", "Clyde") ) ) );

        assertThat( ((MapValue) bananaOut).get("foo"), equalTo( ValueUtils.of( Arrays.asList( "bar", "baz" ) ) ) );
    }

    @Test
    void shouldGiveHelpfulErrorOnConstructorThatRequiresArgument()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> compile( WierdConstructorFunction.class ) );
        assertThat( exception.getMessage(), equalTo( "Unable to find a usable public no-argument constructor " +
                                                    "in the class `WierdConstructorFunction`. Please add a " +
                                                    "valid, public constructor, recompile the class and try again." ) );
    }

    @Test
    void shouldGiveHelpfulErrorOnNoPublicConstructor()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> compile( PrivateConstructorFunction.class ) );
        assertThat( exception.getMessage(), equalTo( "Unable to find a usable public no-argument constructor " +
                                                    "in the class `PrivateConstructorFunction`. Please add " +
                                                    "a valid, public constructor, recompile the class and try again." ) );
    }

    @Test
    void shouldNotAllowVoidOutput()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> compile( FunctionWithVoidOutput.class ) );
        assertThat( exception.getMessage(), startsWith( "Don't know how to map `void` to the Neo4j Type System." ) );
    }

    @Test
    void shouldGiveHelpfulErrorOnFunctionReturningInvalidType()
    {

        // When
        ProcedureException exception = assertThrows( ProcedureException.class, () -> compile( FunctionWithInvalidOutput.class ) );
        assertThat( exception.getMessage(), equalTo( String.format( "Don't know how to map `char[]` to the Neo4j Type System.%n" +
                "Please refer to to the documentation for full details.%n" +
                "For your reference, known types are: [boolean, byte[], double, java.lang.Boolean, " +
                "java.lang.Double, java.lang.Long, java.lang.Number, java.lang.Object, " +
                "java.lang.String, java.time.LocalDate, java.time.LocalDateTime, " +
                "java.time.LocalTime, java.time.OffsetTime, java.time.ZonedDateTime, " +
                "java.time.temporal.TemporalAmount, java.util.List, java.util.Map, long]" ) ) );
    }

    @Test
    void shouldGiveHelpfulErrorOnContextAnnotatedStaticField()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> compile( FunctionWithStaticContextAnnotatedField.class ) );
        assertThat( exception.getMessage(), equalTo( String.format( "The field `gdb` in the class named `FunctionWithStaticContextAnnotatedField` is " +
                                                    "annotated as a @Context field,%n" +
                                                    "but it is static. @Context fields must be public, non-final and non-static,%n" +
                                                    "because they are reset each time a procedure is invoked." ) ) );
    }

    @Test
    void shouldAllowOverridingProcedureName() throws Throwable
    {
        // When
        CallableUserFunction proc = compile( FunctionWithOverriddenName.class ).get( 0 );

        // Then
        assertEquals("org.mystuff.thisisActuallyTheName", proc.signature().name().toString() );
    }

    @Test
    void shouldNotAllowOverridingFunctionNameWithoutNamespace()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> compile( FunctionWithSingleName.class ) );
        assertThat( exception.getMessage(), equalTo( "It is not allowed to define functions in the root namespace please use a " +
                "namespace, e.g. `@UserFunction(\"org.example.com.singleName\")" ) );
    }

    @Test
    void shouldGiveHelpfulErrorOnNullMessageException() throws Throwable
    {
        // Given
        CallableUserFunction proc = compile( FunctionThatThrowsNullMsgExceptionAtInvocation.class ).get( 0 );

        // When
        ProcedureException exception = assertThrows( ProcedureException.class, () -> proc.apply( prepareContext(), new AnyValue[0] ) );
        assertThat( exception.getMessage(),
                equalTo( "Failed to invoke function `org.neo4j.procedure.impl.throwsAtInvocation`: Caused by: java.lang.IndexOutOfBoundsException" ) );
    }

    @Test
    void shouldLoadWhiteListedFunction() throws Throwable
    {
        // Given
        procedureCompiler = new ProcedureCompiler( new TypeCheckers(), components, new ComponentRegistry(),
                NullLog.getInstance(), new ProcedureConfig( Config.defaults( GraphDatabaseSettings.procedure_whitelist, List.of(
                "org.neo4j.procedure.impl.listCoolPeople" ) ) ) );

        CallableUserFunction method = compile( SingleReadOnlyFunction.class ).get( 0 );

        // Expect
        Object out = method.apply( prepareContext(), new AnyValue[0] );
        assertThat(out, equalTo( ValueUtils.of( Arrays.asList("Bonnie", "Clyde") ) ) );
    }

    @Test
    void shouldNotLoadNoneWhiteListedFunction() throws Throwable
    {
        // Given
        Log log = spy(Log.class);
        procedureCompiler = new ProcedureCompiler( new TypeCheckers(), components, new ComponentRegistry(),
                log, new ProcedureConfig( Config.defaults( GraphDatabaseSettings.procedure_whitelist, List.of( "WrongName" ) ) ) );

        List<CallableUserFunction> method = compile( SingleReadOnlyFunction.class );
        verify( log ).warn( "The function 'org.neo4j.procedure.impl.listCoolPeople' is not on the whitelist and won't be loaded." );
        assertThat( method.size(), equalTo( 0 ) );
    }

    @Test
    void shouldNotLoadAnyFunctionIfConfigIsEmpty() throws Throwable
    {
        // Given
        Log log = spy(Log.class);
        procedureCompiler = new ProcedureCompiler( new TypeCheckers(), components, new ComponentRegistry(),
                log, new ProcedureConfig( Config.defaults( GraphDatabaseSettings.procedure_whitelist, List.of( "" ) ) ) );

        List<CallableUserFunction> method = compile( SingleReadOnlyFunction.class );
        verify( log ).warn( "The function 'org.neo4j.procedure.impl.listCoolPeople' is not on the whitelist and won't be loaded." );
        assertThat( method.size(), equalTo( 0 ) );
    }

    @Test
    void shouldSupportFunctionDeprecation() throws Throwable
    {
        // Given
        Log log = mock(Log.class);
        ProcedureCompiler procedureCompiler = new ProcedureCompiler( new TypeCheckers(), components,
                new ComponentRegistry(), log, ProcedureConfig.DEFAULT );

        // When
        List<CallableUserFunction> funcs = procedureCompiler.compileFunction( FunctionWithDeprecation.class, false );

        // Then
        verify( log ).warn( "Use of @UserFunction(deprecatedBy) without @Deprecated in org.neo4j.procedure.impl.badFunc" );
        verifyNoMoreInteractions( log );
        for ( CallableUserFunction func : funcs )
        {
            String name = func.signature().name().name();
            func.apply( prepareContext(), new AnyValue[0] );
            switch ( name )
            {
            case "newFunc":
                assertFalse( func.signature().deprecated().isPresent(), "Should not be deprecated" );
                break;
            case "oldFunc":
            case "badFunc":
                assertTrue( func.signature().deprecated().isPresent(), "Should be deprecated" );
                assertThat( func.signature().deprecated().get(), equalTo( "newFunc" ) );
                break;
            default:
                fail( "Unexpected function: " + name );
            }
        }
    }

    @Test
    void shouldSupportInternalTypes() throws Throwable
    {
        // Given
        CallableUserFunction func = compile( FunctionsWithInternalTypes.class ).get( 0 );

        // When
        Object out = func.apply( prepareContext(), new AnyValue[]{stringValue("hello")} );

        // Then
        assertThat(out, equalTo( longValue( 5 ) ) );
    }

    @Test
    void shouldSupportInternalTypesWithNull() throws Throwable
    {
        // Given
        CallableUserFunction func = compile( FunctionsWithInternalTypes.class ).get( 1 );

        // When
        Object out = func.apply( prepareContext(), new AnyValue[]{stringValue("hello")} );

        // Then
        assertThat(out, equalTo( NO_VALUE ) );
    }

    private org.neo4j.kernel.api.procedure.Context prepareContext()
    {
        return buildContext( dependencyResolver, valueMapper ).context();
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

    public static class FunctionsWithInternalTypes
    {

        @UserFunction
        public LongValue countLetters( @Name( value = "text" ) StringValue text )
        {
            return longValue( text.length() );
        }

        @UserFunction
        public LongValue nullMethod( @Name( value = "text" ) StringValue text )
        {
            return null;
        }
    }

    private List<CallableUserFunction> compile( Class<?> clazz ) throws KernelException
    {
        return procedureCompiler.compileFunction( clazz, false );
    }
}
