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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import org.neo4j.collection.Dependencies;
import org.neo4j.collection.RawIterator;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.kernel.api.exceptions.ResourceCloseFailureException;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_whitelist;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.api.ResourceManager.EMPTY_RESOURCE_MANAGER;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

@SuppressWarnings( {"WeakerAccess", "unused"} )
public class ProcedureTest
{
    private ProcedureCompiler procedureCompiler;
    private ComponentRegistry components;
    private final DependencyResolver dependencyResolver = new Dependencies();
    private final ValueMapper<Object> valueMapper = new DefaultValueMapper( mock( EmbeddedProxySPI.class ) );

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
        CallableProcedure procedure =
                procedureCompiler.compileProcedure( LoggingProcedure.class, null, true ).get( 0 );

        // When
        procedure.apply( prepareContext(), new AnyValue[0], EMPTY_RESOURCE_MANAGER );

        // Then
        verify( log ).debug( "1" );
        verify( log ).info( "2" );
        verify( log ).warn( "3" );
        verify( log ).error( "4" );
    }

    @Test
    void shouldCompileProcedure() throws Throwable
    {
        // When
        List<CallableProcedure> procedures = compile( SingleReadOnlyProcedure.class );

        // Then
        assertEquals( 1, procedures.size() );
        assertThat( procedures.get( 0 ).signature(), Matchers.equalTo(
                procedureSignature( "org", "neo4j", "procedure", "impl", "listCoolPeople" )
                        .out( "name", Neo4jTypes.NTString )
                        .build() ) );
    }

    @Test
    void shouldRunSimpleReadOnlyProcedure() throws Throwable
    {
        // Given
        CallableProcedure proc = compile( SingleReadOnlyProcedure.class ).get( 0 );

        // When
        RawIterator<AnyValue[],ProcedureException> out = proc.apply( prepareContext(), new AnyValue[0], EMPTY_RESOURCE_MANAGER );

        // Then
        assertThat( asList( out ), Matchers.contains(
                new AnyValue[]{stringValue( "Bonnie" )},
                new AnyValue[]{stringValue( "Clyde" )}
        ) );
    }

    @Test
    void shouldIgnoreClassesWithNoProcedures() throws Throwable
    {
        // When
        List<CallableProcedure> procedures = compile( PrivateConstructorButNoProcedures.class );

        // Then
        assertEquals( 0, procedures.size() );
    }

    @Test
    void shouldRunClassWithMultipleProceduresDeclared() throws Throwable
    {
        // Given
        List<CallableProcedure> compiled = compile( MultiProcedureProcedure.class );
        CallableProcedure bananaPeople = compiled.get( 0 );
        CallableProcedure coolPeople = compiled.get( 1 );

        // When
        RawIterator<AnyValue[],ProcedureException> coolOut = coolPeople.apply( prepareContext(), new AnyValue[0], EMPTY_RESOURCE_MANAGER );
        RawIterator<AnyValue[],ProcedureException> bananaOut = bananaPeople.apply( prepareContext(), new AnyValue[0], EMPTY_RESOURCE_MANAGER );

        // Then
        assertThat( asList( coolOut ), Matchers.contains(
                new AnyValue[]{stringValue( "Bonnie" )},
                new AnyValue[]{stringValue( "Clyde" )}
        ) );

        assertThat( asList( bananaOut ), Matchers.contains(
                new AnyValue[]{stringValue( "Jake" ), longValue( 18L )},
                new AnyValue[]{stringValue( "Pontus" ), longValue( 2L )}
        ) );
    }

    @Test
    void shouldGiveHelpfulErrorOnConstructorThatRequiresArgument()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> compile( WierdConstructorProcedure.class ) );
        assertThat( exception.getMessage(), equalTo( "Unable to find a usable public no-argument constructor " +
                                                    "in the class `WierdConstructorProcedure`. Please add a " +
                                                    "valid, public constructor, recompile the class and try again." ) );
    }

    @Test
    void shouldGiveHelpfulErrorOnNoPublicConstructor()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> compile( PrivateConstructorProcedure.class ) );
        assertThat( exception.getMessage(), equalTo( "Unable to find a usable public no-argument constructor " +
                                                    "in the class `PrivateConstructorProcedure`. Please add " +
                                                    "a valid, public constructor, recompile the class and try again." ) );
    }

    @Test
    void shouldAllowVoidOutput() throws Throwable
    {
        // When
        CallableProcedure proc = compile( ProcedureWithVoidOutput.class ).get( 0 );

        // Then
        assertEquals( 0, proc.signature().outputSignature().size() );
        assertFalse( proc.apply( prepareContext(), new AnyValue[0], EMPTY_RESOURCE_MANAGER ).hasNext() );
    }

    @Test
    void shouldGiveHelpfulErrorOnProcedureReturningInvalidRecordType()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> compile( ProcedureWithInvalidRecordOutput.class ) );
        assertThat( exception.getMessage(), equalTo( String.format( "Procedures must return a Stream of records, where a record is a concrete class%n" +
                                                "that you define, with public non-final fields defining the fields in the record.%n" +
                                                "If you''d like your procedure to return `String`, you could define a record class " +
                                                "like:%n" +
                                                "public class Output '{'%n" +
                                                "    public String out;%n" +
                                                "'}'%n" +
                                                "%n" +
                                                "And then define your procedure as returning `Stream<Output>`." ) ) );
    }

    @Test
    void shouldGiveHelpfulErrorOnContextAnnotatedStaticField()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> compile( ProcedureWithStaticContextAnnotatedField.class ) );
        assertThat( exception.getMessage(), equalTo( String.format("The field `gdb` in the class named `ProcedureWithStaticContextAnnotatedField` is " +
                                                    "annotated as a @Context field,%n" +
                                                    "but it is static. @Context fields must be public, non-final and non-static,%n" +
                                                    "because they are reset each time a procedure is invoked." ) ) );

    }

    @Test
    void shouldAllowNonStaticOutput() throws Throwable
    {
        // When
        CallableProcedure proc = compile( ProcedureWithNonStaticOutputRecord.class ).get( 0 );

        // Then
        assertEquals( 1, proc.signature().outputSignature().size() );
    }

    @Test
    void shouldAllowOverridingProcedureName() throws Throwable
    {
        // When
        CallableProcedure proc = compile( ProcedureWithOverriddenName.class ).get( 0 );

        // Then
        assertEquals("org.mystuff.thisisActuallyTheName", proc.signature().name().toString() );
    }

    @Test
    void shouldAllowOverridingProcedureNameWithoutNamespace() throws Throwable
    {
        // When
        CallableProcedure proc = compile( ProcedureWithSingleName.class ).get( 0 );

        // Then
        assertEquals("singleName", proc.signature().name().toString() );
    }

    @Test
    void shouldGiveHelpfulErrorOnNullMessageException() throws Throwable
    {
        // Given
        CallableProcedure proc = compile( ProcedureThatThrowsNullMsgExceptionAtInvocation.class ).get( 0 );

        ProcedureException exception = assertThrows( ProcedureException.class, () -> proc.apply( prepareContext(), new AnyValue[0], EMPTY_RESOURCE_MANAGER ) );
        assertThat( exception.getMessage(), equalTo( "Failed to invoke procedure `org.neo4j.procedure.impl.throwsAtInvocation`: " +
                                                    "Caused by: java.lang.IndexOutOfBoundsException" ) );
    }

    @Test
    void shouldCloseResourcesAndGiveHelpfulErrorOnMidStreamException() throws Throwable
    {
        // Given
        CallableProcedure proc = compile( ProcedureThatThrowsNullMsgExceptionMidStream.class ).get( 0 );

        ProcedureException exception = assertThrows( ProcedureException.class, () ->
        {
            RawIterator<AnyValue[],ProcedureException> stream = proc.apply( prepareContext(), new AnyValue[0], EMPTY_RESOURCE_MANAGER );
            if ( stream.hasNext() )
            {
                stream.next();
            }
        } );
        assertThat( exception.getMessage(), equalTo( "Failed to invoke procedure `org.neo4j.procedure.impl.throwsInStream`: " +
                                            "Caused by: java.lang.IndexOutOfBoundsException" ) );
        // Expect that we get a suppressed exception from Stream.onClose (which also verifies that we actually call
        // onClose on the first exception)
        assertThat( exception, new BaseMatcher<Exception>()
        {
            @Override
            public void describeTo( Description description )
            {
                description.appendText( "a suppressed exception with cause ExceptionDuringClose" );
            }

            @Override
            public boolean matches( Object item )
            {
                Exception e = (Exception) item;
                for ( Throwable suppressed : e.getSuppressed() )
                {
                    if ( suppressed instanceof ResourceCloseFailureException )
                    {
                        if ( suppressed.getCause() instanceof ExceptionDuringClose )
                        {
                            return true;
                        }
                    }
                }
                return false;
            }
        } );
    }

    @Test
    void shouldSupportProcedureDeprecation() throws Throwable
    {
        // Given
        Log log = mock(Log.class);
        ProcedureCompiler procedureCompiler = new ProcedureCompiler( new TypeCheckers(), components,
                components, log, ProcedureConfig.DEFAULT );

        // When
        List<CallableProcedure> procs =
                procedureCompiler.compileProcedure( ProcedureWithDeprecation.class, null, true );

        // Then
        verify( log ).warn( "Use of @Procedure(deprecatedBy) without @Deprecated in badProc" );
        verifyNoMoreInteractions( log );
        for ( CallableProcedure proc : procs )
        {
            String name = proc.signature().name().name();
            proc.apply( prepareContext(), new AnyValue[0], EMPTY_RESOURCE_MANAGER );
            switch ( name )
            {
            case "newProc":
                assertFalse( proc.signature().deprecated().isPresent(), "Should not be deprecated" );
                break;
            case "oldProc":
            case "badProc":
                assertTrue( proc.signature().deprecated().isPresent(), "Should be deprecated" );
                assertThat( proc.signature().deprecated().get(), equalTo( "newProc" ) );
                break;
            default:
                fail( "Unexpected procedure: " + name );
            }
        }
    }

    @Test
    void shouldLoadWhiteListedProcedure() throws Throwable
    {
        // Given
        ProcedureConfig config = new ProcedureConfig(
                Config.defaults( procedure_whitelist, List.of( "org.neo4j.procedure.impl.listCoolPeople" ) ) );

        Log log = mock(Log.class);
        ProcedureCompiler procedureCompiler = new ProcedureCompiler( new TypeCheckers(), components,
                components, log, config );

        // When
        CallableProcedure proc =
                procedureCompiler.compileProcedure( SingleReadOnlyProcedure.class, null, false ).get( 0 );
        // When
        RawIterator<AnyValue[],ProcedureException> result = proc.apply( prepareContext(), new AnyValue[0], EMPTY_RESOURCE_MANAGER );

        // Then
        assertEquals( result.next()[0], stringValue( "Bonnie" ) );
    }

    @Test
    void shouldNotLoadNoneWhiteListedProcedure() throws Throwable
    {
        // Given
        ProcedureConfig config = new ProcedureConfig(
                Config.defaults( procedure_whitelist, List.of( "org.neo4j.procedure.impl.NOTlistCoolPeople" ) ) );

        Log log = mock(Log.class);
        ProcedureCompiler procedureCompiler = new ProcedureCompiler( new TypeCheckers(), components,
                components, log, config );

        // When
        List<CallableProcedure> proc =
                procedureCompiler.compileProcedure( SingleReadOnlyProcedure.class, null, false );
        // Then
        verify( log )
                .warn( "The procedure 'org.neo4j.procedure.impl.listCoolPeople' is not on the whitelist and won't be loaded." );
        assertThat( proc.isEmpty(), is(true) );
    }

    @Test
    void shouldIgnoreWhiteListingIfFullAccess() throws Throwable
    {
        // Given
        ProcedureConfig config = new ProcedureConfig( Config.defaults( procedure_whitelist, List.of( "empty" ) ) );
        Log log = mock(Log.class);
        ProcedureCompiler procedureCompiler = new ProcedureCompiler( new TypeCheckers(), components,
                components, log, config );

        // When
        CallableProcedure proc =
                procedureCompiler.compileProcedure( SingleReadOnlyProcedure.class, null, true ).get( 0 );
        // Then
        RawIterator<AnyValue[],ProcedureException> result = proc.apply( prepareContext(), new AnyValue[0], EMPTY_RESOURCE_MANAGER );
        assertEquals( result.next()[0], stringValue( "Bonnie" ) );
    }

    @Test
    void shouldNotLoadAnyProcedureIfConfigIsEmpty() throws Throwable
    {
        // Given
        ProcedureConfig config = new ProcedureConfig( Config.defaults( procedure_whitelist, List.of( "" ) ) );
        Log log = mock(Log.class);
        ProcedureCompiler procedureCompiler = new ProcedureCompiler( new TypeCheckers(), components,
                components, log, config );

        // When
        List<CallableProcedure> proc =
                procedureCompiler.compileProcedure( SingleReadOnlyProcedure.class, null, false );
        // Then
        verify( log )
                .warn( "The procedure 'org.neo4j.procedure.impl.listCoolPeople' is not on the whitelist and won't be loaded." );
        assertThat( proc.isEmpty(), is(true) );
    }

    @Test
    void shouldRunProcedureWithInternalTypes() throws Throwable
    {
        // Given
        CallableProcedure proc = compile( InternalTypes.class ).get( 0 );

        // When
        RawIterator<AnyValue[],ProcedureException> out =
                proc.apply( prepareContext(), new AnyValue[]{longValue( 42 ), stringValue( "hello" ),
                        Values.TRUE}, EMPTY_RESOURCE_MANAGER );

        // Then
        assertThat( out.next(), equalTo(
                new AnyValue[]{longValue( 42 ), stringValue( "hello" ), Values.TRUE}
        ) );
        assertFalse( out.hasNext() );
    }

    private org.neo4j.kernel.api.procedure.Context prepareContext()
    {
        return buildContext( dependencyResolver, valueMapper ).context();
    }

    public static class MyOutputRecord
    {
        public String name;

        public MyOutputRecord( String name )
        {
            this.name = name;
        }
    }

    public static class SomeOtherOutputRecord
    {
        public String name;
        public long bananas;

        public SomeOtherOutputRecord( String name, long bananas )
        {
            this.name = name;
            this.bananas = bananas;
        }
    }

    public static class LoggingProcedure
    {
        @Context
        public Log log;

        @Procedure
        public Stream<MyOutputRecord> logAround()
        {
            log.debug( "1" );
            log.info( "2" );
            log.warn( "3" );
            log.error( "4" );
            return Stream.empty();
        }
    }

    public static class SingleReadOnlyProcedure
    {
        @Procedure
        public Stream<MyOutputRecord> listCoolPeople()
        {
            return Stream.of(
                    new MyOutputRecord( "Bonnie" ),
                    new MyOutputRecord( "Clyde" ) );
        }
    }

    public static class ProcedureWithVoidOutput
    {
        @Procedure
        public void voidOutput()
        {
        }
    }

    public static class ProcedureWithNonStaticOutputRecord
    {
        @Procedure
        public Stream<NonStatic> voidOutput()
        {
            return Stream.of(new NonStatic());
        }

        public class NonStatic
        {
            public String field = "hello, rodl!";
        }
    }

    public static class MultiProcedureProcedure
    {
        @Procedure
        public Stream<MyOutputRecord> listCoolPeople()
        {
            return Stream.of(
                    new MyOutputRecord( "Bonnie" ),
                    new MyOutputRecord( "Clyde" ) );
        }

        @Procedure
        public Stream<SomeOtherOutputRecord> listBananaOwningPeople()
        {
            return Stream.of(
                    new SomeOtherOutputRecord( "Jake", 18 ),
                    new SomeOtherOutputRecord( "Pontus", 2 ) );
        }
    }

    public static class WierdConstructorProcedure
    {
        public WierdConstructorProcedure( WierdConstructorProcedure wat )
        {

        }

        @Procedure
        public Stream<MyOutputRecord> listCoolPeople()
        {
            return Stream.of( new MyOutputRecord( "Bonnie" ), new MyOutputRecord( "Clyde" ) );
        }
    }

    public static class ProcedureWithInvalidRecordOutput
    {
        @Procedure
        public String test( )
        {
            return "Testing";
        }
    }

    public static class ProcedureWithStaticContextAnnotatedField
    {
        @Context
        public static GraphDatabaseService gdb;

        @Procedure
        public Stream<MyOutputRecord> test( )
        {
            return null;
        }
    }

    public static class ProcedureThatThrowsNullMsgExceptionAtInvocation
    {
        @Procedure
        public Stream<MyOutputRecord> throwsAtInvocation( )
        {
            throw new IndexOutOfBoundsException();
        }
    }

    public static class ProcedureThatThrowsNullMsgExceptionMidStream
    {
        @Procedure
        public Stream<MyOutputRecord> throwsInStream( )
        {
            return Stream.<MyOutputRecord>generate( () ->
            {
                throw new IndexOutOfBoundsException();
            }).onClose( () ->
            {
                throw new ExceptionDuringClose();
            } );
        }
    }

    public static class PrivateConstructorProcedure
    {
        private PrivateConstructorProcedure()
        {

        }

        @Procedure
        public Stream<MyOutputRecord> listCoolPeople()
        {
            return Stream.of( new MyOutputRecord( "Bonnie" ), new MyOutputRecord( "Clyde" ) );
        }
    }

    public static class PrivateConstructorButNoProcedures
    {
        private PrivateConstructorButNoProcedures()
        {

        }

        public Stream<MyOutputRecord> thisIsNotAProcedure()
        {
            return null;
        }
    }

    public static class ProcedureWithOverriddenName
    {
        @Procedure( "org.mystuff.thisisActuallyTheName" )
        public void somethingThatShouldntMatter()
        {

        }

        @Procedure( "singleName" )
        public void blahDoesntMatterEither()
        {

        }
    }

    public static class ProcedureWithSingleName
    {
        @Procedure( "singleName" )
        public void blahDoesntMatterEither()
        {

        }
    }

    public static class ProcedureWithDeprecation
    {
        @Procedure( "newProc" )
        public void newProc()
        {
        }

        @Deprecated
        @Procedure( value = "oldProc", deprecatedBy = "newProc" )
        public void oldProc()
        {
        }

        @Procedure( value = "badProc", deprecatedBy = "newProc" )
        public void badProc()
        {
        }
    }

    public static class InternalTypes
    {
        @Procedure
        public Stream<InternalTypeRecord> internalTypes( @Name( value = "long" ) LongValue longValue,
                @Name( value = "text" ) TextValue textValue, @Name( value = "bool" ) BooleanValue booleanValue )
        {
            return Stream.of( new InternalTypeRecord( longValue, textValue, booleanValue ) );
        }
    }

    public static class InternalTypeRecord
    {
        public final LongValue longValue;
        public final TextValue textValue;
        public final BooleanValue booleanValue;

        public InternalTypeRecord( LongValue longValue, TextValue textValue,
                BooleanValue booleanValue )
        {
            this.longValue = longValue;
            this.textValue = textValue;
            this.booleanValue = booleanValue;
        }
    }

    private List<CallableProcedure> compile( Class<?> clazz ) throws KernelException
    {
        return procedureCompiler.compileProcedure( clazz, null, true );
    }

    private static class ExceptionDuringClose extends RuntimeException
    {
    }
}
