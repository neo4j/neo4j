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

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.procedure_whitelist;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

@SuppressWarnings( "WeakerAccess" )
public class ReflectiveProcedureTest
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
    public void shouldInjectLogging() throws KernelException
    {
        // Given
        Log log = spy( Log.class );
        components.register( Log.class, ctx -> log );
        CallableProcedure procedure =
                procedureCompiler.compileProcedure( LoggingProcedure.class, Optional.empty(), true ).get( 0 );

        // When
        procedure.apply( new BasicContext(), new Object[0] );

        // Then
        verify( log ).debug( "1" );
        verify( log ).info( "2" );
        verify( log ).warn( "3" );
        verify( log ).error( "4" );
    }

    @Test
    public void shouldCompileProcedure() throws Throwable
    {
        // When
        List<CallableProcedure> procedures = compile( SingleReadOnlyProcedure.class );

        // Then
        assertEquals( 1, procedures.size() );
        assertThat( procedures.get( 0 ).signature(), Matchers.equalTo(
                procedureSignature( "org", "neo4j", "kernel", "impl", "proc", "listCoolPeople" )
                        .out( "name", Neo4jTypes.NTString )
                        .build() ) );
    }

    @Test
    public void shouldRunSimpleReadOnlyProcedure() throws Throwable
    {
        // Given
        CallableProcedure proc = compile( SingleReadOnlyProcedure.class ).get( 0 );

        // When
        RawIterator<Object[],ProcedureException> out = proc.apply( new BasicContext(), new Object[0] );

        // Then
        assertThat( asList( out ), contains(
                new Object[]{"Bonnie"},
                new Object[]{"Clyde"}
        ) );
    }

    @Test
    public void shouldIgnoreClassesWithNoProcedures() throws Throwable
    {
        // When
        List<CallableProcedure> procedures = compile( PrivateConstructorButNoProcedures.class );

        // Then
        assertEquals( 0, procedures.size() );
    }

    @Test
    public void shouldRunClassWithMultipleProceduresDeclared() throws Throwable
    {
        // Given
        List<CallableProcedure> compiled = compile( MultiProcedureProcedure.class );
        CallableProcedure bananaPeople = compiled.get( 0 );
        CallableProcedure coolPeople = compiled.get( 1 );

        // When
        RawIterator<Object[],ProcedureException> coolOut = coolPeople.apply( new BasicContext(), new Object[0] );
        RawIterator<Object[],ProcedureException> bananaOut = bananaPeople.apply( new BasicContext(), new Object[0] );

        // Then
        assertThat( asList( coolOut ), contains(
                new Object[]{"Bonnie"},
                new Object[]{"Clyde"}
        ) );

        assertThat( asList( bananaOut ), contains(
                new Object[]{"Jake", 18L},
                new Object[]{"Pontus", 2L}
        ) );
    }

    @Test
    public void shouldGiveHelpfulErrorOnConstructorThatRequiresArgument() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Unable to find a usable public no-argument constructor " +
                                 "in the class `WierdConstructorProcedure`. Please add a " +
                                 "valid, public constructor, recompile the class and try again." );

        // When
        compile( WierdConstructorProcedure.class );
    }

    @Test
    public void shouldGiveHelpfulErrorOnNoPublicConstructor() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Unable to find a usable public no-argument constructor " +
                                 "in the class `PrivateConstructorProcedure`. Please add " +
                                 "a valid, public constructor, recompile the class and try again." );

        // When
        compile( PrivateConstructorProcedure.class );
    }

    @Test
    public void shouldAllowVoidOutput() throws Throwable
    {
        // When
        CallableProcedure proc = compile( ProcedureWithVoidOutput.class ).get( 0 );

        // Then
        assertEquals( 0, proc.signature().outputSignature().size() );
        assertFalse( proc.apply( null, new Object[0] ).hasNext() );
    }

    @Test
    public void shouldGiveHelpfulErrorOnProcedureReturningInvalidRecordType() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( String.format("Procedures must return a Stream of records, where a record is a concrete class%n" +
                                 "that you define, with public non-final fields defining the fields in the record.%n" +
                                 "If you''d like your procedure to return `String`, you could define a record class " +
                                 "like:%n" +
                                 "public class Output '{'%n" +
                                 "    public String out;%n" +
                                 "'}'%n" +
                                 "%n" +
                                 "And then define your procedure as returning `Stream<Output>`." ));

        // When
        compile( ProcedureWithInvalidRecordOutput.class ).get( 0 );
    }

    @Test
    public void shouldGiveHelpfulErrorOnContextAnnotatedStaticField() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( String.format("The field `gdb` in the class named `ProcedureWithStaticContextAnnotatedField` is " +
                                 "annotated as a @Context field,%n" +
                                 "but it is static. @Context fields must be public, non-final and non-static,%n" +
                                 "because they are reset each time a procedure is invoked." ));

        // When
        compile( ProcedureWithStaticContextAnnotatedField.class ).get( 0 );
    }

    @Test
    public void shouldAllowNonStaticOutput() throws Throwable
    {
        // When
        CallableProcedure proc = compile( ProcedureWithNonStaticOutputRecord.class ).get( 0 );

        // Then
        assertEquals( 1, proc.signature().outputSignature().size() );
    }

    @Test
    public void shouldAllowOverridingProcedureName() throws Throwable
    {
        // When
        CallableProcedure proc = compile( ProcedureWithOverriddenName.class ).get( 0 );

        // Then
        assertEquals("org.mystuff.thisisActuallyTheName", proc.signature().name().toString() );
    }

    @Test
    public void shouldAllowOverridingProcedureNameWithoutNamespace() throws Throwable
    {
        // When
        CallableProcedure proc = compile( ProcedureWithSingleName.class ).get( 0 );

        // Then
        assertEquals("singleName", proc.signature().name().toString() );
    }

    @Test
    public void shouldGiveHelpfulErrorOnNullMessageException() throws Throwable
    {
        // Given
        CallableProcedure proc = compile( ProcedureThatThrowsNullMsgExceptionAtInvocation.class ).get( 0 );

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Failed to invoke procedure `org.neo4j.kernel.impl.proc.throwsAtInvocation`: " +
                                 "Caused by: java.lang.IndexOutOfBoundsException" );

        // When
        proc.apply( new BasicContext(), new Object[0] );
    }

    @Test
    public void shouldSupportProcedureDeprecation() throws Throwable
    {
        // Given
        Log log = mock(Log.class);
        ReflectiveProcedureCompiler procedureCompiler = new ReflectiveProcedureCompiler( new TypeMappers(), components,
                components, log, ProcedureConfig.DEFAULT );

        // When
        List<CallableProcedure> procs =
                procedureCompiler.compileProcedure( ProcedureWithDeprecation.class, Optional.empty(), true );

        // Then
        verify( log ).warn( "Use of @Procedure(deprecatedBy) without @Deprecated in badProc" );
        verifyNoMoreInteractions( log );
        for ( CallableProcedure proc : procs )
        {
            String name = proc.signature().name().name();
            proc.apply( new BasicContext(), new Object[0] );
            switch ( name )
            {
            case "newProc":
                assertFalse( "Should not be deprecated", proc.signature().deprecated().isPresent() );
                break;
            case "oldProc":
            case "badProc":
                assertTrue( "Should be deprecated", proc.signature().deprecated().isPresent() );
                assertThat( proc.signature().deprecated().get(), equalTo( "newProc" ) );
                break;
            default:
                fail( "Unexpected procedure: " + name );
            }
        }
    }

    @Test
    public void shouldLoadWhiteListedProcedure() throws Throwable
    {
        // Given
        ProcedureConfig config = new ProcedureConfig(
                Config.defaults( procedure_whitelist, "org.neo4j.kernel.impl.proc.listCoolPeople" ) );

        Log log = mock(Log.class);
        ReflectiveProcedureCompiler procedureCompiler = new ReflectiveProcedureCompiler( new TypeMappers(), components,
                components, log, config );

        // When
        CallableProcedure proc =
                procedureCompiler.compileProcedure( SingleReadOnlyProcedure.class, Optional.empty(), false ).get( 0 );
        // When
        RawIterator<Object[],ProcedureException> result = proc.apply( new BasicContext(), new Object[0] );

        // Then
        assertEquals( result.next()[0], "Bonnie" );
    }

    @Test
    public void shouldNotLoadNoneWhiteListedProcedure() throws Throwable
    {
        // Given
        ProcedureConfig config = new ProcedureConfig(
                Config.defaults( procedure_whitelist, "org.neo4j.kernel.impl.proc.NOTlistCoolPeople" ) );

        Log log = mock(Log.class);
        ReflectiveProcedureCompiler procedureCompiler = new ReflectiveProcedureCompiler( new TypeMappers(), components,
                components, log, config );

        // When
        List<CallableProcedure> proc =
                procedureCompiler.compileProcedure( SingleReadOnlyProcedure.class, Optional.empty(), false );
        // Then
        verify( log )
                .warn( "The procedure 'org.neo4j.kernel.impl.proc.listCoolPeople' is not on the whitelist and won't be loaded." );
        assertThat( proc.isEmpty(), is(true) );
    }

    @Test
    public void shouldIgnoreWhiteListingIfFullAccess() throws Throwable
    {
        // Given
        ProcedureConfig config = new ProcedureConfig( Config.defaults( procedure_whitelist, "empty" ) );
        Log log = mock(Log.class);
        ReflectiveProcedureCompiler procedureCompiler = new ReflectiveProcedureCompiler( new TypeMappers(), components,
                components, log, config );

        // When
        CallableProcedure proc =
                procedureCompiler.compileProcedure( SingleReadOnlyProcedure.class, Optional.empty(), true ).get( 0 );
        // Then
        RawIterator<Object[],ProcedureException> result = proc.apply( new BasicContext(), new Object[0] );
        assertEquals( result.next()[0], "Bonnie" );
    }

    @Test
    public void shouldNotLoadAnyProcedureIfConfigIsEmpty() throws Throwable
    {
        // Given
        ProcedureConfig config = new ProcedureConfig( Config.defaults( procedure_whitelist, "" ) );
        Log log = mock(Log.class);
        ReflectiveProcedureCompiler procedureCompiler = new ReflectiveProcedureCompiler( new TypeMappers(), components,
                components, log, config );

        // When
        List<CallableProcedure> proc =
                procedureCompiler.compileProcedure( SingleReadOnlyProcedure.class, Optional.empty(), false );
        // Then
        verify( log )
                .warn( "The procedure 'org.neo4j.kernel.impl.proc.listCoolPeople' is not on the whitelist and won't be loaded." );
        assertThat( proc.isEmpty(), is(true) );
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
            return Stream.generate( () ->
            {
                throw new IndexOutOfBoundsException();
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

    private List<CallableProcedure> compile( Class<?> clazz ) throws KernelException
    {
        return procedureCompiler.compileProcedure( clazz, Optional.empty(), true );
    }
}
