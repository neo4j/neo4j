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

import org.hamcrest.core.IsEqual;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.logging.NullLog;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTInteger;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class ProcedureJarLoaderTest
{
    @Rule
    public TemporaryFolder tmpdir = new TemporaryFolder();
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final ProcedureJarLoader jarloader =
            new ProcedureJarLoader( new ReflectiveProcedureCompiler( new TypeMappers(), new ComponentRegistry(),
                    NullLog.getInstance(), ProcedureAllowedConfig.DEFAULT ), NullLog.getInstance() );

    @Test
    public void shouldLoadProcedureFromJar() throws Throwable
    {
        // Given
        URL jar = createJarFor( ClassWithOneProcedure.class );

        // When
        List<CallableProcedure> procedures = jarloader.loadProcedures( jar ).procedures();

        // Then
        List<ProcedureSignature> signatures = procedures.stream().map( CallableProcedure::signature ).collect( toList() );
        assertThat( signatures, contains(
                procedureSignature( "org","neo4j", "kernel", "impl", "proc", "myProcedure" ).out( "someNumber", NTInteger ).build() ));

        assertThat( asList( procedures.get( 0 ).apply( new BasicContext(), new Object[0] ) ),
                contains( IsEqual.equalTo( new Object[]{1337L} )) );
    }

    @Test
    public void shouldLoadProcedureWithArgumentFromJar() throws Throwable
    {
        // Given
        URL jar = createJarFor( ClassWithProcedureWithArgument.class );

        // When
        List<CallableProcedure> procedures = jarloader.loadProcedures( jar ).procedures();

        // Then
        List<ProcedureSignature> signatures = procedures.stream().map( CallableProcedure::signature ).collect( toList() );
        assertThat( signatures, contains(
                procedureSignature( "org","neo4j", "kernel", "impl", "proc", "myProcedure" )
                        .in( "value", NTInteger )
                        .out( "someNumber", NTInteger )
                        .build() ));

        assertThat( asList(procedures.get( 0 ).apply( new BasicContext(), new Object[]{42L} ) ),
                contains( IsEqual.equalTo( new Object[]{42L} )) );
    }

    @Test
    public void shouldLoadProcedureFromJarWithMultipleProcedureClasses() throws Throwable
    {
        // Given
        URL jar = createJarFor( ClassWithOneProcedure.class, ClassWithAnotherProcedure.class, ClassWithNoProcedureAtAll.class );

        // When
        List<CallableProcedure> procedures = jarloader.loadProcedures( jar ).procedures();

        // Then
        List<ProcedureSignature> signatures = procedures.stream().map( CallableProcedure::signature ).collect( toList() );
        assertThat( signatures, containsInAnyOrder(
                procedureSignature( "org","neo4j", "kernel", "impl", "proc", "myOtherProcedure" ).out( "someNumber", NTInteger ).build(),
                procedureSignature( "org","neo4j", "kernel", "impl", "proc", "myProcedure" ).out( "someNumber", NTInteger ).build() ));
    }

    @Test
    public void shouldGiveHelpfulErrorOnInvalidProcedure() throws Throwable
    {
        // Given
        URL jar = createJarFor( ClassWithOneProcedure.class, ClassWithInvalidProcedure.class );

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( String.format("Procedures must return a Stream of records, where a record is a concrete class%n" +
                                 "that you define, with public non-final fields defining the fields in the record.%n" +
                                 "If you''d like your procedure to return `boolean`, you could define a record class " +
                                 "like:%n" +
                                 "public class Output '{'%n" +
                                 "    public boolean out;%n" +
                                 "'}'%n" +
                                 "%n" +
                                 "And then define your procedure as returning `Stream<Output>`." ));

        // When
        jarloader.loadProcedures( jar );
    }

    @Test
    public void shouldLoadProceduresFromDirectory() throws Throwable
    {
        // Given
        createJarFor( ClassWithOneProcedure.class );
        createJarFor( ClassWithAnotherProcedure.class );

        // When
        List<CallableProcedure> procedures = jarloader.loadProceduresFromDir( tmpdir.getRoot() ).procedures();

        // Then
        List<ProcedureSignature> signatures = procedures.stream().map( CallableProcedure::signature ).collect( toList() );
        assertThat( signatures, containsInAnyOrder(
                procedureSignature( "org","neo4j", "kernel", "impl", "proc", "myOtherProcedure" ).out( "someNumber", NTInteger ).build(),
                procedureSignature( "org","neo4j", "kernel", "impl", "proc", "myProcedure" ).out( "someNumber", NTInteger ).build() ));
    }

    @Test
    public void shouldGiveHelpfulErrorOnWildCardProcedure() throws Throwable
    {
        // Given
        URL jar = createJarFor( ClassWithWildCardStream.class );

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( String.format("Procedures must return a Stream of records, where a record is a concrete class%n" +
                                 "that you define and not a Stream<?>." ));

        // When
        jarloader.loadProcedures( jar );
    }

    @Test
    public void shouldGiveHelpfulErrorOnRawStreamProcedure() throws Throwable
    {
        // Given
        URL jar = createJarFor( ClassWithRawStream.class );

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( String.format("Procedures must return a Stream of records, where a record is a concrete class%n" +
                                 "that you define and not a raw Stream." ));

        // When
        jarloader.loadProcedures( jar );
    }

    @Test
    public void shouldGiveHelpfulErrorOnGenericStreamProcedure() throws Throwable
    {
        // Given
        URL jar = createJarFor( ClassWithGenericStream.class );

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( String.format("Procedures must return a Stream of records, where a record is a concrete class%n" +
                                 "that you define and not a parameterized type such as java.util.List<org.neo4j" +
                                 ".kernel.impl.proc.ProcedureJarLoaderTest$Output>."));

        // When
        jarloader.loadProcedures( jar );
    }

    public URL createJarFor( Class<?> ... targets ) throws IOException
    {
        return new JarBuilder().createJarFor( tmpdir.newFile( new Random().nextInt() + ".jar" ), targets );
    }

    public static class Output
    {
        public long someNumber = 1337;

        public Output()
        {

        }

        public Output( long anotherNumber )
        {
            this.someNumber = anotherNumber;
        }
    }

    public static class ClassWithInvalidProcedure
    {
        @Procedure
        public boolean booleansAreNotAcceptableReturnTypes()
        {
            return false;
        }
    }

    public static class ClassWithOneProcedure
    {
        @Procedure
        public Stream<Output> myProcedure()
        {
            return Stream.of( new Output() );
        }
    }

    public static class ClassWithNoProcedureAtAll
    {
        void thisMethodIsEntirelyUnrelatedToAllThisExcitement()
        {

        }
    }

    public static class ClassWithAnotherProcedure
    {
        @Procedure
        public Stream<Output> myOtherProcedure()
        {
            return Stream.of( new Output() );
        }
    }

    public static class ClassWithProcedureWithArgument
    {
        @Procedure
        public Stream<Output> myProcedure(@Name( "value" ) long value)
        {
            return Stream.of( new Output(value) );
        }
    }

    public static class ClassWithWildCardStream
    {
        @Procedure
        public Stream<?> wildCardProc()
        {
            return Stream.of( new Output() );
        }
    }

    public static class ClassWithRawStream
    {
        @Procedure
        public Stream rawStreamProc()
        {
            return Stream.of( new Output() );
        }
    }

    public static class ClassWithGenericStream
    {
        @Procedure
        public Stream<List<Output>> genericStream()
        {
            return Stream.of( Collections.singletonList( new Output() ));
        }
    }
}
