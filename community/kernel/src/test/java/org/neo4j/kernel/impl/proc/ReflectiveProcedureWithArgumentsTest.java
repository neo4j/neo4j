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

import junit.framework.TestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.Procedure;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.IteratorUtil.asList;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class ReflectiveProcedureWithArgumentsTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldCompileSimpleProcedure() throws Throwable
    {
        // When
        List<Procedure> procedures = compile( ClassWithProcedureWithSimpleArgs.class );

        // Then
        TestCase.assertEquals( 1, procedures.size() );
        assertThat( procedures.get( 0 ).signature(), equalTo(
                procedureSignature( "org", "neo4j", "kernel", "impl", "proc", "listCoolPeople" )
                        .in( "name", Neo4jTypes.NTString )
                        .in( "age", Neo4jTypes.NTInteger )
                        .out( "name", Neo4jTypes.NTString )
                        .build() ) );
    }

    @Test
    public void shouldRunSimpleProcedure() throws Throwable
    {
        // Given
        Procedure procedure = compile( ClassWithProcedureWithSimpleArgs.class ).get( 0 );

        // When
        RawIterator<Object[],ProcedureException> out = procedure.apply( new Procedure.BasicContext(), new Object[]{"Pontus", 35L} );

        // Then
        List<Object[]> collect = asList( out );
        assertThat( collect.get( 0 )[0], equalTo( "Pontus is 35 years old." ) );
    }

    @Test
    public void shouldRunGenericProcedure() throws Throwable
    {
        // Given
        Procedure procedure = compile( ClassWithProcedureWithGenericArgs.class ).get( 0 );

        // When
        RawIterator<Object[],ProcedureException> out = procedure.apply( new Procedure.BasicContext(), new Object[]{
                Arrays.asList( "Roland", "Eddie", "Susan", "Jake" ),
                Arrays.asList( 1000L, 23L, 29L, 12L )} );

        // Then
        List<Object[]> collect = asList( out );
        assertThat( collect.get( 0 )[0], equalTo( "Roland is 1000 years old." ) );
        assertThat( collect.get( 1 )[0], equalTo( "Eddie is 23 years old." ) );
        assertThat( collect.get( 2 )[0], equalTo( "Susan is 29 years old." ) );
        assertThat( collect.get( 3 )[0], equalTo( "Jake is 12 years old." ) );
    }

    @Test
    public void shouldFailIfMissingAnnotations() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Argument at position 0 in method `listCoolPeople` is missing an " +
                                 "`@Name` annotation. Please add the annotation, recompile the class " +
                                 "and try again." );

        // When
        compile( ClassWithProcedureWithoutAnnotatedArgs.class );
    }


    public static class MyOutputRecord
    {
        public String name;

        public MyOutputRecord( String name )
        {
            this.name = name;
        }
    }

    public static class ClassWithProcedureWithSimpleArgs
    {
        @ReadOnlyProcedure
        public Stream<MyOutputRecord> listCoolPeople( @Name( "name" ) String name, @Name( "age" ) long age )
        {
            return Stream.of( new MyOutputRecord( name + " is " + age + " years old." ) );
        }
    }

    public static class ClassWithProcedureWithGenericArgs
    {
        @ReadOnlyProcedure
        public Stream<MyOutputRecord> listCoolPeople( @Name( "names" ) List<String> names,
                                                      @Name( "age" ) List<Long> ages )
        {
            Iterator<String> nameIterator = names.iterator();
            Iterator<Long> ageIterator = ages.iterator();
            List<MyOutputRecord> result = new ArrayList<>( names.size() );
            while ( nameIterator.hasNext() )
            {
                long age = ageIterator.hasNext() ? ageIterator.next() : -1;
                result.add( new MyOutputRecord( nameIterator.next() + " is " + age + " years old." ) );
            }
            return result.stream();
        }
    }

    public static class ClassWithProcedureWithoutAnnotatedArgs
    {
        @ReadOnlyProcedure
        public Stream<MyOutputRecord> listCoolPeople( String name, int age )
        {
            return Stream.of( new MyOutputRecord( name + " is " + age + " years old." ) );
        }
    }

    private List<Procedure> compile( Class<?> clazz ) throws KernelException
    {
        return new ReflectiveProcedureCompiler( new TypeMappers(), new ComponentRegistry() ).compile( clazz );
    }
}
