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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature.FieldSignature;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public class MethodSignatureCompilerTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    public static class MyOutputRecord
    {
        public String name;

        public MyOutputRecord( String name )
        {
            this.name = name;
        }
    }

    public static class UnmappableRecord
    {
        public UnmappableRecord wat;
    }

    public static class ClassWithProcedureWithSimpleArgs
    {
        @ReadOnlyProcedure
        public Stream<MyOutputRecord> echo( @Name("name") String in)
        {
            return Stream.of( new MyOutputRecord( in ));
        }

        @ReadOnlyProcedure
        public Stream<MyOutputRecord> echoWithoutAnnotations( @Name("name")String in1, String in2)
        {
            return Stream.of( new MyOutputRecord( in1 + in2 ));
        }

        @ReadOnlyProcedure
        public Stream<MyOutputRecord> echoWithInvalidType( @Name("name") UnmappableRecord in)
        {
            return Stream.of( new MyOutputRecord( "echo" ));
        }
    }

    @Test
    public void shouldMapSimpleRecordWithString() throws Throwable
    {
        // When
        Method echo = ClassWithProcedureWithSimpleArgs.class.getMethod( "echo", String.class );
        List<FieldSignature> signature = new MethodSignatureCompiler( new TypeMappers() ).signatureFor( echo );

        // THen
        assertThat(signature, contains( new FieldSignature("name", Neo4jTypes.NTString)));
    }


    @Test
    public void shouldGiveHelpfulErrorOnUnmappable() throws Throwable
    {
        // Given
        Method echo = ClassWithProcedureWithSimpleArgs.class.getMethod( "echoWithInvalidType", UnmappableRecord.class );

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Argument `name` at position 0 in `echoWithInvalidType` with type `UnmappableRecord` " +
                                 "cannot be converted to a Neo4j type: Don't know how to map " +
                                 "`class org.neo4j.kernel.impl.proc.MethodSignatureCompilerTest$UnmappableRecord` to `ANY?`" );

        // When
        new MethodSignatureCompiler(new TypeMappers()).signatureFor( echo );
    }

    @Test
    public void shouldGiveHelpfulErrorOnMissingAnnotations() throws Throwable
    {
        // Given
        Method echo = ClassWithProcedureWithSimpleArgs.class.getMethod( "echoWithoutAnnotations", String.class, String.class);

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Argument at position 1 in method `echoWithoutAnnotations` is missing an " +
                                 "`@Name` annotation. Please add the annotation, recompile the class and " +
                                 "try again." );

        // When
        new MethodSignatureCompiler(new TypeMappers()).signatureFor( echo );
    }
}
