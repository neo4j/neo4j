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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings( "WeakerAccess" )
class MethodSignatureCompilerTest
{
    public static class MyOutputRecord
    {
        public String name;

        MyOutputRecord( String name )
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
        @Procedure
        public Stream<MyOutputRecord> echo( @Name( "name" ) String in )
        {
            return Stream.of( new MyOutputRecord( in ));
        }

        @Procedure
        public Stream<MyOutputRecord> echoWithoutAnnotations( @Name( "name" ) String in1, String in2 )
        {
            return Stream.of( new MyOutputRecord( in1 + in2 ));
        }

        @Procedure
        public Stream<MyOutputRecord> echoWithInvalidType( @Name( "name" ) UnmappableRecord in )
        {
            return Stream.of( new MyOutputRecord( "echo" ));
        }
    }

    @Test
    void shouldMapSimpleRecordWithString() throws Throwable
    {
        // When
        Method echo = ClassWithProcedureWithSimpleArgs.class.getMethod( "echo", String.class );
        List<FieldSignature> signature = new MethodSignatureCompiler( new TypeCheckers() ).signatureFor( echo );

        // THen
        assertThat(signature, contains( FieldSignature.inputField( "name", Neo4jTypes.NTString ) ));
    }

    @Test
    void shouldGiveHelpfulErrorOnUnmappable() throws Throwable
    {
        // Given
        Method echo = ClassWithProcedureWithSimpleArgs.class.getMethod( "echoWithInvalidType", UnmappableRecord.class );

        ProcedureException exception = assertThrows( ProcedureException.class, () -> new MethodSignatureCompiler( new TypeCheckers() ).signatureFor( echo ) );
        assertThat( exception.getMessage(), startsWith( String.format("Argument `name` at position 0 in `echoWithInvalidType` with%n" +
                                                "type `UnmappableRecord` cannot be converted to a Neo4j type: Don't know how to map " +
                                                "`org.neo4j.procedure.impl.MethodSignatureCompilerTest$UnmappableRecord` to " +
                                                "the Neo4j Type System.%n" +
                                                "Please refer to to the documentation for full details.%n" +
                                                "For your reference, known types are:" ) ) );
    }

    @Test
    void shouldGiveHelpfulErrorOnMissingAnnotations() throws Throwable
    {
        // Given
        Method echo = ClassWithProcedureWithSimpleArgs.class.getMethod( "echoWithoutAnnotations", String.class, String.class);

        ProcedureException exception = assertThrows( ProcedureException.class, () -> new MethodSignatureCompiler( new TypeCheckers() ).signatureFor( echo ) );
        assertThat( exception.getMessage(), equalTo( String.format("Argument at position 1 in method `echoWithoutAnnotations` is missing an `@Name` " +
                                                    "annotation.%n" +
                                                    "Please add the annotation, recompile the class and try again." ) ) );
    }
}
