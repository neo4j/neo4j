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
package org.neo4j.codegen;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.neo4j.codegen.ByteCodeUtils.desc;
import static org.neo4j.codegen.ByteCodeUtils.exceptions;
import static org.neo4j.codegen.ByteCodeUtils.signature;
import static org.neo4j.codegen.ByteCodeUtils.typeName;
import static org.neo4j.codegen.MethodDeclaration.method;
import static org.neo4j.codegen.Parameter.param;
import static org.neo4j.codegen.TypeReference.extending;
import static org.neo4j.codegen.TypeReference.typeParameter;
import static org.neo4j.codegen.TypeReference.typeReference;

public class ByteCodeUtilsTest
{
    @Test
    public void shouldTranslateTypeNames()
    {
        //primitive types
        assertTypeName( int.class, "I" );
        assertTypeName( byte.class, "B" );
        assertTypeName( short.class, "S" );
        assertTypeName( char.class, "C" );
        assertTypeName( float.class, "F" );
        assertTypeName( double.class, "D" );
        assertTypeName( boolean.class, "Z" );
        assertTypeName( void.class, "V" );

        //primitive array types
        assertTypeName( int[].class, "[I" );
        assertTypeName( byte[].class, "[B" );
        assertTypeName( short[].class, "[S" );
        assertTypeName( char[].class, "[C" );
        assertTypeName( float[].class, "[F" );
        assertTypeName( double[].class, "[D" );
        assertTypeName( boolean[].class, "[Z" );

        //reference type
        assertTypeName( String.class, "Ljava/lang/String;" );

        //reference array type
        assertTypeName( String[].class, "[Ljava/lang/String;" );
    }

    @Test
    public void shouldDescribeMethodWithNoParameters()
    {
        // GIVEN
        TypeReference owner = typeReference( ByteCodeUtilsTest.class );
        MethodDeclaration declaration = method( boolean.class, "foo" ).build( owner );

        // WHEN
        String description = desc( declaration );

        // THEN
        assertThat( description, equalTo( "()Z" ) );
    }

    @Test
    public void shouldDescribeMethodWithParameters()
    {
        // GIVEN
        TypeReference owner = typeReference( ByteCodeUtilsTest.class );
        MethodDeclaration declaration =
                method( List.class, "foo", param( String.class, "string" ), param( char[].class, "chararray" ) )
                        .build( owner );

        // WHEN
        String description = desc( declaration );

        // THEN
        assertThat( description, equalTo( "(Ljava/lang/String;[C)Ljava/util/List;" ) );
    }

    @Test
    public void signatureShouldBeNullWhenNotGeneric()
    {
        // GIVEN
        TypeReference reference = typeReference( String.class );

        // WHEN
        String signature = signature( reference );

        // THEN
        assertNull( signature );
    }

    @Test
    public void signatureShouldBeCorrectWhenGeneric()
    {
        // GIVEN
        TypeReference reference = TypeReference.parameterizedType( List.class, String.class );

        // WHEN
        String signature = signature( reference );

        // THEN
        assertThat( signature, equalTo( "Ljava/util/List<Ljava/lang/String;>;" ) );
    }

    @Test
    public void methodSignatureShouldBeNullWhenNotGeneric()
    {
        // GIVEN
        TypeReference owner = typeReference( ByteCodeUtilsTest.class );
        MethodDeclaration declaration =
                method( String.class, "foo", param( String.class, "string" ), param( char[].class, "chararray" ) )
                        .build( owner );

        // WHEN
        String signature = signature( declaration );

        // THEN
       assertNull( signature );
    }

    @Test
    public void methodSignatureShouldBeCorrectWhenGeneric()
    {
        // GIVEN
        TypeReference owner = typeReference( ByteCodeUtilsTest.class );
        MethodDeclaration declaration =
                method( TypeReference.parameterizedType( List.class, String.class ), "foo", param( String.class, "string" ) )
                        .build( owner );

        // WHEN
        String signature = signature( declaration );

        // THEN
        assertThat( signature, equalTo( "(Ljava/lang/String;)Ljava/util/List<Ljava/lang/String;>;" ) );
    }

    @Test
    public void shouldHandleGenericReturnType()
    {
        // GIVEN
        TypeReference owner = typeReference( ByteCodeUtilsTest.class );
        MethodDeclaration declaration = MethodDeclaration.method( typeParameter( "T" ), "fail")
                .parameterizedWith( "T", extending( Object.class ) )
                .build( owner );

        // WHEN
        String desc = desc( declaration );
        String signature = signature( declaration );

        // THEN
        assertThat(desc, equalTo("()Ljava/lang/Object;"));
        assertThat(signature,
                equalTo( "<T:Ljava/lang/Object;>()TT;" ));
    }

    @Test
    public void shouldHandleGenericThrows()
    {
        // GIVEN
        TypeReference owner = typeReference( ByteCodeUtilsTest.class );
        MethodDeclaration declaration = MethodDeclaration.method( void.class, "fail",
                param( TypeReference.parameterizedType( CodeGenerationTest.Thrower.class, typeParameter( "E" ) ), "thrower" ) )
                .parameterizedWith( "E", extending( Exception.class ) )
                .throwsException( typeParameter( "E" ) ).build( owner );

        // WHEN
        String signature = signature( declaration );
        String[] exceptions = exceptions( declaration );
        // THEN
        assertThat(signature,
                equalTo( "<E:Ljava/lang/Exception;>(Lorg/neo4j/codegen/CodeGenerationTest$Thrower<TE;>;)V^TE;" ));
        assertThat( exceptions, equalTo(new String[]{"java/lang/Exception"} ));
    }

    private void assertTypeName( Class<?> type, String expected )
    {
        // GIVEN
        TypeReference reference = typeReference( type );

        // WHEN
        String byteCodeName = typeName( reference );

        // THEN
        assertThat( byteCodeName, equalTo( expected ) );
    }
}
