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
package org.neo4j.codegen;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.neo4j.codegen.ByteCodeUtils.desc;
import static org.neo4j.codegen.ByteCodeUtils.exceptions;
import static org.neo4j.codegen.ByteCodeUtils.signature;
import static org.neo4j.codegen.ByteCodeUtils.type;
import static org.neo4j.codegen.MethodDeclaration.method;
import static org.neo4j.codegen.Parameter.param;
import static org.neo4j.codegen.TypeReference.extending;
import static org.neo4j.codegen.TypeReference.typeParameter;
import static org.neo4j.codegen.TypeReference.typeReference;

public class ByteCodeUtilsTest
{
    @Test
    public void shouldTranslateIntToByteCode()
    {
        // GIVEN
        TypeReference reference = typeReference( int.class );

        // WHEN
        String byteCodeName = type( reference );

        // THEN
        assertThat( byteCodeName, equalTo( "I" ) );
    }

    @Test
    public void shouldTranslateShortToByteCode()
    {
        // GIVEN
        TypeReference reference = typeReference( short.class );

        // WHEN
        String byteCodeName = type( reference );

        // THEN
        assertThat( byteCodeName, equalTo( "S" ) );
    }

    @Test
    public void shouldTranslateCharToByteCode()
    {
        // GIVEN
        TypeReference reference = typeReference( char.class );

        // WHEN
        String byteCodeName = type( reference );

        // THEN
        assertThat( byteCodeName, equalTo( "C" ) );
    }

    @Test
    public void shouldTranslateLongToByteCode()
    {
        // GIVEN
        TypeReference reference = typeReference( long.class );

        // WHEN
        String byteCodeName = type( reference );

        // THEN
        assertThat( byteCodeName, equalTo( "J" ) );
    }

    @Test
    public void shouldTranslateFloatToByteCode()
    {
        // GIVEN
        TypeReference reference = typeReference( float.class );

        // WHEN
        String byteCodeName = type( reference );

        // THEN
        assertThat( byteCodeName, equalTo( "F" ) );
    }

    @Test
    public void shouldTranslateDoubleToByteCode()
    {
        // GIVEN
        TypeReference reference = typeReference( double.class );

        // WHEN
        String byteCodeName = type( reference );

        // THEN
        assertThat( byteCodeName, equalTo( "D" ) );
    }

    @Test
    public void shouldTranslateBooleanToByteCode()
    {
        // GIVEN
        TypeReference reference = typeReference( boolean.class );

        // WHEN
        String byteCodeName = type( reference );

        // THEN
        assertThat( byteCodeName, equalTo( "Z" ) );
    }

    @Test
    public void shouldTranslateVoidToByteCode()
    {
        // GIVEN
        TypeReference reference = typeReference( void.class );

        // WHEN
        String byteCodeName = type( reference );

        // THEN
        assertThat( byteCodeName, equalTo( "V" ) );
    }

    @Test
    public void shouldTranslateReferenceTypeToByteCode()
    {
        // GIVEN
        TypeReference reference = typeReference( String.class );

        // WHEN
        String byteCodeName = type( reference );

        // THEN
        assertThat( byteCodeName, equalTo( "Ljava/lang/String;" ) );
    }

    @Test
    public void shouldTranslateArrayTypeToByteCode()
    {
        // GIVEN
        TypeReference reference = typeReference( boolean[].class );

        // WHEN
        String byteCodeName = type( reference );

        // THEN
        assertThat( byteCodeName, equalTo( "[Z" ) );
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
        String description = signature( declaration );
        String[] exceptions = exceptions( declaration );
        // THEN
        assertThat(description,
                equalTo( "<E:Ljava/lang/Exception;>(Lorg/neo4j/codegen/CodeGenerationTest.Thrower<TE;>;)V^TE;" ));
        assertThat( exceptions, equalTo(new String[]{"java/lang/Exception"} ));
    }



}
