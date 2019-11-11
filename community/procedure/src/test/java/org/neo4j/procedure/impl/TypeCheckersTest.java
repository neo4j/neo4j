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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTFloat;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNumber;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;

class TypeCheckersTest
{
    private static Stream<Arguments> parameters()
    {
        return Stream.of(
                of( Object.class, NTAny ),
                of( String.class, NTString ),
                of( Map.class, NTMap ),
                of( List.class, NTList( NTAny ) ),
                of( listOfListOfMap, NTList( NTList( NTMap ) ) ),
                of( boolean.class, NTBoolean ),
                of( Number.class, NTNumber ),
                of( long.class, NTInteger ),
                of( Long.class, NTInteger ),
                of( double.class, NTFloat ),
                of( Double.class, NTFloat )
        );
    }

    private static Stream<Arguments> defaultValues()
    {
        return Stream.of(
                of( Object.class, "null", DefaultParameterValue.nullValue( NTAny ) ),
                of( Object.class, "{}", DefaultParameterValue.ntMap( emptyMap() ) ),
                of( Object.class, "[]", DefaultParameterValue.ntList( emptyList(), NTAny ) ),
                of( Object.class, "true", DefaultParameterValue.ntBoolean( true ) ),
                of( Object.class, "false", DefaultParameterValue.ntBoolean( false ) ),
                of( Object.class, "42", DefaultParameterValue.ntInteger( 42 ) ),
                of( Object.class, "13.37", DefaultParameterValue.ntFloat( 13.37 ) ),
                of( Object.class, "foo", DefaultParameterValue.ntString( "foo" ) ),
                of( Object.class, "{foo: 'bar'}", DefaultParameterValue.ntMap( Map.of( "foo", "bar" ) ) ),
                of( Object.class, "['foo', 42, true]", DefaultParameterValue.ntList( List.of( "foo", 42L, true ), NTAny ) ),
                of( Object.class, "[1, 3, 3, 7, 42]", DefaultParameterValue.ntByteArray( new byte[]{1, 3, 3, 7, 42} ) ),

                of( Map.class, "{}", DefaultParameterValue.ntMap( emptyMap() ) ),
                of( Map.class, "{foo: 'bar'}", DefaultParameterValue.ntMap( Map.of( "foo", "bar" ) ) ),

                of( List.class, "[]", DefaultParameterValue.ntList( emptyList(), NTAny ) ),
                of( List.class, "['foo', 42, true]", DefaultParameterValue.ntList( List.of( "foo", 42, true ), NTAny ) ),
                of( List.class, "[1, 3, 3, 7, 42]", DefaultParameterValue.ntList( List.of( 1L, 3L, 3L, 7L, 42L ), NTAny ) ),

                of( boolean.class, "true", DefaultParameterValue.ntBoolean( true ) ),
                of( boolean.class, "false", DefaultParameterValue.ntBoolean( false ) ),
                of( Boolean.class, "true", DefaultParameterValue.ntBoolean( true ) ),
                of( Boolean.class, "false", DefaultParameterValue.ntBoolean( false ) ),

                of( long.class, "42", DefaultParameterValue.ntInteger( 42 ) ),
                of( Long.class, "42", DefaultParameterValue.ntInteger( 42 ) ),
                of( Number.class, "42", DefaultParameterValue.ntInteger( 42 ) ),

                of( double.class, "13.37", DefaultParameterValue.ntFloat( 13.37 ) ),
                of( Double.class, "13.37", DefaultParameterValue.ntFloat( 13.37 ) ),
                of( Number.class, "13.37", DefaultParameterValue.ntFloat( 13.37 ) ),

                of( String.class, "null", DefaultParameterValue.ntString( "null" ) ),
                of( String.class, "{}", DefaultParameterValue.ntString( "{}" ) ),
                of( String.class, "[]", DefaultParameterValue.ntString( "[]" ) ),
                of( String.class, "true", DefaultParameterValue.ntString( "true" ) ),
                of( String.class, "false", DefaultParameterValue.ntString( "false" ) ),
                of( String.class, "42", DefaultParameterValue.ntString( "42" ) ),
                of( String.class, "13.37", DefaultParameterValue.ntString( "13.37" ) ),
                of( String.class, "foo", DefaultParameterValue.ntString( "foo" ) ),
                of( String.class, "{foo: 'bar'}", DefaultParameterValue.ntString( "{foo: 'bar'}" ) ),
                of( String.class, "['foo', 42, true]", DefaultParameterValue.ntString( "['foo', 42, true]" ) ),
                of( String.class, "[1, 3, 3, 7, 42]", DefaultParameterValue.ntString( "[1, 3, 3, 7, 42]" ) )
        );
    }

    private static final Type listOfListOfMap = typeOf( "listOfListOfMap" );

    @ParameterizedTest( name = "{0} to {1}" )
    @MethodSource( "parameters" )
    void shouldDetectCorrectTypeAndMap( Type javaClass, Neo4jTypes.AnyType expected ) throws Throwable
    {
        var actual = new TypeCheckers().checkerFor( javaClass ).type();
        assertEquals( expected, actual );
    }

    @ParameterizedTest( name = "{1} as {0} -> {2}" )
    @MethodSource( "defaultValues" )
    void shouldConvertDefaultValue( Type javaClass, String defaultValue, Object expected ) throws Throwable
    {
        var maybeParsedValue = new TypeCheckers().converterFor( javaClass ).defaultValue( defaultValue );
        assertTrue( maybeParsedValue.isPresent() );
        assertEquals( expected, maybeParsedValue.get() );
    }

    @SuppressWarnings( "unused" )
    interface ClassToGetGenericTypeSignatures
    {
        void listOfListOfMap( List<List<Map<String,Object>>> arg );
    }

    static Type typeOf( String methodName )
    {
        for ( Method method : ClassToGetGenericTypeSignatures.class.getDeclaredMethods() )
        {
            if ( method.getName().equals( methodName ) )
            {
                return method.getGenericParameterTypes()[0];
            }
        }
        throw new AssertionError( "No method named " + methodName );
    }
}
