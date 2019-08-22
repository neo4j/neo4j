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

import org.neo4j.internal.kernel.api.procs.Neo4jTypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    private static final Type listOfListOfMap = typeOf( "listOfListOfMap" );

    @ParameterizedTest( name = "{0} to {1}" )
    @MethodSource( "parameters" )
    void shouldDetectCorrectTypeAndMap( Type javaClass, Neo4jTypes.AnyType expected ) throws Throwable
    {
        var actual = new TypeCheckers().checkerFor( javaClass ).type();
        assertEquals( expected, actual );
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
