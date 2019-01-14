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
package org.neo4j.kernel.impl.proc;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.kernel.impl.proc.TypeMappers.TypeChecker;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTFloat;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNumber;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;

@RunWith( Parameterized.class )
public class TypeMappersTest
{
    @Parameterized.Parameter( 0 )
    public Type javaClass;
    @Parameterized.Parameter( 1 )
    public Neo4jTypes.AnyType neoType;
    @Parameterized.Parameter( 2 )
    public Object javaValue;
    @Parameterized.Parameter( 3 )
    public Object expectedNeoValue;

    @Parameterized.Parameters( name = "{0} to {1}" )
    public static List<Object[]> conversions()
    {
        return asList(
                new Object[]{Object.class, NTAny, "", ""},
                new Object[]{Object.class, NTAny, null, null},
                new Object[]{Object.class, NTAny, 1, 1},
                new Object[]{Object.class, NTAny, true, true},
                new Object[]{Object.class, NTAny, asList( 1, 2, 3 ), asList( 1, 2, 3 )},
                new Object[]{Object.class, NTAny, new HashMap<>(), new HashMap<>()},

                new Object[]{String.class, NTString, "", ""},
                new Object[]{String.class, NTString, "not empty", "not empty"},
                new Object[]{String.class, NTString, null, null},

                new Object[]{Map.class, NTMap, new HashMap<>(), new HashMap<>()},
                new Object[]{Map.class, NTMap, getKMap(), getKMap()},
                new Object[]{Map.class, NTMap, null, null},

                new Object[]{List.class, NTList( NTAny ), emptyList(), emptyList()},
                new Object[]{List.class, NTList( NTAny ), asList( 1, 2, 3, 4 ), asList( 1, 2, 3, 4 )},
                new Object[]{List.class, NTList( NTAny ), asList( asList( 1, 2 ), asList( "three", "four" ) ),
                        asList( asList( 1, 2 ), asList( "three", "four" ) )},
                new Object[]{List.class, NTList( NTAny ), null, null},

                new Object[]{listOfListOfMap, NTList( NTList( NTMap ) ), asList(), asList()},

                new Object[]{boolean.class, NTBoolean, false, false},
                new Object[]{boolean.class, NTBoolean, true, true},
                new Object[]{boolean.class, NTBoolean, null, null},
                new Object[]{Boolean.class, NTBoolean, false, false},
                new Object[]{Boolean.class, NTBoolean, true, true},
                new Object[]{Boolean.class, NTBoolean, null, null},

                new Object[]{Number.class, NTNumber, 1L, 1L},
                new Object[]{Number.class, NTNumber, 0L, 0L},
                new Object[]{Number.class, NTNumber, null, null},
                new Object[]{Number.class, NTNumber, Long.MIN_VALUE, Long.MIN_VALUE},
                new Object[]{Number.class, NTNumber, Long.MAX_VALUE, Long.MAX_VALUE},
                new Object[]{Number.class, NTNumber, 1D, 1D},
                new Object[]{Number.class, NTNumber, 0D, 0D},
                new Object[]{Number.class, NTNumber, 1.234D, 1.234D},
                new Object[]{Number.class, NTNumber, null, null},
                new Object[]{Number.class, NTNumber, Double.MIN_VALUE, Double.MIN_VALUE},
                new Object[]{Number.class, NTNumber, Double.MAX_VALUE, Double.MAX_VALUE},

                new Object[]{long.class, NTInteger, 1L, 1L},
                new Object[]{long.class, NTInteger, 0L, 0L},
                new Object[]{long.class, NTInteger, null, null},
                new Object[]{long.class, NTInteger, Long.MIN_VALUE, Long.MIN_VALUE},
                new Object[]{long.class, NTInteger, Long.MAX_VALUE, Long.MAX_VALUE},
                new Object[]{Long.class, NTInteger, 1L, 1L},
                new Object[]{Long.class, NTInteger, 0L, 0L},
                new Object[]{Long.class, NTInteger, null, null},
                new Object[]{Long.class, NTInteger, Long.MIN_VALUE, Long.MIN_VALUE},
                new Object[]{Long.class, NTInteger, Long.MAX_VALUE, Long.MAX_VALUE},

                new Object[]{double.class, NTFloat, 1D, 1D},
                new Object[]{double.class, NTFloat, 0D, 0D},
                new Object[]{double.class, NTFloat, 1.234D, 1.234D},
                new Object[]{double.class, NTFloat, null, null},
                new Object[]{double.class, NTFloat, Double.MIN_VALUE, Double.MIN_VALUE},
                new Object[]{double.class, NTFloat, Double.MAX_VALUE, Double.MAX_VALUE},
                new Object[]{Double.class, NTFloat, 1D, 1D},
                new Object[]{Double.class, NTFloat, 0D, 0D},
                new Object[]{Double.class, NTFloat, 1.234D, 1.234D},
                new Object[]{Double.class, NTFloat, null, null},
                new Object[]{Double.class, NTFloat, Double.MIN_VALUE, Double.MIN_VALUE},
                new Object[]{Double.class, NTFloat, Double.MAX_VALUE, Double.MAX_VALUE}
        );
    }

    private static HashMap<String,Object> getKMap()
    {
        return new HashMap<String,Object>()
        {{
            put( "k", 1 );
        }};
    }

    @Test
    public void shouldDetectCorrectType() throws Throwable
    {
        // When
        Neo4jTypes.AnyType type = new TypeMappers().toNeo4jType( javaClass );

        // Then
        assertEquals( neoType, type );
    }

    @Test
    public void shouldMapCorrectly() throws Throwable
    {
        // Given
        TypeChecker mapper = new TypeMappers().checkerFor( javaClass );

        // When
        Object converted = mapper.typeCheck( javaValue );

        // Then
        Assert.assertEquals( expectedNeoValue, converted );
    }

    static Type listOfListOfMap = typeOf( "listOfListOfMap" );

    interface ClassToGetGenericTypeSignatures
    {
        void listOfListOfMap( List<List<Map<String,Object>>> arg );
    }

    static Type typeOf( String methodName )
    {
        try
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
        catch ( Throwable e )
        {
            throw new AssertionError( e );
        }
    }
}
