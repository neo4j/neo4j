/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.parser.javacc;

import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.internal.ast.factory.LiteralInterpreter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@SuppressWarnings( "ConstantConditions" )
public class LiteralJavaccParserTest
{
    private final LiteralInterpreter interpreter = new LiteralInterpreter();

    @Test
    void shouldInterpretNumbers() throws ParseException
    {
        assertEquals( 0L, parseLiteral( "0" ) );
        assertEquals( 12345L, parseLiteral( "12345" ) );
        assertEquals( -12345L, parseLiteral( "-12345" ) );
        assertEquals( Long.MAX_VALUE, parseLiteral( Long.toString( Long.MAX_VALUE ) ) );
        assertEquals( Long.MIN_VALUE, parseLiteral( Long.toString( Long.MIN_VALUE ) ) );

        assertEquals( 8L, parseLiteral( "010" ) );
        assertEquals( -8L, parseLiteral( "-010" ) );
        assertEquals( Long.MIN_VALUE, parseLiteral( "-01000000000000000000000" ) );

        assertEquals( 255L, parseLiteral( "0xff" ) );
        assertEquals( -255L, parseLiteral( "-0xff" ) );
        assertEquals( Long.MIN_VALUE, parseLiteral( "-0x8000000000000000" ) );

        assertEquals( 0L, parseLiteral( "0" ) );
        assertEquals( 0.0d, parseLiteral( "0.0" ) );
        assertEquals( -0.0d, parseLiteral( "-0.0") );
        assertEquals( 1.0d, parseLiteral( "1.0" ) );
        assertEquals( 98723.0e31d, parseLiteral( "98723.0e31" ) );
        assertEquals( Double.MAX_VALUE, parseLiteral( Double.toString( Double.MAX_VALUE ) ) );
        assertEquals( Double.MIN_VALUE, parseLiteral( Double.toString( Double.MIN_VALUE ) ) );
    }

    @Test
    void shouldInterpretString() throws ParseException
    {
        assertEquals( "a string", parseLiteral( "'a string'" ) );

        assertEquals( "ÅÄü", parseLiteral( "'ÅÄü'" ) );
        assertEquals( "Ελληνικά", parseLiteral( "'Ελληνικά'" ) );
        assertEquals( "\uD83D\uDCA9", parseLiteral( "'\uD83D\uDCA9'" ) );
    }

    @Test
    void shouldInterpretNull() throws ParseException
    {
        assertNull( parseLiteral( "null" ) );
    }

    @Test
    void shouldInterpretBoolean() throws ParseException
    {
        assertEquals( true, parseLiteral( "true" ) );
        assertEquals( false, parseLiteral( "false" ) );
    }

    @Test
    void shouldInterpretList() throws ParseException
    {
        assertThat( (List<?>)parseLiteral( "[1,2,3]" ), contains( 1L, 2L, 3L ) );
        assertThat( (List<?>)parseLiteral( " [ 1, 2, 3 ] " ), contains( 1L, 2L, 3L ) );
    }

    @Test
    void shouldInterpretNestedList() throws ParseException
    {
        List<?> list1 = (List<?>) parseLiteral( "[1,[2,[3]]]" );

        assertThat( list1, hasSize( 2 ) );
        assertThat( list1.get( 0 ), equalTo( 1L ) );

        List<?> list2 = (List<?>) list1.get( 1 );
        assertThat( list2, hasSize( 2 ) );
        assertThat( list2.get( 0 ), equalTo( 2L ) );

        List<?> list3 = (List<?>) list2.get( 1 );
        assertThat( list3, hasSize( 1 ) );
        assertThat( list3.get( 0 ), equalTo( 3L ) );
    }

    @Test
    void shouldInterpretMap() throws ParseException
    {
        assertThat( (Map<?,?>)parseLiteral( "{}}" ), anEmptyMap() );
        assertThat( (Map<?,?>) parseLiteral( "{age: 2}" ),
                    allOf(
                            aMapWithSize( 1 ), // make sure there are no extra key/value pairs in map
                            hasEntry( "age", 2L )
                    ) );

        assertThat( (Map<?,?>)parseLiteral( "{name: 'Scotty', age: 4, height: 94.3}" ),
                     allOf(
                             aMapWithSize( 3 ),
                             hasEntry( "name", (Object)"Scotty" ),
                             hasEntry( "age", (Object)4L ),
                             hasEntry( "height", 94.3 )
                     ) );
    }

    @Test
    void shouldInterpretNestedMap() throws ParseException
    {
        Map<?,?> map1 = (Map<?,?>) parseLiteral( "{k1: 1, map2: {k2: 2, map3: {k3: 3}}}" );

        assertThat( map1, aMapWithSize( 2 ) );
        assertThat( map1.get( "k1" ), equalTo( 1L ) );

        Map<?,?> map2 = (Map<?,?>) map1.get( "map2" );
        assertThat( map2, aMapWithSize( 2 ) );
        assertThat( map2.get( "k2" ), equalTo( 2L ) );

        Map<?,?> map3 = (Map<?,?>) map2.get( "map3" );
        assertThat( map3, aMapWithSize( 1 ) );
        assertThat( map3.get( "k3" ), equalTo( 3L ) );
    }

    private Object parseLiteral( String str ) throws ParseException
    {
        return new Cypher<>( interpreter, new TestExceptionFactory(), new StringReader( str ) ).Literal();
    }
}
