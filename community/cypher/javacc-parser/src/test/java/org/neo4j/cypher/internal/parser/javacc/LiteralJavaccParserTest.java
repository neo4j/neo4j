/*
 * Copyright (c) "Neo4j"
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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.internal.ast.factory.LiteralInterpreter;
import org.neo4j.exceptions.UnsupportedTemporalUnitException;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.cypher.internal.ast.factory.LiteralInterpreter.DEFAULT_ZONE_ID;

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

        assertEquals( 8L, parseLiteral( "0o10" ) );
        assertEquals( -8L, parseLiteral( "-0o10" ) );
        assertEquals( -8L, parseLiteral( "-0o10" ) );
        assertEquals( Long.MIN_VALUE, parseLiteral( "-0o1000000000000000000000" ) );

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

    @Test
    void shouldInterpretDate() throws ParseException
    {
        DateValue date = DateValue.date( 2020, 12, 10 );
        assertEquals( date, parseLiteral( "date('2020-12-10')" ) );
        assertEquals( date, parseLiteral( "date({year:2020, month:12, day:10})" ) );

        assertNotNull( parseLiteral( "date()" ) ); // should not throw

        assertThrows( IllegalArgumentException.class, () -> parseLiteral( "date(2020, 12, 10)" ) );
        assertThrows( UnsupportedTemporalUnitException.class, () -> parseLiteral( "date({year:2020, month:12, day:10, timezone: 'America/Los Angeles'})" ) );
        assertNull( parseLiteral( "date(null)" ) );
    }

    @Test
    void shouldInterpretDateTime() throws ParseException
    {
        DateTimeValue date = DateTimeValue.datetime( 2020, 12, 10, 6, 41, 23, 0, DEFAULT_ZONE_ID );
        DateTimeValue dateTimeZone = DateTimeValue.datetime( 2020, 12, 10, 6, 41, 23, 0, ZoneId.of( "America/Los_Angeles" ) );
        assertEquals( date, parseLiteral( "datetime('2020-12-10T6:41:23.0')" ) );
        assertEquals( date, parseLiteral( "datetime({year:2020, month:12, day:10, hour: 6, minute: 41, second: 23})" ) );
        assertEquals( dateTimeZone,
                      parseLiteral( "datetime({year:2020, month:12, day:10, hour: 6, minute: 41, second: 23, timezone: 'America/Los Angeles'})" ) );
        assertNotNull( parseLiteral( "datetime()" ) ); // should not throw

        assertThrows( IllegalArgumentException.class, () -> parseLiteral( "datetime(2020, 12, 10, 6, 41, 23, 0)" ) );

        assertNull( parseLiteral( "datetime(null)" ) );
    }

    @Test
    void shouldInterpretTime() throws ParseException
    {
        Instant instant = Instant.now();
        ZoneOffset currentOffsetForMyZone = DEFAULT_ZONE_ID.getRules().getOffset(instant);
        TimeValue date = TimeValue.time( 6, 41, 23, 0, currentOffsetForMyZone);
        assertEquals( date, parseLiteral( "time('6:41:23.0')" ) );
        assertEquals( date, parseLiteral( "time({hour: 6, minute: 41, second: 23})" ) );
        assertNotNull( parseLiteral( "time()" ) ); // should not throw

        assertThrows( IllegalArgumentException.class, () -> parseLiteral( "time(6, 41, 23, 0)" ) );

        assertNull( parseLiteral( "time(null)" ) );
    }

    @Test
    void shouldInterpretLocalTime() throws ParseException
    {
        LocalTimeValue date = LocalTimeValue.localTime( 6, 41, 23, 0);
        assertEquals( date, parseLiteral( "localtime('6:41:23.0')" ) );
        assertEquals( date, parseLiteral( "localtime({hour: 6, minute: 41, second: 23})" ) );
        assertNotNull( parseLiteral( "localtime()" ) ); // should not throw

        assertThrows( IllegalArgumentException.class, () -> parseLiteral( "localtime(6, 41, 23, 0)" ) );

        assertNull( parseLiteral( "localtime(null)" ) );
    }

    @Test
    void shouldInterpretLocalDateTime() throws ParseException
    {
        LocalDateTimeValue date = LocalDateTimeValue.localDateTime( 2020, 12, 10, 6, 41, 23, 0);
        assertEquals( date, parseLiteral( "localdatetime('2020-12-10T6:41:23.0')" ) );
        assertEquals( date, parseLiteral( "localdatetime({year:2020, month:12, day:10, hour: 6, minute: 41, second: 23})" ) );
        assertNotNull( parseLiteral( "localdatetime()" ) ); // should not throw

        assertThrows( IllegalArgumentException.class, () -> parseLiteral( "localdatetime(2020, 12, 10, 6, 41, 23, 0)" ) );

        assertNull( parseLiteral( "localdatetime(null)" ) );
    }

    @Test
    void shouldInterpretPoint() throws ParseException
    {
        PointValue point = PointValue.parse( "{ x:3, y:0 }" );
        assertEquals( point, parseLiteral( "point({ x:3, y:0 })" ) );

        PointValue point3d = PointValue.parse( "{ x:0, y:4, z:1 }" );
        assertEquals( point3d, parseLiteral( "point({ x:0, y:4, z:1 })" ) );

        PointValue pointWGS84 = PointValue.parse( "{ longitude: 56.7, latitude: 12.78 }" );
        assertEquals( pointWGS84, parseLiteral( "point({ longitude: 56.7, latitude: 12.78 })" ) );
        assertEquals( pointWGS84.getCoordinateReferenceSystem().getName(), "wgs-84" );

        assertThrows( IllegalArgumentException.class, () -> parseLiteral( "point(2020)" ) );

        assertNull( parseLiteral( "point(null)" ) );
    }

    private Object parseLiteral( String str ) throws ParseException
    {
        return new Cypher<>( interpreter, new TestExceptionFactory(), new CypherCharStream( str ) ).Literal();
    }
}
