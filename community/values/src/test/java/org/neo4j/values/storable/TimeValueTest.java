/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.values.storable;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.function.Supplier;

import org.junit.Test;

import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.assertEquals;
import static org.neo4j.values.storable.TimeValue.parse;
import static org.neo4j.values.storable.TimeValue.time;

public class TimeValueTest
{
    static final Supplier<ZoneId> inUTC = () -> UTC;
    static final Supplier<ZoneId> orFail = () ->
    {
        throw new AssertionError( "should not request timezone" );
    };

    @Test
    public void shouldParseTimeWithOnlyHour() throws Exception
    {
        assertEquals( time( 14, 0, 0, 0, UTC ), parse( "14", inUTC ) );
        assertEquals( time( 4, 0, 0, 0, UTC ), parse( "4", inUTC ) );
        assertEquals( time( 4, 0, 0, 0, UTC ), parse( "04", inUTC ) );
    }

    @Test
    public void shouldParseTimeWithHourAndMinute() throws Exception
    {
        assertEquals( time( 14, 5, 0, 0, UTC ), parse( "1405", inUTC ) );
        assertEquals( time( 14, 5, 0, 0, UTC ), parse( "14:5", inUTC ) );
        assertEquals( time( 4, 15, 0, 0, UTC ), parse( "4:15", inUTC ) );
        assertEquals( time( 9, 7, 0, 0, UTC ), parse( "9:7", inUTC ) );
        assertEquals( time( 3, 4, 0, 0, UTC ), parse( "03:04", inUTC ) );
    }

    @Test
    public void shouldParseTimeWithHourMinuteAndSecond() throws Exception
    {
        assertEquals( time( 14, 5, 17, 0, UTC ), parse( "140517", inUTC ) );
        assertEquals( time( 14, 5, 17, 0, UTC ), parse( "14:5:17", inUTC ) );
        assertEquals( time( 4, 15, 4, 0, UTC ), parse( "4:15:4", inUTC ) );
        assertEquals( time( 9, 7, 19, 0, UTC ), parse( "9:7:19", inUTC ) );
        assertEquals( time( 3, 4, 1, 0, UTC ), parse( "03:04:01", inUTC ) );
    }

    @Test
    public void shouldParseTimeWithHourMinuteSecondAndFractions() throws Exception
    {
        assertEquals( time( 14, 5, 17, 123000000, UTC ), parse( "140517.123", inUTC ) );
        assertEquals( time( 14, 5, 17, 1, UTC ), parse( "14:5:17.000000001", inUTC ) );
        assertEquals( time( 4, 15, 4, 0, UTC ), parse( "4:15:4.000", inUTC ) );
        assertEquals( time( 9, 7, 19, 999999999, UTC ), parse( "9:7:19.999999999", inUTC ) );
        assertEquals( time( 3, 4, 1, 123456789, UTC ), parse( "03:04:01.123456789", inUTC ) );
    }

    @Test
    @SuppressWarnings( "ThrowableNotThrown" )
    public void shouldFailToParseTimeOutOfRange() throws Exception
    {
        assertCannotParse( "24" );
        assertCannotParse( "1760" );
        assertCannotParse( "173260" );
        assertCannotParse( "173250.0000000001" );
    }

    @SuppressWarnings( "UnusedReturnValue" )
    private DateTimeException assertCannotParse( String text )
    {
        try
        {
            parse( text, inUTC );
        }
        catch ( DateTimeException e )
        {
            return e;
        }
        throw new AssertionError( text );
    }
}
