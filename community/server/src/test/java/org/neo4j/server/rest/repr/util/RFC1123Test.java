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
package org.neo4j.server.rest.repr.util;

import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RFC1123Test
{
    private final Calendar calendar = Calendar.getInstance( RFC1123.GMT, Locale.US );

    @Test
    public void shouldParseRFC1123() throws Exception
    {
        // given
        String input = "Mon, 15 Aug 2005 15:52:01 +0000";

        // when
        Date result = RFC1123.parseTimestamp( input );

        // then
        calendar.setTime( result );
        assertEquals( Calendar.MONDAY, calendar.get( Calendar.DAY_OF_WEEK ) );
        assertEquals( 15, calendar.get( Calendar.DAY_OF_MONTH ) );
        assertEquals( Calendar.AUGUST, calendar.get( Calendar.MONTH ) );
        assertEquals( 2005, calendar.get( Calendar.YEAR ) );
        assertEquals( 15, calendar.get( Calendar.HOUR_OF_DAY ) );
        assertEquals( 52, calendar.get( Calendar.MINUTE ) );
        assertEquals( 1, calendar.get( Calendar.SECOND ) );
    }

    @Test
    public void shouldFormatRFC1123() throws Exception
    {
        // given
        String input = "Mon, 15 Aug 2005 15:52:01 +0000";

        // when
        String output = RFC1123.formatDate( RFC1123.parseTimestamp( input ) );

        // then
        assertEquals( input, output );
    }

    @Test
    public void shouldReturnSameInstanceInSameThread() throws Exception
    {
        // given
        RFC1123 instance = RFC1123.instance();

        // when
        RFC1123 instance2 = RFC1123.instance();

        // then
        assertTrue(
                "Expected to get same instance from second call to RFC1123.instance() in same thread",
                instance == instance2 );
    }
}
