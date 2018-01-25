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

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import static java.time.ZoneOffset.UTC;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateTimeValue.parse;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.LocalDateTimeValue.inUTC;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.TimeValueTest.inUTC;
import static org.neo4j.values.storable.TimeValueTest.orFail;

public class DateTimeValueTest
{
    @Rule
    public final FrozenClockRule clock = new FrozenClockRule();

    @Test
    public void shouldParseDateTime() throws Exception
    {
        assertEquals(
                datetime( date( 2017, 12, 17 ), time( 17, 14, 35, 123456789, UTC ) ),
                parse( "2017-12-17T17:14:35.123456789", inUTC ) );
        assertEquals(
                datetime( date( 2017, 12, 17 ), time( 17, 14, 35, 123456789, UTC ) ),
                parse( "2017-12-17T17:14:35.123456789Z", orFail ) );
        assertEquals(
                datetime( date( 2017, 12, 17 ), time( 17, 14, 35, 123456789, UTC ) ),
                parse( "2017-12-17T17:14:35.123456789+0000", orFail ) );
    }

    @Test
    public void shouldWriteDateTime() throws Exception
    {
        // given
        for ( DateTimeValue value : new DateTimeValue[] {
                datetime( date( 2017, 3, 26 ), localTime( 1, 0, 0, 0 ), ZoneId.of( "Europe/Stockholm" ) ),
                datetime( date( 2017, 3, 26 ), localTime( 2, 0, 0, 0 ), ZoneId.of( "Europe/Stockholm" ) ),
                datetime( date( 2017, 3, 26 ), localTime( 3, 0, 0, 0 ), ZoneId.of( "Europe/Stockholm" ) ),
                datetime( date( 2017, 10, 29 ), localTime( 2, 0, 0, 0 ), ZoneId.of( "Europe/Stockholm" ) ),
                datetime( date( 2017, 10, 29 ), localTime( 3, 0, 0, 0 ), ZoneId.of( "Europe/Stockholm" ) ),
                datetime( date( 2017, 10, 29 ), localTime( 4, 0, 0, 0 ), ZoneId.of( "Europe/Stockholm" ) ),
        } )
        {
            List<DateTimeValue> values = new ArrayList<>( 1 );
            List<LocalDateTimeValue> locals = new ArrayList<>( 1 );
            ValueWriter<RuntimeException> writer = new ThrowingValueWriter.AssertOnly()
            {
                @Override
                public void writeDateTime( long epochSecondUTC, int nano, int offsetSeconds ) throws RuntimeException
                {
                    values.add( datetime( epochSecondUTC, nano, ZoneOffset.ofTotalSeconds( offsetSeconds ) ) );
                    locals.add( localDateTime( epochSecondUTC, nano ) );
                }

                @Override
                public void writeDateTime( long epochSecondUTC, int nano, String zoneId ) throws RuntimeException
                {
                    values.add( datetime( epochSecondUTC, nano, ZoneId.of( zoneId ) ) );
                    locals.add( localDateTime( epochSecondUTC, nano ) );
                }
            };

            // when
            value.writeTo( writer );

            // then
            assertEquals( singletonList( value ), values );
            assertEquals( singletonList( inUTC( value ) ), locals );
        }
    }
}
