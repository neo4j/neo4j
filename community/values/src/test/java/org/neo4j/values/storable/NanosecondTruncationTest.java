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

import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.MICRO_OF_SECOND;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;

public class NanosecondTruncationTest
{
    static final Supplier<ZoneId> inUTC = () -> UTC;

    @Test
    public void datetimeShouldEqualSameWithOtherNano()
    {
        assertEqual( datetime( 2018, 1, 31, 10, 52, 5, 6, UTC ), datetime( 2018, 1, 31, 10, 52, 5, 7, UTC ) );
    }

    @Test
    public void localdatetimeShouldNotEqualOther()
    {
        assertNotEqual( localDateTime( 2018, 1, 31, 10, 52, 5, 0 ), localDateTime( 2018, 1, 31, 10, 52, 6, 0 ) );
    }

    @Test
    public void localdatetimeShouldEqualSameWithOtherNano()
    {
        assertEqual( localDateTime( 2018, 1, 31, 10, 52, 5, 6 ), localDateTime( 2018, 1, 31, 10, 52, 5, 7 ) );
    }

    @Test
    public void localtimeShouldNotEqualOther()
    {
        assertNotEqual( localTime( 10, 52, 5, 1037 ), localTime( 10, 52, 5, 2037 ) );
    }

    @Test
    public void localtimeShouldEqualSameWithOtherNano()
    {
        assertEqual( localTime( 10, 52, 5, 6 ), localTime( 10, 52, 5, 7 ) );
    }

    @Test
    public void timeShouldEqualSameWithOtherNano()
    {
        assertEqual( time( 10, 52, 5, 6, UTC ), time( 10, 52, 5, 7, UTC ) );
    }


    @Test
    public void durationShouldEqualSameWithOtherNano()
    {
        assertEqual( duration( 0, 0, 0, 6 ), duration( 0, 0, 0, 7 ) );
    }

    @Test
    public void durationShouldNotEqualOther()
    {
        assertNotEqual( duration( 0, 0, 0, 6999 ), duration( 0, 0, 0, 7000 ) );
    }

    @Test
    public void localtimeShouldTruncateOnCreation()
    {
        LocalTimeValue lt = localTime( 10, 52, 5, 123456789 );
        assertEquals( 123456000, lt.get( NANO_OF_SECOND ) );
        assertEquals( 123456, lt.get( MICRO_OF_SECOND ) );
    }

    @Test
    public void timeShouldTruncateOnParse()
    {
        TimeValue t = TimeValue.parse( "10:12:24.123456789", inUTC );
        assertEquals( 123456000, t.get( NANO_OF_SECOND ) );
        assertEquals( 123456, t.get( MICRO_OF_SECOND ) );
    }

    @Test
    public void durationShouldTruncateOnParse()
    {
        DurationValue d = DurationValue.parse( "PT1.123456789S" );
        assertEquals( 123456000, d.get( NANOS ) );
    }

    @Test
    public void timeShouldTruncateOnBuild()
    {
        Map<String,AnyValue> map = new HashMap<>(  );
        map.put( "hour", Values.intValue( 11 ) );
        map.put( "minute", Values.intValue( 48 ) );
        map.put( "second", Values.intValue( 23 ) );
        map.put( "nanosecond", Values.intValue( 1234 ) );

        TimeValue t = TimeValue.build( VirtualValues.map( map ), inUTC );

        assertEquals( 1000, t.get( NANO_OF_SECOND ) );
        assertEquals( 1, t.get( MICRO_OF_SECOND ) );
    }

    @Test
    public void datetimeShouldTruncateOnTruncation ()
    {
        Map<String,AnyValue> map = new HashMap<>(  );
        map.put( "nanosecond", Values.intValue( 1234 ) );

        LocalDateTimeValue ldt = localDateTime( 2018, 4, 11, 11, 48, 12, 123456789 );
        DateTimeValue dt = DateTimeValue.truncate( SECONDS, ldt, VirtualValues.map( map ), inUTC );

        assertEquals( 1000, dt.get( NANO_OF_SECOND ) );
        assertEquals( 1, dt.get( MICRO_OF_SECOND ) );
    }

    @Test
    public void shouldTruncateOnArithmetics()
    {
        DurationValue dur = duration( 0, 0, 0, 1000 );
        DurationValue res1 = dur.mul( Values.doubleValue( 0.5 ) );
        DurationValue res2 = dur.mul( Values.doubleValue( 1.5 ) );

        assertEquals(0, res1.get( NANOS ) );
        assertEquals(1000, res2.get( NANOS ) );
    }
}
