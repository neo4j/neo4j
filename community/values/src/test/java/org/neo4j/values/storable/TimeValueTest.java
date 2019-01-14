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
package org.neo4j.values.storable;

import org.junit.Test;

import java.time.DateTimeException;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.neo4j.values.utils.TemporalParseException;

import static java.time.ZoneOffset.UTC;
import static java.time.ZoneOffset.ofHours;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.neo4j.values.storable.TimeValue.parse;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;

public class TimeValueTest
{
    static final Supplier<ZoneId> inUTC = () -> UTC;
    static final Supplier<ZoneId> orFail = () ->
    {
        throw new AssertionError( "should not request timezone" );
    };

    @Test
    public void shouldParseTimeWithOnlyHour()
    {
        assertEquals( time( 14, 0, 0, 0, UTC ), parse( "14", inUTC ) );
        assertEquals( time( 4, 0, 0, 0, UTC ), parse( "4", inUTC ) );
        assertEquals( time( 4, 0, 0, 0, UTC ), parse( "04", inUTC ) );
    }

    @Test
    public void shouldParseTimeWithHourAndMinute()
    {
        assertEquals( time( 14, 5, 0, 0, UTC ), parse( "1405", inUTC ) );
        assertEquals( time( 14, 5, 0, 0, UTC ), parse( "14:5", inUTC ) );
        assertEquals( time( 4, 15, 0, 0, UTC ), parse( "4:15", inUTC ) );
        assertEquals( time( 9, 7, 0, 0, UTC ), parse( "9:7", inUTC ) );
        assertEquals( time( 3, 4, 0, 0, UTC ), parse( "03:04", inUTC ) );
    }

    @Test
    public void shouldParseTimeWithHourMinuteAndSecond()
    {
        assertEquals( time( 14, 5, 17, 0, UTC ), parse( "140517", inUTC ) );
        assertEquals( time( 14, 5, 17, 0, UTC ), parse( "14:5:17", inUTC ) );
        assertEquals( time( 4, 15, 4, 0, UTC ), parse( "4:15:4", inUTC ) );
        assertEquals( time( 9, 7, 19, 0, UTC ), parse( "9:7:19", inUTC ) );
        assertEquals( time( 3, 4, 1, 0, UTC ), parse( "03:04:01", inUTC ) );
    }

    @Test
    public void shouldParseTimeWithHourMinuteSecondAndFractions()
    {
        assertEquals( time( 14, 5, 17, 123000000, UTC ), parse( "140517.123", inUTC ) );
        assertEquals( time( 14, 5, 17, 1, UTC ), parse( "14:5:17.000000001", inUTC ) );
        assertEquals( time( 4, 15, 4, 0, UTC ), parse( "4:15:4.000", inUTC ) );
        assertEquals( time( 9, 7, 19, 999999999, UTC ), parse( "9:7:19.999999999", inUTC ) );
        assertEquals( time( 3, 4, 1, 123456789, UTC ), parse( "03:04:01.123456789", inUTC ) );
    }

    @Test
    @SuppressWarnings( "ThrowableNotThrown" )
    public void shouldFailToParseTimeOutOfRange()
    {
        assertCannotParse( "24" );
        assertCannotParse( "1760" );
        assertCannotParse( "173260" );
        assertCannotParse( "173250.0000000001" );
    }

    @Test
    public void shouldWriteTime()
    {
        // given
        for ( TimeValue time : new TimeValue[] {
                time( 11, 30, 4, 112233440, ofHours( 3 ) ),
                time( 23, 59, 59, 999999999, ofHours( 18 ) ),
                time( 23, 59, 59, 999999999, ofHours( -18 ) ),
                time( 0, 0, 0, 0, ofHours( -18 ) ),
                time( 0, 0, 0, 0, ofHours( 18 ) ),
        } )
        {
            List<TimeValue> values = new ArrayList<>( 1 );
            ValueWriter<RuntimeException> writer = new ThrowingValueWriter.AssertOnly()
            {
                @Override
                public void writeTime( OffsetTime offsetTime )
                {
                    values.add( time( offsetTime ) );
                }
            };

            // when
            time.writeTo( writer );

            // then
            assertEquals( singletonList( time ), values );
        }
    }

    @Test
    public void shouldAddDurationToTimes()
    {
        assertEquals( time(12, 15, 0, 0, UTC),
                time(12, 0, 0, 0, UTC).add( DurationValue.duration( 1, 1, 900, 0 ) ) );
        assertEquals( time(12, 0, 2, 0, UTC),
                time(12, 0, 0, 0, UTC).add( DurationValue.duration( 0, 0, 1, 1_000_000_000 ) ) );
        assertEquals( time(12, 0, 0, 0, UTC),
                time(12, 0, 0, 0, UTC).add( DurationValue.duration( 0, 0, 1, -1_000_000_000 ) ) );
    }

    @Test
    public void shouldReuseInstanceInArithmetics()
    {
        final TimeValue noon = time( 12, 0, 0, 0, UTC );
        assertSame( noon,
                noon.add( DurationValue.duration( 0, 0, 0, 0 ) ) );
        assertSame( noon,
                noon.add( DurationValue.duration( 1, 1, 0, 0 ) ) );
        assertSame( noon,
                noon.add( DurationValue.duration( -1, 1, 0, -0 ) ) );
    }

    @Test
    public void shouldSubtractDurationFromTimes()
    {
        assertEquals( time(12, 0, 0, 0, UTC),
                time(12, 15, 0, 0, UTC).sub( DurationValue.duration( 1, 1, 900, 0 ) ) );
        assertEquals( time(12, 0, 0, 0, UTC),
                time(12, 0, 2, 0, UTC).sub( DurationValue.duration( 0, 0, 1, 1_000_000_000 ) ) );
        assertEquals( time(12, 0, 0, 0, UTC),
                time(12, 0, 0, 0, UTC).sub( DurationValue.duration( 0, 0, 1, -1_000_000_000 ) ) );
    }

    @Test
    public void shouldEqualItself()
    {
        assertEqual( time( 10, 52, 5, 6, UTC ), time( 10, 52, 5, 6, UTC ) );
    }

    @Test
    public void shouldNotEqualSameInstantButDifferentTimezone()
    {
        assertNotEqual( time( 10000, UTC ), time( 10000, ZoneOffset.of( "+01:00" ) ) );
    }

    @Test
    public void shouldNotEqualSameInstantInSameLocalTimeButDifferentTimezone()
    {
        assertNotEqual( time( 10, 52, 5, 6, UTC ), time( 11, 52, 5, 6, "+01:00" ) );
    }

    @Test
    public void shouldBeAbleToParseTimeThatOverridesHeaderInformation()
    {
        String headerInformation = "{timezone:-01:00}";
        String data = "14:05:17Z";

        TimeValue expected = TimeValue.parse( data, orFail );
        TimeValue actual = TimeValue.parse( data, orFail, TemporalValue.parseHeaderInformation( headerInformation ) );

        assertEqual( expected, actual );
        assertEquals( UTC, actual.getZoneOffset() );
    }

    @Test
    public void shouldBeAbleToParseTimeWithoutTimeZoneWithHeaderInformation()
    {
        String headerInformation = "{timezone:-01:00}";
        String data = "14:05:17";

        TimeValue expected = TimeValue.parse( data, () -> ZoneId.of( "-01:00" ) );
        TimeValue unexpected = TimeValue.parse( data, inUTC );
        TimeValue actual = TimeValue.parse( data, orFail, TemporalValue.parseHeaderInformation( headerInformation ) );

        assertEqual( expected, actual );
        assertNotEquals( unexpected, actual );
    }

    @Test
    public void shouldWriteDerivedValueThatIsEqual()
    {
        TimeValue value1 = time( 42, ZoneOffset.of( "-18:00" ) );
        TimeValue value2 = time( value1.temporal() );

        OffsetTime offsetTime1 = write( value1 );
        OffsetTime offsetTime2 = write( value2 );

        assertEquals( offsetTime1, offsetTime2 );
    }

    @Test
    public void shouldCompareDerivedValue()
    {
        TimeValue value1 = time( 4242, ZoneOffset.of( "-12:00" ) );
        TimeValue value2 = time( value1.temporal() );

        assertEquals( 0, value1.unsafeCompareTo( value2 ) );
    }

    @SuppressWarnings( "UnusedReturnValue" )
    private TemporalParseException assertCannotParse( String text )
    {
        try
        {
            parse( text, inUTC );
        }
        catch ( TemporalParseException e )
        {
            return e;
        }
        throw new AssertionError( text );
    }

    private static OffsetTime write( TimeValue value )
    {
        AtomicReference<OffsetTime> result = new AtomicReference<>();
        value.writeTo( new ThrowingValueWriter.AssertOnly()
        {
            @Override
            public void writeTime( OffsetTime offsetTime )
            {
                result.set( offsetTime );
            }
        } );
        return result.get();
    }
}
