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

import org.hamcrest.Matchers;
import org.junit.Test;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.values.utils.InvalidValuesArgumentException;
import org.neo4j.values.utils.TemporalParseException;
import org.neo4j.values.utils.TemporalUtil;

import static java.time.ZoneOffset.UTC;
import static java.time.ZoneOffset.ofHours;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Pair.pair;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DurationValue.between;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.DurationValue.durationBetween;
import static org.neo4j.values.storable.DurationValue.parse;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;

public class DurationValueTest
{
    @Test
    public void shouldNormalizeNanoseconds()
    {
        // given
        DurationValue evenPos = duration( 0, 0, 0, 1_000_000_000 );
        DurationValue evenNeg = duration( 0, 0, 0, -1_000_000_000 );
        DurationValue pos = duration( 0, 0, 0, 1_500_000_000 );
        DurationValue neg = duration( 0, 0, 0, -1_400_000_000 );

        // then
        assertEquals( "+nanos", 500_000_000, pos.get( NANOS ) );
        assertEquals( "+seconds", 1, pos.get( SECONDS ) );
        assertEquals( "+nanos", 600_000_000, neg.get( NANOS ) );
        assertEquals( "-seconds", -2, neg.get( SECONDS ) );

        assertEquals( "+nanos", 0, evenPos.get( NANOS ) );
        assertEquals( "+seconds", 1, evenPos.get( SECONDS ) );
        assertEquals( "+nanos", 0, evenNeg.get( NANOS ) );
        assertEquals( "-seconds", -1, evenNeg.get( SECONDS ) );
    }

    @Test
    public void shouldFormatDurationToString()
    {
        testDurationToString( 1, 0, "PT1S" );
        testDurationToString( -1, 0, "PT-1S" );

        testDurationToString( 59, -500_000_000, "PT58.5S" );
        testDurationToString( 59, 500_000_000, "PT59.5S" );
        testDurationToString( 60, -500_000_000, "PT59.5S" );
        testDurationToString( 60, 500_000_000, "PT1M0.5S" );
        testDurationToString( 61, -500_000_000, "PT1M0.5S" );

        testDurationToString( -59, 500_000_000, "PT-58.5S" );
        testDurationToString( -59, -500_000_000, "PT-59.5S" );
        testDurationToString( -60, 500_000_000, "PT-59.5S" );
        testDurationToString( -60, -500_000_000, "PT-1M-0.5S" );
        testDurationToString( -61, 500_000_000, "PT-1M-0.5S" );
        testDurationToString( -61, -500_000_000, "PT-1M-1.5S" );

        testDurationToString( 0, 5, "PT0.000000005S" );
        testDurationToString( 0, -5, "PT-0.000000005S" );
        testDurationToString( 0, 999_999_999, "PT0.999999999S" );
        testDurationToString( 0, -999_999_999, "PT-0.999999999S" );

        testDurationToString( 1, 5, "PT1.000000005S" );
        testDurationToString( -1, -5, "PT-1.000000005S" );
        testDurationToString( 1, -5, "PT0.999999995S" );
        testDurationToString( -1, 5, "PT-0.999999995S" );
        testDurationToString( 1, 999999999, "PT1.999999999S" );
        testDurationToString( -1, -999999999, "PT-1.999999999S" );
        testDurationToString( 1, -999999999, "PT0.000000001S" );
        testDurationToString( -1, 999999999, "PT-0.000000001S" );

        testDurationToString( -78036, -143000000, "PT-21H-40M-36.143S" );
    }

    private void testDurationToString( long seconds, int nanos, String expectedValue )
    {
        assertEquals( expectedValue, duration( 0, 0, seconds, nanos ).prettyPrint() );
    }

    @Test
    public void shouldNormalizeSecondsAndNanos()
    {
        // given
        DurationValue pos = duration( 0, 0, 5, -1_400_000_000 );
        DurationValue neg = duration( 0, 0, -5, 1_500_000_000 );
        DurationValue x = duration( 0, 0, 1, -1_400_000_000 );

        DurationValue y = duration( 0, 0, -59, -500_000_000 );
        DurationValue y2 = duration( 0, 0, -60, 500_000_000 );

        // then
        assertEquals( "+nanos", 600_000_000, pos.get( NANOS ) );
        assertEquals( "+seconds", 3, pos.get( SECONDS ) );
        assertEquals( "+nanos", 500_000_000, neg.get( NANOS ) );
        assertEquals( "-seconds", -4, neg.get( SECONDS ) );
        assertEquals( "+nanos", 600_000_000, x.get( NANOS ) );
        assertEquals( "-seconds", -1, x.get( SECONDS ) );
        assertEquals( "+nanos", 500_000_000, y.get( NANOS ) );
        assertEquals( "-seconds", -60, y.get( SECONDS ) );
        assertEquals( "+nanos", 500_000_000, y2.get( NANOS ) );
        assertEquals( "-seconds", -60, y2.get( SECONDS ) );
    }

    @Test
    public void shouldFormatAsPrettyString()
    {
        assertEquals( "P1Y", prettyPrint( 12, 0, 0, 0 ) );
        assertEquals( "P5M", prettyPrint( 5, 0, 0, 0 ) );
        assertEquals( "P84D", prettyPrint( 0, 84, 0, 0 ) );
        assertEquals( "P2Y4M11D", prettyPrint( 28, 11, 0, 0 ) );
        assertEquals( "PT5S", prettyPrint( 0, 0, 5, 0 ) );
        assertEquals( "PT30H22M8S", prettyPrint( 0, 0, 109328, 0 ) );
        assertEquals( "PT7.123456789S", prettyPrint( 0, 0, 7, 123_456_789 ) );
        assertEquals( "PT0.000000001S", prettyPrint( 0, 0, 0, 1 ) );
        assertEquals( "PT0.1S", prettyPrint( 0, 0, 0, 100_000_000 ) );
        assertEquals( "PT0S", prettyPrint( 0, 0, 0, 0 ) );
        assertEquals( "PT1S", prettyPrint( 0, 0, 0, 1_000_000_000 ) );
        assertEquals( "PT-1S", prettyPrint( 0, 0, 0, -1_000_000_000 ) );
        assertEquals( "PT1.5S", prettyPrint( 0, 0, 1, 500_000_000 ) );
        assertEquals( "PT-1.4S", prettyPrint( 0, 0, -1, -400_000_000 ) );
    }

    private static String prettyPrint( long months, long days, long seconds, int nanos )
    {
        return duration( months, days, seconds, nanos ).prettyPrint();
    }

    @Test
    public void shouldHandleLargeNanos()
    {
        DurationValue duration = DurationValue.duration( 0L, 0L, 0L, Long.MAX_VALUE );
        assertEquals( Long.MAX_VALUE, duration.get( "nanoseconds" ).value() );
    }

    @Test
    public void shouldParseDuration()
    {
        assertEquals(
                duration( 14, 25, 18367, 800_000_000 ),
                parse( "+P1Y2M3W4DT5H6M7.8S" ) );

        assertEquals( duration( 0, 0, 0, -100000000 ), parse( "PT-0.1S" ) );
        assertEquals( duration( 0, 0, 0, -20000000 ), parse( "PT-0.02S" ) );
        assertEquals( duration( 0, 0, 0, -3000000 ), parse( "PT-0.003S" ) );
        assertEquals( duration( 0, 0, 0, -400000 ), parse( "PT-0.0004S" ) );
        assertEquals( duration( 0, 0, 0, -50000 ), parse( "PT-0.00005S" ) );
        assertEquals( duration( 0, 0, 0, -6000 ), parse( "PT-0.000006S" ) );
        assertEquals( duration( 0, 0, 0, -700 ), parse( "PT-0.0000007S" ) );
        assertEquals( duration( 0, 0, 0, -80 ), parse( "PT-0.00000008S" ) );
        assertEquals( duration( 0, 0, 0, -9 ), parse( "PT-0.000000009S" ) );

        assertEquals( duration( 0, 0, 0, 900_000_000 ), parse( "PT0.900000000S" ) );
        assertEquals( duration( 0, 0, 0, 800_000_000 ), parse( "PT0.80000000S" ) );
        assertEquals( duration( 0, 0, 0, 700_000_000 ), parse( "PT0.7000000S" ) );
        assertEquals( duration( 0, 0, 0, 600_000_000 ), parse( "PT0.600000S" ) );
        assertEquals( duration( 0, 0, 0, 500_000_000 ), parse( "PT0.50000S" ) );
        assertEquals( duration( 0, 0, 0, 400_000_000 ), parse( "PT0.4000S" ) );
        assertEquals( duration( 0, 0, 0, 300_000_000 ), parse( "PT0.300S" ) );
        assertEquals( duration( 0, 0, 0, 200_000_000 ), parse( "PT0.20S" ) );
        assertEquals( duration( 0, 0, 0, 100_000_000 ), parse( "PT0.1S" ) );

        assertParsesOne( "P", "Y", 12, 0, 0 );
        assertParsesOne( "P", "M", 1, 0, 0 );
        assertParsesOne( "P", "W", 0, 7, 0 );
        assertParsesOne( "P", "D", 0, 1, 0 );
        assertParsesOne( "PT", "H", 0, 0, 3600 );
        assertParsesOne( "PT", "M", 0, 0, 60 );
        assertParsesOne( "PT", "S", 0, 0, 1 );

        assertEquals( duration( 0, 0, -1, -100_000_000 ), parse( "PT-1,1S" ) );

        assertEquals( duration( 10, 0, 0, 0 ), parse( "P1Y-2M" ) );
        assertEquals( duration( 0, 20, 0, 0 ), parse( "P3W-1D" ) );
        assertEquals( duration( 0, 0, 3000, 0 ), parse( "PT1H-10M" ) );
        assertEquals( duration( 0, 0, 3000, 0 ), parse( "PT1H-600S" ) );
        assertEquals( duration( 0, 0, 50, 0 ), parse( "PT1M-10S" ) );
    }

    private void assertParsesOne( String prefix, String suffix, int months, int days, int seconds )
    {
        assertEquals( duration( months, days, seconds, 0 ), parse( prefix + "1" + suffix ) );
        assertEquals( duration( months, days, seconds, 0 ), parse( "+" + prefix + "1" + suffix ) );
        assertEquals( duration( months, days, seconds, 0 ), parse( prefix + "+1" + suffix ) );
        assertEquals( duration( months, days, seconds, 0 ), parse( "+" + prefix + "+1" + suffix ) );

        assertEquals( duration( -months, -days, -seconds, 0 ), parse( "-" + prefix + "1" + suffix ) );
        assertEquals( duration( -months, -days, -seconds, 0 ), parse( prefix + "-1" + suffix ) );
        assertEquals( duration( -months, -days, -seconds, 0 ), parse( "+" + prefix + "-1" + suffix ) );
        assertEquals( duration( -months, -days, -seconds, 0 ), parse( "-" + prefix + "+1" + suffix ) );

        assertEquals( duration( months, days, seconds, 0 ), parse( "-" + prefix + "-1" + suffix ) );
    }

    @Test
    public void shouldParseDateBasedDuration()
    {
        assertEquals( duration( 14, 17, 45252, 123400000 ), parse( "P0001-02-17T12:34:12.1234" ) );
        assertEquals( duration( 14, 17, 45252, 123400000 ), parse( "P00010217T123412.1234" ) );
    }

    @Test
    public void shouldNotParseInvalidDurationStrings()
    {
        assertNotParsable( "" );
        assertNotParsable( "P" );
        assertNotParsable( "PT" );
        assertNotParsable( "PT.S" );
        assertNotParsable( "PT,S" );
        assertNotParsable( "PT.0S" );
        assertNotParsable( "PT,0S" );
        assertNotParsable( "PT0.S" );
        assertNotParsable( "PT0,S" );
        assertNotParsable( "PT1,-1S" );
        assertNotParsable( "PT1.-1S" );
        for ( String s : new String[] {"Y", "M", "W", "D"} )
        {
            assertNotParsable( "P-" + s );
            assertNotParsable( "P1" + s + "T" );
        }
        for ( String s : new String[] {"H", "M", "S"} )
        {
            assertNotParsable( "PT-" + s );
            assertNotParsable( "T1" + s );
        }
    }

    private void assertNotParsable( String text )
    {
        try
        {
            parse( text );
        }
        catch ( TemporalParseException e )
        {
            return;
        }
        fail( "should not be able to parse: " + text );
    }

    @Test
    public void shouldWriteDuration()
    {
        // given
        for ( DurationValue duration : new DurationValue[] {
                duration( 0, 0, 0, 0 ),
                duration( 1, 0, 0, 0 ),
                duration( 0, 1, 0, 0 ),
                duration( 0, 0, 1, 0 ),
                duration( 0, 0, 0, 1 ),
        } )
        {
            List<DurationValue> values = new ArrayList<>( 1 );
            ValueWriter<RuntimeException> writer = new ThrowingValueWriter.AssertOnly()
            {
                @Override
                public void writeDuration( long months, long days, long seconds, int nanos )
                {
                    values.add( duration( months, days, seconds, nanos ) );
                }
            };

            // when
            duration.writeTo( writer );

            // then
            assertEquals( singletonList( duration ), values );
        }
    }

    @Test
    public void shouldAddToLocalDate()
    {
        assertEquals( "seconds", LocalDate.of( 2017, 12, 5 ), LocalDate.of( 2017, 12, 4 ).plus( parse( "PT24H" ) ) );
        assertEquals( "seconds", LocalDate.of( 2017, 12, 3 ), LocalDate.of( 2017, 12, 4 ).minus( parse( "PT24H" ) ) );
        assertEquals( "seconds", LocalDate.of( 2017, 12, 4 ), LocalDate.of( 2017, 12, 4 ).plus( parse( "PT24H-1S" ) ) );
        assertEquals(
                "seconds",
                LocalDate.of( 2017, 12, 4 ),
                LocalDate.of( 2017, 12, 4 ).minus( parse( "PT24H-1S" ) ) );
        assertEquals( "days", LocalDate.of( 2017, 12, 5 ), LocalDate.of( 2017, 12, 4 ).plus( parse( "P1D" ) ) );
        assertEquals( "days", LocalDate.of( 2017, 12, 3 ), LocalDate.of( 2017, 12, 4 ).minus( parse( "P1D" ) ) );
    }

    @Test
    public void shouldHaveSensibleHashCode()
    {
        assertEquals( 0, duration( 0, 0, 0, 0 ).computeHash() );

        assertNotEquals(
                duration( 0, 0, 0, 1 ).computeHash(),
                duration( 0, 0, 0, 2 ).computeHash() );
        assertNotEquals(
                duration( 0, 0, 0, 1 ).computeHash(),
                duration( 0, 0, 1, 0 ).computeHash() );
        assertNotEquals(
                duration( 0, 0, 0, 1 ).computeHash(),
                duration( 0, 1, 0, 0 ).computeHash() );
        assertNotEquals(
                duration( 0, 0, 0, 1 ).computeHash(),
                duration( 1, 0, 0, 0 ).computeHash() );

        assertNotEquals(
                duration( 0, 0, 1, 0 ).computeHash(),
                duration( 0, 0, 2, 0 ).computeHash() );
        assertNotEquals(
                duration( 0, 0, 1, 0 ).computeHash(),
                duration( 0, 0, 0, 1 ).computeHash() );
        assertNotEquals(
                duration( 0, 0, 1, 0 ).computeHash(),
                duration( 0, 1, 0, 0 ).computeHash() );
        assertNotEquals(
                duration( 0, 0, 1, 0 ).computeHash(),
                duration( 1, 0, 0, 0 ).computeHash() );

        assertNotEquals(
                duration( 0, 1, 0, 0 ).computeHash(),
                duration( 0, 2, 0, 0 ).computeHash() );
        assertNotEquals(
                duration( 0, 1, 0, 0 ).computeHash(),
                duration( 0, 0, 0, 1 ).computeHash() );
        assertNotEquals(
                duration( 0, 1, 0, 0 ).computeHash(),
                duration( 0, 0, 1, 0 ).computeHash() );
        assertNotEquals(
                duration( 0, 1, 0, 0 ).computeHash(),
                duration( 1, 0, 0, 0 ).computeHash() );

        assertNotEquals(
                duration( 1, 0, 0, 0 ).computeHash(),
                duration( 2, 0, 0, 0 ).computeHash() );
        assertNotEquals(
                duration( 1, 0, 0, 0 ).computeHash(),
                duration( 0, 0, 0, 1 ).computeHash() );
        assertNotEquals(
                duration( 1, 0, 0, 0 ).computeHash(),
                duration( 0, 0, 1, 0 ).computeHash() );
        assertNotEquals(
                duration( 1, 0, 0, 0 ).computeHash(),
                duration( 0, 1, 0, 0 ).computeHash() );
    }

    @Test
    public void shouldMultiplyDurationByInteger()
    {
        assertEquals( duration( 2, 0, 0, 0 ), duration( 1, 0, 0, 0 ).mul( longValue( 2 ) ) );
        assertEquals( duration( 0, 2, 0, 0 ), duration( 0, 1, 0, 0 ).mul( longValue( 2 ) ) );
        assertEquals( duration( 0, 0, 2, 0 ), duration( 0, 0, 1, 0 ).mul( longValue( 2 ) ) );
        assertEquals( duration( 0, 0, 0, 2 ), duration( 0, 0, 0, 1 ).mul( longValue( 2 ) ) );

        assertEquals( duration( 0, 40, 0, 0 ), duration( 0, 20, 0, 0 ).mul( longValue( 2 ) ) );
        assertEquals(
                duration( 0, 0, 100_000, 0 ),
                duration( 0, 0, 50_000, 0 ).mul( longValue( 2 ) ) );
        assertEquals(
                duration( 0, 0, 1, 0 ),
                duration( 0, 0, 0, 500_000_000 ).mul( longValue( 2 ) ) );
    }

    @Test
    public void shouldMultiplyDurationByFloat()
    {
        assertEquals(
                duration( 0, 0, 0, 500_000_000 ),
                duration( 0, 0, 1, 0 ).mul( doubleValue( 0.5 ) ) );
        assertEquals( duration( 0, 0, 43200, 0 ), duration( 0, 1, 0, 0 ).mul( doubleValue( 0.5 ) ) );
        assertEquals( duration( 0, 15, 18873, 0 ), duration( 1, 0, 0, 0 ).mul( doubleValue( 0.5 ) ) );
    }

    @Test
    public void shouldDivideDuration()
    {
        assertEquals(
                duration( 0, 0, 0, 500_000_000 ),
                duration( 0, 0, 1, 0 ).div( longValue( 2 ) ) );
        assertEquals( duration( 0, 0, 43200, 0 ), duration( 0, 1, 0, 0 ).div( longValue( 2 ) ) );
        assertEquals( duration( 0, 15, 18873, 0 ), duration( 1, 0, 0, 0 ).div( longValue( 2 ) ) );
    }

    @Test
    public void shouldComputeDurationBetweenDates()
    {
        assertEquals( duration( 22, 23, 0, 0 ), durationBetween( date( 2016, 1, 27 ), date( 2017, 12, 20 ) ) );
        assertEquals( duration( 0, 693, 0, 0 ), between(DAYS, date( 2016, 1, 27 ), date( 2017, 12, 20 ) ) );
        assertEquals( duration( 22, 0, 0, 0 ), between(MONTHS, date( 2016, 1, 27 ), date( 2017, 12, 20 ) ) );
    }

    @Test
    public void shouldComputeDurationBetweenLocalTimes()
    {
        assertEquals( duration( 0, 0, 10623, 0 ), durationBetween(
                localTime( 11, 30, 52, 0 ), localTime( 14, 27, 55, 0 ) ) );
        assertEquals( duration( 0, 0, 10623, 0 ), between(
                SECONDS, localTime( 11, 30, 52, 0 ), localTime( 14, 27, 55, 0 ) ) );
    }

    @Test
    public void shouldComputeDurationBetweenTimes()
    {
        assertEquals( duration( 0, 0, 140223, 0 ), durationBetween(
                time( 11, 30, 52, 0, ofHours( 18 ) ), time( 14, 27, 55, 0, ofHours( -18 ) ) ) );
        assertEquals( duration( 0, 0, 10623, 0 ), between(
                SECONDS, time( 11, 30, 52, 0, UTC ), time( 14, 27, 55, 0, UTC ) ) );

        assertEquals( duration( 0, 0, 10623, 0 ), durationBetween(
                time( 11, 30, 52, 0, UTC ), localTime( 14, 27, 55, 0 ) ) );
        assertEquals( duration( 0, 0, 10623, 0 ), durationBetween(
                time( 11, 30, 52, 0, ofHours( 17 ) ), localTime( 14, 27, 55, 0 ) ) );
        assertEquals( duration( 0, 0, -10623, 0 ), durationBetween(
                localTime( 14, 27, 55, 0 ), time( 11, 30, 52, 0, UTC ) ) );
        assertEquals( duration( 0, 0, -10623, 0 ), durationBetween(
                localTime( 14, 27, 55, 0 ), time( 11, 30, 52, 0, ofHours( 17 ) ) ) );
    }

    @Test
    public void shouldComputeDurationBetweenDateAndTime()
    {
        assertEquals( parse( "PT14H32M11S" ), durationBetween( date( 2017, 12, 21 ), localTime( 14, 32, 11, 0 ) ) );
        assertEquals( parse( "-PT14H32M11S" ), durationBetween( localTime( 14, 32, 11, 0 ), date( 2017, 12, 21 ) ) );
        assertEquals( parse( "PT14H32M11S" ), durationBetween( date( 2017, 12, 21 ), time( 14, 32, 11, 0, UTC ) ) );
        assertEquals( parse( "-PT14H32M11S" ), durationBetween( time( 14, 32, 11, 0, UTC ), date( 2017, 12, 21 ) ) );
        assertEquals( parse( "PT14H32M11S" ), durationBetween(
                date( 2017, 12, 21 ), time( 14, 32, 11, 0, ofHours( -12 ) ) ) );
        assertEquals( parse( "-PT14H32M11S" ), durationBetween(
                time( 14, 32, 11, 0, ofHours( -12 ) ), date( 2017, 12, 21 ) ) );
    }

    @Test
    public void shouldComputeDurationBetweenDateTimeAndTime()
    {
        assertEquals( parse( "PT8H-20M" ), durationBetween(
                datetime( date( 2017, 12, 21 ), time( 6, 52, 11, 0, UTC ) ), localTime( 14, 32, 11, 0 ) ) );
        assertEquals( parse( "PT-8H+20M" ), durationBetween(
                localTime( 14, 32, 11, 0 ), datetime( date( 2017, 12, 21 ), time( 6, 52, 11, 0, UTC ) ) ) );

        assertEquals( parse( "-PT14H32M11S" ), durationBetween( localTime( 14, 32, 11, 0 ), date( 2017, 12, 21 ) ) );
        assertEquals( parse( "PT14H32M11S" ), durationBetween( date( 2017, 12, 21 ), time( 14, 32, 11, 0, UTC ) ) );
        assertEquals( parse( "-PT14H32M11S" ), durationBetween( time( 14, 32, 11, 0, UTC ), date( 2017, 12, 21 ) ) );
        assertEquals( parse( "PT14H32M11S" ), durationBetween(
                date( 2017, 12, 21 ), time( 14, 32, 11, 0, ofHours( -12 ) ) ) );
        assertEquals( parse( "-PT14H32M11S" ), durationBetween(
                time( 14, 32, 11, 0, ofHours( -12 ) ), date( 2017, 12, 21 ) ) );
    }

    @Test
    public void shouldComputeDurationBetweenDateTimeAndDateTime()
    {
        assertEquals( parse( "PT1H" ), durationBetween(
                datetime( date( 2017, 12, 21 ), time( 6, 52, 11, 0, UTC ) ),
                datetime( date( 2017, 12, 21 ), time( 7, 52, 11, 0, UTC ) ) ) );
        assertEquals( parse( "P1D" ), durationBetween(
                datetime( date( 2017, 12, 21 ), time( 6, 52, 11, 0, UTC ) ),
                datetime( date( 2017, 12, 22 ), time( 6, 52, 11, 0, UTC ) ) ) );
        assertEquals( parse( "P1DT1H" ), durationBetween(
                datetime( date( 2017, 12, 21 ), time( 6, 52, 11, 0, UTC ) ),
                datetime( date( 2017, 12, 22 ), time( 7, 52, 11, 0, UTC ) ) ) );
    }

    @Test
    public void shouldGetSameInstantWhenAddingDurationBetweenToInstant()
    {
        // given
        @SuppressWarnings( "unchecked" )
        Pair<Temporal,Temporal>[] input = new Pair[] {
                pair( // change from CET to CEST - second time of day after first
                        datetime( date( 2017, 3, 20 ), localTime( 13, 37, 0, 0 ), ZoneId.of( "Europe/Stockholm" ) ),
                        datetime( date( 2017, 3, 26 ), localTime( 19, 40, 0, 0 ), ZoneId.of( "Europe/Stockholm" ) ) ),
                pair( // change from CET to CEST - second time of day before first
                        datetime( date( 2017, 3, 20 ), localTime( 13, 37, 0, 0 ), ZoneId.of( "Europe/Stockholm" ) ),
                        datetime( date( 2017, 3, 26 ), localTime( 11, 40, 0, 0 ), ZoneId.of( "Europe/Stockholm" ) ) ),
                pair( // change from CEST to CET - second time of day after first
                        datetime( date( 2017, 10, 20 ), localTime( 13, 37, 0, 0 ), ZoneId.of( "Europe/Stockholm" ) ),
                        datetime( date( 2017, 10, 29 ), localTime( 19, 40, 0, 0 ), ZoneId.of( "Europe/Stockholm" ) ) ),
                pair( // change from CEST to CET - second time of day before first
                        datetime( date( 2017, 10, 20 ), localTime( 13, 37, 0, 0 ), ZoneId.of( "Europe/Stockholm" ) ),
                        datetime( date( 2017, 10, 29 ), localTime( 11, 40, 0, 0 ), ZoneId.of( "Europe/Stockholm" ) ) ),
        };
        for ( Pair<Temporal,Temporal> pair : input )
        {
            Temporal a = pair.first(), b = pair.other();

            // when
            DurationValue diffAB = durationBetween( a, b );
            DurationValue diffBA = durationBetween( b, a );
            DurationValue diffABs = between( SECONDS, a, b );
            DurationValue diffBAs = between( SECONDS, b, a );

            // then
            assertEquals( diffAB.prettyPrint(), b, a.plus( diffAB ) );
            assertEquals( diffBA.prettyPrint(), a, b.plus( diffBA ) );
            assertEquals( diffABs.prettyPrint(), b, a.plus( diffABs ) );
            assertEquals( diffBAs.prettyPrint(), a, b.plus( diffBAs ) );
        }
    }

    @Test
    public void shouldEqualItself()
    {
        assertEqual( duration( 40, 3, 13, 37 ), duration( 40, 3, 13, 37 ) );
        assertEqual( duration( 40, 3, 14, 37 ), duration( 40, 3, 13, 1_000_000_037 ) );
    }

    @Test
    public void shouldNotEqualOther()
    {
        assertNotEqual( duration( 40, 3, 13, 37 ), duration( 40, 3, 14, 37 ) );

        // average nbr of seconds on a month doesn't imply equality
        assertNotEqual( duration( 1, 0, 0, 0 ), duration( 0, 0, 2_629_800, 0 ) );

        // not the same due to leap seconds
        assertNotEqual( duration( 0, 1, 0, 0 ), duration( 0, 0, 60 * 60 * 24, 0 ) );

        // average nbr of days in 400 years doesn't imply equality
        assertNotEqual( duration( 400 * 12, 0, 0, 0 ), duration( 0, 146_097, 0, 0 ) );
    }

    @Test
    public void shouldNotThrowWhenInsideOverflowLimit()
    {
        // when
        duration(0, 0, Long.MAX_VALUE, 999_999_999 );

        // then should not throw
    }

    @Test
    public void shouldNotThrowWhenInsideNegativeOverflowLimit()
    {
        // when
        duration(0, 0, Long.MIN_VALUE, -999_999_999 );

        // then should not throw
    }

    @Test
    public void shouldThrowOnOverflowOnNanos()
    {
        // when
        int nanos = 1_000_000_000;
        long seconds = Long.MAX_VALUE;
        assertConstructorThrows( 0, 0, seconds, nanos );
    }

    @Test
    public void shouldThrowOnNegativeOverflowOnNanos()
    {
        // when
        int nanos = -1_000_000_000;
        long seconds = Long.MIN_VALUE;
        assertConstructorThrows( 0, 0, seconds, nanos );
    }

    @Test
    public void shouldThrowOnOverflowOnDays()
    {
        // when
        long days = Long.MAX_VALUE / TemporalUtil.SECONDS_PER_DAY;
        long seconds = Long.MAX_VALUE - days * TemporalUtil.SECONDS_PER_DAY;
        assertConstructorThrows( 0, days, seconds + 1, 0 );
    }

    @Test
    public void shouldThrowOnNegativeOverflowOnDays()
    {
        // when
        long days = Long.MIN_VALUE / TemporalUtil.SECONDS_PER_DAY;
        long seconds = Long.MIN_VALUE - days * TemporalUtil.SECONDS_PER_DAY;
        assertConstructorThrows( 0, days, seconds - 1, 0 );
    }

    @Test
    public void shouldThrowOnOverflowOnMonths()
    {
        // when
        long months = Long.MAX_VALUE / TemporalUtil.AVG_SECONDS_PER_MONTH;
        long seconds = Long.MAX_VALUE - months * TemporalUtil.AVG_SECONDS_PER_MONTH;
        assertConstructorThrows( months, 0, seconds + 1, 0 );
    }

    @Test
    public void shouldThrowOnNegativeOverflowOnMonths()
    {
        // when
        long months = Long.MIN_VALUE / TemporalUtil.AVG_SECONDS_PER_MONTH;
        long seconds = Long.MIN_VALUE - months * TemporalUtil.AVG_SECONDS_PER_MONTH;
        assertConstructorThrows( months, 0, seconds - 1, 0 );
    }

    private void assertConstructorThrows( long months, long days, long seconds, int nanos )
    {
        try
        {
            DurationValue duration = duration( months, days, seconds, nanos );
            fail( "Should have failed" );
        }
        catch ( InvalidValuesArgumentException e )
        {
            assertThat( e.getMessage(), Matchers.allOf(
                    Matchers.containsString( "Invalid value for duration" ),
                    Matchers.containsString( "months=" + months ),
                    Matchers.containsString( "days=" + days ),
                    Matchers.containsString( "seconds=" + seconds ),
                    Matchers.containsString( "nanos=" + nanos )
            ) );
        }
    }
}
