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
package org.neo4j.kernel.api.index;

import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.function.Function;

import org.neo4j.graphdb.spatial.Point;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.test.Race;
import org.neo4j.test.rule.concurrent.ThreadingRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ArrayEncoderTest
{
    @Rule
    public final ThreadingRule threads = new ThreadingRule();

    private static final Character[] base64chars = new Character[]{
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U',
            'V', 'W', 'X', 'Y', 'Z',
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
            'v', 'w', 'x', 'y', 'z',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '+', '/'};
    private static final char ARRAY_ENTRY_SEPARATOR = '|';
    private static final char PADDING = '=';

    @Test
    public void encodingShouldContainOnlyBase64EncodingChars()
    {
        String[] array = {
                "This string is long enough for BASE64 to emit a line break, making the encoding platform dependant.",
                "Something else to trigger padding."
        };
        String encoded = ArrayEncoder.encode( Values.of( array ) );

        int separators = 0;
        boolean padding = false;
        for ( int i = 0; i < encoded.length(); i++ )
        {
            char character = encoded.charAt( i );
            if ( character == ARRAY_ENTRY_SEPARATOR )
            {
                padding = false;
                separators++;
            }
            else if ( padding )
            {
                assertEquals( PADDING, character );
            }
            else if ( character == PADDING )
            {
                padding = true;
            }
            else
            {
                assertTrue( "Char " + character + " at position " + i + " is not a valid Base64 encoded char",
                        ArrayUtil.contains( base64chars, character ) );
            }
        }
        assertEquals( array.length, separators );
    }

    @Test
    public void shouldEncodeArrays()
    {
        assertEncoding( "D1.0|2.0|3.0|", new int[]{1, 2, 3} );
        assertEncoding( "Ztrue|false|", new boolean[]{true, false} );
        assertEncoding( "LYWxp|YXJl|eW91|b2s=|", new String[]{"ali", "are", "you", "ok"} );
        assertEncoding( "", new String[]{} );
        assertEncoding( "P1:4326:1.234;2.567|1:4326:2.345;5.678|2:9157:3.0;4.0;5.0|",
                new Point[]{Values.pointValue( CoordinateReferenceSystem.WGS84, 1.234, 2.567 ),
                        Values.pointValue( CoordinateReferenceSystem.WGS84, 2.345, 5.678 ),
                        Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 3, 4, 5 )} );
        assertEncoding( "T1991-03-05|1992-04-06|", new LocalDate[]{DateValue.date( 1991, 3, 5 ).asObjectCopy(), DateValue.date( 1992, 4, 6 ).asObjectCopy()} );
        assertEncoding( "T12:45:13.000008676|05:04:50.000000076|",
                new LocalTime[]{LocalTimeValue.localTime( 12, 45, 13, 8676 ).asObjectCopy(), LocalTimeValue.localTime( 5, 4, 50, 76 ).asObjectCopy()} );
        assertEncoding( "T1991-03-05T12:45:13.000008676|1992-04-06T05:04:50.000000076|",
                new LocalDateTime[]{LocalDateTimeValue.localDateTime( 1991, 3, 5, 12, 45, 13, 8676 ).asObjectCopy(),
                        LocalDateTimeValue.localDateTime( 1992, 4, 6, 5, 4, 50, 76 ).asObjectCopy()} );
        assertEncoding( "T02:45:13.000008676Z|01:05:00.0000003+01:00|05:04:50.000000076+05:00|",
                new OffsetTime[]{TimeValue.time( 2, 45, 13, 8676, UTC ).asObjectCopy(),
                        TimeValue.time( OffsetTime.ofInstant( Instant.ofEpochSecond( 300, 300 ), ZoneId.of( "Europe/Stockholm" ) ) ).asObjectCopy(),
                        TimeValue.time( 5, 4, 50, 76, "+05:00" ).asObjectCopy()} );
        assertEncoding( "T1991-03-05T02:45:13.000008676Z|1991-03-05T02:45:13.000008676+01:00[Europe/Stockholm]|1992-04-06T05:04:50.000000076+05:00|",
                new ZonedDateTime[]{DateTimeValue.datetime( 1991, 3, 5, 2, 45, 13, 8676, UTC ).asObjectCopy(),
                        DateTimeValue.datetime( 1991, 3, 5, 2, 45, 13, 8676, ZoneId.of( "Europe/Stockholm" ) ).asObjectCopy(),
                        DateTimeValue.datetime( 1992, 4, 6, 5, 4, 50, 76, "+05:00" ).asObjectCopy()} );
        assertEncoding( "AP165Y11M3DT5.000000012S|P166Y4DT6.000000005S|",
                new TemporalAmount[]{DurationValue.duration( 1991, 3, 5, 12 ).asObjectCopy(), DurationValue.duration( 1992, 4, 6, 5 ).asObjectCopy()} );
    }

    @Test
    public void shouldEncodeProperlyWithMultipleThreadsRacing() throws Throwable
    {
        // given
        final String[] INPUT = {
                "These strings need to be longer than 57 bytes, because that is the line wrapping length of BASE64.",
                "This next line is also long. The number of strings in this array is the number of threads to use.",
                "Each thread will get a different string as input to encode, and ensure the result is always the same.",
                "Should the result of an encoding differ even once, the thread will yield a negative overall result.",
                "If any of the threads yields a negative result, the test will fail, since that should not happen.",
                "All threads are allowed to run together for a predetermined amount of time, to try to get contention.",
                "This predetermined time is the minimum runtime of the test, since the timer starts after all threads.",
                "The idea to use the input data as documentation for the test was just a cute thing I came up with.",
                "Since my imagination for coming up with test data is usually poor, I figured I'd do something useful.",
                "Hopefully this isn't just nonsensical drivel, and maybe, just maybe someone might actually read it."
            };

        raceEncode( INPUT, ArrayEncoder::encode );
    }

    private void raceEncode( String[] INPUT, Function<Value, String> encodeFunction ) throws Throwable
    {
        Race race = new Race();
        for ( String input : INPUT )
        {
            final Value inputValue = Values.of( new String[]{input} );
            race.addContestant( () ->
            {
                String first = encodeFunction.apply( inputValue );
                for ( int i = 0; i < 1000; i++ )
                {
                    String encoded = encodeFunction.apply( inputValue );
                    assertEquals( "Each attempt at encoding should yield the same result. Turns out that first one was '"
                            + first + "', yet another one was '" + encoded + "'", first, encoded );
                }
            } );
        }
        race.go();
    }

    private void assertEncoding( String expected, Object toEncode )
    {
        assertEquals( expected, ArrayEncoder.encode( Values.of( toEncode ) ) );
    }
}
