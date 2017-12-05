/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.index;

import org.junit.Rule;
import org.junit.Test;

import java.util.function.Function;

import org.neo4j.graphdb.spatial.Point;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.test.Race;
import org.neo4j.test.rule.concurrent.ThreadingRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

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
    public void encodingShouldContainOnlyBase64EncodingChars() throws Exception
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
    public void shouldEncodeArrays() throws Exception
    {
        assertEncoding( "D1.0|2.0|3.0|", new int[]{1, 2, 3} );
        assertEncoding( "Ztrue|false|", new boolean[]{true, false} );
        assertEncoding( "LYWxp|YXJl|eW91|b2s=|", new String[]{"ali", "are", "you", "ok"} );
        assertEncoding( "", new String[]{} );
        PointValue a = Values.pointValue( CoordinateReferenceSystem.WGS84, 1.234, 2.567 );
        PointValue b = Values.pointValue( CoordinateReferenceSystem.WGS84, 2.345, 5.678 );
        PointValue c = Values.pointValue( CoordinateReferenceSystem.Cartesian, 3, 4, 5 );
        assertEncoding( "P1:4326:1.234;2.567|1:4326:2.345;5.678|2:7203:3.0;4.0;5.0|", new Point[]{a, b, c} );
    }

    @Test
    public void shouldEncodeProperlyWithMultipleThreadsRacing() throws Throwable
    {
        // given
        String[] INPUT = {
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
