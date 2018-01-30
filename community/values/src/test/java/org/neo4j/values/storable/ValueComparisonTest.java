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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.pointValue;

public class ValueComparisonTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private Comparator<Value> comparator = Values.COMPARATOR;

    private Object[] objs = new Object[]{
            // ARRAYS
            new PointValue[] {},
            new PointValue[] { pointValue( WGS84, -1.0, -1.0 ) },
            new PointValue[] { pointValue( WGS84, -1.0, -1.0 ), pointValue( WGS84, -1.0, -1.0 ) },
            new PointValue[] { pointValue( WGS84, -1.0, -1.0 ), pointValue( Cartesian, 1.0, 2.0 ) },
            new String[]{},
            new String[]{"a"},
            new String[]{"a", "aa"},
            new char[]{'a', 'b'},
            new String[]{"aa"},
            new boolean[]{},
            new boolean[]{false},
            new boolean[]{false, true},
            new boolean[]{true},
            new int[]{},
            new double[]{-1.0},
            new long[]{-1, 44},
            new float[]{2},
            new short[]{2, 3},
            new byte[]{3, -99, -99},

            // POINTS
            pointValue( WGS84, -1.0, -1.0 ),
            pointValue( WGS84, 1.0, 1.0 ),
            pointValue( WGS84, 1.0, 2.0 ),
            pointValue( WGS84, 2.0, 1.0 ),
            pointValue( Cartesian, -1.0, -1.0 ),
            pointValue( Cartesian, 1.0, 1.0 ),
            pointValue( Cartesian, 1.0, 2.0 ),
            pointValue( Cartesian, 2.0, 1.0 ),

            // DateTime and the likes
            datetime(2018, 2, 2, 0, 0, 0, 0, "+00:00"),
            datetime(2018, 2, 2, 1, 30, 0, 0, "+01:00"),
            datetime(2018, 2, 2, 1, 0, 0, 0, "+00:00"),
            localDateTime(2018, 2, 2, 0, 0, 0, 0),
            localDateTime(2018, 2, 2, 0, 0, 0, 1),
            localDateTime(2018, 2, 2, 0, 0, 1, 0),
            localDateTime(2018, 2, 2, 0, 1, 0, 0),
            localDateTime(2018, 2, 2, 1, 0, 0, 0),
            date(2018, 2, 1),
            date(2018, 2, 2),
            time(12, 0, 0, 0, "+00:00"),
            time(13, 30, 0, 0, "+01:00"),
            time(13, 0, 0, 0, "+00:00"),
            localTime(0, 0, 0, 1),
            localTime(0, 0, 0, 3),

            // Duration
            duration(0, 0, 0, 0),
            duration(0, 0, 0, 1),
            duration(0, 0, 1, 0),
            duration(0, 0, 60, 0),
            duration(0, 0, 60 * 60, 0),
            duration(0, 0, 60 * 60 * 24, 0),
            duration(0, 1, 0, 0),
            duration(0, 0, 60 * 60 * 24, 1),
            duration(0, 1, 60 * 60 * 24, 0),
            duration(0, 2, 0, 0),
            duration(0, 1, 60 * 60 * 24, 1),
            duration(0, 10, 60 * 60 * 24, 2_000_000_500),
            duration(0, 11, 2, 500), // Same duration as above, but higher days value
            duration(0, 10, 60 * 60 * 24, 2_000_000_501),
            duration(0, 30, 0, 0),
            duration(1, 0, 0, 0),
            duration(0, 31, 0, 0),
            duration(9999999999L * 12, 0, 0, 0),
            duration(9999999999L * 12, 0, 0, 1),
            duration(9999999999L * 12, 0, 0, 2),

            // STRING
            "",
            Character.MIN_VALUE,
            " ",
            "20",
            "x",
            "y",
            Character.MIN_HIGH_SURROGATE,
            Character.MAX_HIGH_SURROGATE,
            Character.MIN_LOW_SURROGATE,
            Character.MAX_LOW_SURROGATE,
            Character.MAX_VALUE,

            // BOOLEAN
            false,
            true,

            // NUMBER
            Double.NEGATIVE_INFINITY,
            -Double.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MIN_VALUE + 1,
            Integer.MIN_VALUE,
            Short.MIN_VALUE,
            Byte.MIN_VALUE,
            0,
            Double.MIN_VALUE,
            Double.MIN_NORMAL,
            Float.MIN_VALUE,
            Float.MIN_NORMAL,
            1L,
            1.1d,
            1.2f,
            Math.E,
            Math.PI,
            (byte) 10,
            (short) 20,
            Byte.MAX_VALUE,
            Short.MAX_VALUE,
            Integer.MAX_VALUE,
            9007199254740992D,
            9007199254740993L,
            Long.MAX_VALUE,
            Float.MAX_VALUE,
            Double.MAX_VALUE,
            Double.POSITIVE_INFINITY,
            Double.NaN,

            // OTHER
            null
    };

    @Test
    public void shouldOrderValuesCorrectly()
    {
        List<Value> values = Arrays.stream( objs ).map( Values::of ).collect( Collectors.toList() );

        for ( int i = 0; i < values.size(); i++ )
        {
            for ( int j = 0; j < values.size(); j++ )
            {
                Value left = values.get( i );
                Value right = values.get( j );

                int cmpPos = sign( i - j );
                int cmpVal = sign( compare( comparator, left, right ) );

                if ( cmpPos != cmpVal )
                {
                    throw new AssertionError( format(
                            "Comparing %s against %s does not agree with their positions in the sorted list (%d and " +
                                    "%d)",
                            left, right, i, j
                    ) );
                }
            }
        }
    }

    private <T> int compare( Comparator<T> comparator, T left, T right )
    {
        int cmp1 = comparator.compare( left, right );
        int cmp2 = comparator.compare( right, left );
        if ( sign( cmp1 ) != -sign( cmp2 ) )
        {
            throw new AssertionError( format( "%s is not symmetric on %s and %s", comparator, left, right ) );
        }
        return cmp1;
    }

    private int sign( int value )
    {
        return value == 0 ? 0 : (value < 0 ? -1 : +1);
    }
}
