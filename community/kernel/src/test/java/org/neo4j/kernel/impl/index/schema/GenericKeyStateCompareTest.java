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
package org.neo4j.kernel.impl.index.schema;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GenericKeyStateCompareTest
{
    @Test
    void compareGenericKeyState()
    {
        List<Value> allValues = Arrays.asList(
                Values.of( "string1" ),
                Values.of( 42 ),
                Values.of( true ),
                Values.of( new char[]{'a', 'z'} ),
                Values.of( new String[]{"arrayString1", "arraysString2"} ),
                Values.of( new byte[]{(byte) 1, (byte) 12} ),
                Values.of( new short[]{314, 1337} ),
                Values.of( new int[]{3140, 13370} ),
                Values.of( new long[]{31400, 133700} ),
                Values.of( new boolean[]{false, true} ),
                DateValue.epochDate( 2 ),
                LocalTimeValue.localTime( 100000 ),
                TimeValue.time( 43_200_000_000_000L, ZoneOffset.UTC ), // Noon
                TimeValue.time( 43_201_000_000_000L, ZoneOffset.UTC ),
                TimeValue.time( 43_200_000_000_000L, ZoneOffset.of( "+01:00" ) ), // Noon in the next time-zone
                TimeValue.time( 46_800_000_000_000L, ZoneOffset.UTC ), // Same time UTC as prev time
                LocalDateTimeValue.localDateTime( 2018, 3, 1, 13, 50, 42, 1337 ),
                DateTimeValue.datetime( 2014, 3, 25, 12, 45, 13, 7474, "UTC" ),
                DateTimeValue.datetime( 2014, 3, 25, 12, 45, 13, 7474, "Europe/Stockholm" ),
                DateTimeValue.datetime( 2014, 3, 25, 12, 45, 13, 7474, "+05:00" ),
                DateTimeValue.datetime( 2015, 3, 25, 12, 45, 13, 7474, "+05:00" ),
                DateTimeValue.datetime( 2014, 4, 25, 12, 45, 13, 7474, "+05:00" ),
                DateTimeValue.datetime( 2014, 3, 26, 12, 45, 13, 7474, "+05:00" ),
                DateTimeValue.datetime( 2014, 3, 25, 13, 45, 13, 7474, "+05:00" ),
                DateTimeValue.datetime( 2014, 3, 25, 12, 46, 13, 7474, "+05:00" ),
                DateTimeValue.datetime( 2014, 3, 25, 12, 45, 14, 7474, "+05:00" ),
                DateTimeValue.datetime( 2014, 3, 25, 12, 45, 13, 7475, "+05:00" ),
                // only runnable it JVM supports East-Saskatchewan
                // DateTimeValue.datetime( 2001, 1, 25, 11, 11, 30, 0, "Canada/East-Saskatchewan" ),
                DateTimeValue.datetime( 2038, 1, 18, 9, 14, 7, 0, "-18:00" ),
                DateTimeValue.datetime( 10000, 100, ZoneOffset.ofTotalSeconds( 3 ) ),
                DateTimeValue.datetime( 10000, 101, ZoneOffset.ofTotalSeconds( -3 ) ),
                DurationValue.duration( 10, 20, 30, 40 ),
                DurationValue.duration( 11, 20, 30, 40 ),
                DurationValue.duration( 10, 21, 30, 40 ),
                DurationValue.duration( 10, 20, 31, 40 ),
                DurationValue.duration( 10, 20, 30, 41 ),
                Values.dateTimeArray( new ZonedDateTime[]{
                        ZonedDateTime.of( 2018, 10, 9, 8, 7, 6, 5, ZoneId.of( "UTC" ) ),
                        ZonedDateTime.of( 2017, 9, 8, 7, 6, 5, 4, ZoneId.of( "UTC" ) )
                } ),
                Values.localDateTimeArray( new LocalDateTime[]{
                        LocalDateTime.of( 2018, 10, 9, 8, 7, 6, 5 ),
                        LocalDateTime.of( 2018, 10, 9, 8, 7, 6, 5 )
                } ),
                Values.timeArray( new OffsetTime[]{
                        OffsetTime.of( 20, 8, 7, 6, ZoneOffset.UTC ),
                        OffsetTime.of( 20, 8, 7, 6, ZoneOffset.UTC )
                } ),
                Values.dateArray( new LocalDate[]{
                        LocalDate.of( 2018, 12, 28 ),
                        LocalDate.of( 2018, 12, 28 )
                } ),
                Values.localTimeArray( new LocalTime[]{
                        LocalTime.of( 9, 28 ),
                        LocalTime.of( 9, 28 )
                } ),
                Values.durationArray( new DurationValue[]{
                        DurationValue.duration( 12, 10, 10, 10 ),
                        DurationValue.duration( 12, 10, 10, 10 )
                } )
                // PointValue/PointArray comparison can't be compared to that of the GenericKeyState for those points
                // since the index compares in a way which is designed to be queryable, so they have different sorting.
        );
        allValues.sort( Values.COMPARATOR );

        List<GenericKey> states = new ArrayList<>();
        for ( Value value : allValues )
        {
            GenericKey state = new GenericKey( null );
            state.writeValue( value, NativeIndexKey.Inclusion.NEUTRAL );
            states.add( state );
        }
        Collections.shuffle( states );
        states.sort( GenericKey::compareValueTo );
        List<Value> sortedStatesAsValues = states.stream().map( GenericKey::asValue ).collect( Collectors.toList() );
        assertEquals( allValues, sortedStatesAsValues );
    }
}
