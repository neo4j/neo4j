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
package org.neo4j.values.utils;

import org.junit.Test;

import java.time.Duration;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class TemporalUtilTest
{
    @Test
    public void shouldDoNothingForOffsetWithoutSeconds()
    {
        OffsetTime time = OffsetTime.of( 23, 30, 10, 0, ZoneOffset.ofHoursMinutes( -5, -30 ) );

        OffsetTime truncatedTime = TemporalUtil.truncateOffsetToMinutes( time );

        assertEquals( time, truncatedTime );
    }

    @Test
    public void shouldTruncateOffsetSeconds()
    {
        OffsetTime time = OffsetTime.of( 14, 55, 50, 0, ZoneOffset.ofHoursMinutesSeconds( 2, 15, 45 ) );

        OffsetTime truncatedTime = TemporalUtil.truncateOffsetToMinutes( time );

        assertEquals( OffsetTime.of( 14, 55, 5, 0, ZoneOffset.ofHoursMinutes( 2, 15 ) ), truncatedTime );
    }

    @Test
    public void shouldConvertNanosOfDayToUTCWhenOffsetIsZero()
    {
        int nanosOfDayLocal = 42;

        long nanosOfDayUTC = TemporalUtil.nanosOfDayToUTC( nanosOfDayLocal, 0 );

        assertEquals( nanosOfDayLocal, nanosOfDayUTC );
    }

    @Test
    public void shouldConvertNanosOfDayToUTC()
    {
        int nanosOfDayLocal = 42;
        Duration offsetDuration = Duration.ofMinutes( 35 );

        long nanosOfDayUTC = TemporalUtil.nanosOfDayToUTC( nanosOfDayLocal, (int) offsetDuration.getSeconds() );

        assertEquals( nanosOfDayLocal - offsetDuration.toNanos(), nanosOfDayUTC );
    }

    @Test
    public void shouldGetNanosOfDayUTC()
    {
        LocalTime localTime = LocalTime.of( 14, 19, 18, 123999 );
        ZoneOffset offset = ZoneOffset.ofHours( -12 );
        OffsetTime time = OffsetTime.of( localTime, offset );

        long nanosOfDayUTC = TemporalUtil.getNanosOfDayUTC( time );

        long expectedNanosOfDayUTC = Duration.ofSeconds( localTime.toSecondOfDay() )
                .minus( offset.getTotalSeconds(), SECONDS )
                .toNanos();

        assertEquals( expectedNanosOfDayUTC + localTime.getNano(), nanosOfDayUTC );
    }
}
