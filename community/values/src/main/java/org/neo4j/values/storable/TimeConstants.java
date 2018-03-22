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

import java.time.OffsetTime;
import java.time.ZoneOffset;

import static java.time.temporal.ChronoUnit.DAYS;

@SuppressWarnings( "WeakerAccess" )
public class TimeConstants
{
    public static final long NANOS_PER_SECOND = 1_000_000_000L;
    public static final long SECONDS_PER_DAY = DAYS.getDuration().getSeconds();
    public static final long NANOS_PER_DAY = NANOS_PER_SECOND * SECONDS_PER_DAY;
    /** 30.4375 days = 30 days, 10 hours, 30 minutes */
    public static final double AVG_DAYS_PER_MONTH = 365.2425 / 12;
    public static final long AVG_SECONDS_PER_MONTH = 2_629_746;

    private TimeConstants()
    {
    }

    public static long asValidTime( long nanosPerDay )
    {
        if ( nanosPerDay < 0 )
        {
            return nanosPerDay + NANOS_PER_DAY;
        }
        if ( nanosPerDay >= NANOS_PER_DAY )
        {
            return nanosPerDay - NANOS_PER_DAY;
        }
        return nanosPerDay;
    }

    public static OffsetTime truncateOffsetToMinutes( OffsetTime value )
    {
        int offsetMinutes = value.getOffset().getTotalSeconds() / 60;
        ZoneOffset truncatedOffset = ZoneOffset.ofTotalSeconds( offsetMinutes * 60 );
        return value.withOffsetSameInstant( truncatedOffset );
    }
}
