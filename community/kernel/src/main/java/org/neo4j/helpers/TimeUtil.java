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
package org.neo4j.helpers;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class TimeUtil
{
    public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;

    public static final String VALID_TIME_DESCRIPTION = "Valid units are: 'ms', 's', 'm' and 'h'; default unit is 's'";

    public static final Function<String,Long> parseTimeMillis = timeWithOrWithoutUnit ->
    {
        int unitIndex = -1;
        for ( int i = 0; i < timeWithOrWithoutUnit.length(); i++ )
        {
            char ch = timeWithOrWithoutUnit.charAt( i );
            if ( !Character.isDigit( ch ) )
            {
                unitIndex = i;
                break;
            }
        }
        if ( unitIndex == -1 )
        {
            return DEFAULT_TIME_UNIT.toMillis( Integer.parseInt( timeWithOrWithoutUnit ) );
        }

        String unit = timeWithOrWithoutUnit.substring( unitIndex ).toLowerCase();
        if ( unitIndex == 0 )
        {
            throw new IllegalArgumentException( "Missing numeric value" );
        }

        // We have digits
        int amount = Integer.parseInt( timeWithOrWithoutUnit.substring( 0, unitIndex ) );
        switch ( unit )
        {
        case "ms":
            return TimeUnit.MILLISECONDS.toMillis( amount );
        case "s":
            return TimeUnit.SECONDS.toMillis( amount );
        case "m":
            return TimeUnit.MINUTES.toMillis( amount );
        case "h":
            return TimeUnit.HOURS.toMillis( amount );
        default:
            throw new IllegalArgumentException( "Unrecognized unit '" + unit + "'. " + VALID_TIME_DESCRIPTION );
        }
    };

    public static String nanosToString( long nanos )
    {
        assert nanos >= 0;
        long nanoSeconds = nanos;
        StringBuilder timeString = new StringBuilder();

        long days = DAYS.convert( nanoSeconds, NANOSECONDS );
        if ( days > 0 )
        {
            nanoSeconds -= DAYS.toNanos( days );
            timeString.append( days ).append( 'd' );
        }
        long hours = HOURS.convert( nanoSeconds, NANOSECONDS );
        if ( hours > 0 )
        {
            nanoSeconds -= HOURS.toNanos( hours );
            timeString.append( hours ).append( 'h' );
        }
        long minutes = MINUTES.convert( nanoSeconds, NANOSECONDS );
        if ( minutes > 0 )
        {
            nanoSeconds -= MINUTES.toNanos( minutes );
            timeString.append( minutes ).append( 'm' );
        }
        long seconds = SECONDS.convert( nanoSeconds, NANOSECONDS );
        if ( seconds > 0 )
        {
            nanoSeconds -= SECONDS.toNanos( seconds );
            timeString.append( seconds ).append( 's' );
        }
        long milliseconds = MILLISECONDS.convert( nanoSeconds, NANOSECONDS );
        if ( milliseconds > 0 )
        {
            nanoSeconds -= MILLISECONDS.toNanos( milliseconds );
            timeString.append( milliseconds ).append( "ms" );
        }
        long microseconds = MICROSECONDS.convert( nanoSeconds, NANOSECONDS );
        if ( microseconds > 0 )
        {
            nanoSeconds -= MICROSECONDS.toNanos( microseconds );
            timeString.append( microseconds ).append( "μs" );
        }
        if ( nanoSeconds > 0 || timeString.length() == 0 )
        {
            timeString.append( nanoSeconds ).append( "ns" );
        }
        return timeString.toString();
    }

    private TimeUtil()
    {
        throw new AssertionError(); // no instances
    }
}
