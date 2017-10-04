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
package org.neo4j.helpers;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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

    private TimeUtil()
    {
        throw new AssertionError(); // no instances
    }
}
