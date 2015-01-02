/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

public final class TimeUtil
{
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;

    public static final Function<String, Long> parseTimeMillis = new Function<String, Long>()
    {
        @Override
        public Long apply( String timeWithOrWithoutUnit )
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
            else
            {
                int amount = Integer.parseInt( timeWithOrWithoutUnit.substring( 0, unitIndex ) );
                String unit = timeWithOrWithoutUnit.substring( unitIndex ).toLowerCase();
                TimeUnit timeUnit = null;
                int multiplyFactor = 1;
                if ( unit.equals( "ms" ) )
                {
                    timeUnit = TimeUnit.MILLISECONDS;
                }
                else if ( unit.equals( "s" ) )
                {
                    timeUnit = TimeUnit.SECONDS;
                }
                else if ( unit.equals( "m" ) )
                {
                    // This is only for having to rely on 1.6
                    timeUnit = TimeUnit.SECONDS;
                    multiplyFactor = 60;
                }
                else
                {
                    throw new RuntimeException( "Unrecognized unit " + unit );
                }
                return timeUnit.toMillis( amount * multiplyFactor );
            }
        }
    };

    private TimeUtil()
    {
    }
}
