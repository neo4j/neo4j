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
package org.neo4j.io.pagecache.stress;

import java.util.concurrent.TimeUnit;

import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;

import static java.lang.System.currentTimeMillis;

public class Conditions
{
    public static Condition numberOfEvictions( final PageCacheMonitor monitor,
                                               final long desiredNumberOfEvictions )
    {
        return new Condition()
        {
            @Override
            public boolean fulfilled()
            {
                return monitor.countEvictions() > desiredNumberOfEvictions;
            }
        };
    }

    public static Condition timePeriod( final int duration, final TimeUnit timeUnit )
    {
        final long endTimeInMilliseconds = currentTimeMillis() + timeUnit.toMillis( duration );

        return new Condition()
        {
            @Override
            public boolean fulfilled()
            {
                return currentTimeMillis() > endTimeInMilliseconds;
            }
        };
    }
}
