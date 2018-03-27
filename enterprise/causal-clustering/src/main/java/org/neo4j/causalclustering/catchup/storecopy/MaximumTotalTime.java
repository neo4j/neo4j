/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import org.neo4j.time.Clocks;

import static java.lang.String.format;

public class MaximumTotalTime implements TerminationCondition
{
    private final long endTime;
    private final Clock clock;
    private long time;
    private TimeUnit timeUnit;

    public MaximumTotalTime( long time, TimeUnit timeUnit )
    {
        this( time, timeUnit, Clocks.systemClock() );
    }

    MaximumTotalTime( long time, TimeUnit timeUnit, Clock clock )
    {
        this.endTime = clock.millis() + timeUnit.toMillis( time );
        this.clock = clock;
        this.time = time;
        this.timeUnit = timeUnit;
    }

    @Override
    public void assertContinue() throws StoreCopyFailedException
    {
        if ( clock.millis() > endTime )
        {
            throw new StoreCopyFailedException( format( "Maximum time passed %d %s. Not allowed to continue", time, timeUnit ) );
        }
    }
}
