/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
