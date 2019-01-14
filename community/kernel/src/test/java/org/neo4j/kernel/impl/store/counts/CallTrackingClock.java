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
package org.neo4j.kernel.impl.store.counts;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import org.neo4j.time.SystemNanoClock;

public class CallTrackingClock extends SystemNanoClock
{
    private final SystemNanoClock actual;
    private volatile int nanosCalls;

    public CallTrackingClock( SystemNanoClock actual )
    {
        this.actual = actual;
    }

    @Override
    public ZoneId getZone()
    {
        return actual.getZone();
    }

    @Override
    public Clock withZone( ZoneId zone )
    {
        return actual.withZone( zone );
    }

    @Override
    public Instant instant()
    {
        return actual.instant();
    }

    @Override
    public long millis()
    {
        return actual.millis();
    }

    @Override
    public long nanos()
    {
        try
        {
            return actual.nanos();
        }
        finally
        {
            nanosCalls++;
        }
    }

    public int callsToNanos()
    {
        return nanosCalls;
    }
}
