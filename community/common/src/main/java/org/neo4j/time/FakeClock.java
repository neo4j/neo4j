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
package org.neo4j.time;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

/**
 * A {@link java.time.Clock} that is manually controlled.
 */
public class FakeClock extends SystemNanoClock
{
    private volatile long nanoTime;

    public FakeClock()
    {
    }

    public FakeClock( long initialTime, TimeUnit unit )
    {
        forward( initialTime, unit );
    }

    @Override
    public ZoneId getZone()
    {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone( ZoneId zone )
    {
        return new WithZone( zone );
    }

    @Override
    public Instant instant()
    {
        return Instant.ofEpochMilli( TimeUnit.NANOSECONDS.toMillis( nanoTime ) );
    }

    @Override
    public long nanos()
    {
        return nanoTime;
    }

    @Override
    public long millis()
    {
        return TimeUnit.NANOSECONDS.toMillis( nanoTime );
    }

    public FakeClock forward( Duration delta )
    {
        return forward( delta.toNanos(), TimeUnit.NANOSECONDS );
    }

    public FakeClock forward( long delta, TimeUnit unit )
    {
        nanoTime += unit.toNanos( delta );
        return this;
    }

    private class WithZone extends Clock
    {
        private final ZoneId zone;

        WithZone( ZoneId zone )
        {
            this.zone = zone;
        }

        @Override
        public ZoneId getZone()
        {
            return zone;
        }

        @Override
        public Clock withZone( ZoneId zone )
        {
            return new WithZone( zone );
        }

        @Override
        public long millis()
        {
            return FakeClock.this.millis();
        }

        @Override
        public Instant instant()
        {
            return FakeClock.this.instant();
        }
    }
}
