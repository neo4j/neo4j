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
package org.neo4j.kernel.impl.store.kvstore;

import java.util.concurrent.TimeUnit;

import org.neo4j.time.SystemNanoClock;

public class RotationTimerFactory
{
    private SystemNanoClock clock;
    private long timeoutMillis;

    public RotationTimerFactory( SystemNanoClock clock, long timeoutMillis )
    {
        this.clock = clock;
        this.timeoutMillis = timeoutMillis;
    }

    public RotationTimer createTimer()
    {
        long startTimeNanos = clock.nanos();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos( timeoutMillis );
        return new RotationTimer( startTimeNanos, startTimeNanos + timeoutNanos );
    }

    class RotationTimer
    {
        private long startTimeNanos;
        private long deadlineNanos;

        RotationTimer( long startTimeNanos, long deadlineNanos )
        {
            this.startTimeNanos = startTimeNanos;
            this.deadlineNanos = deadlineNanos;
        }

        public boolean isTimedOut()
        {
            return clock.nanos() > deadlineNanos;
        }

        public long getElapsedTimeMillis()
        {
            long elapsedNanos = clock.nanos() - startTimeNanos;
            return TimeUnit.NANOSECONDS.toMillis( elapsedNanos );
        }

    }
}
