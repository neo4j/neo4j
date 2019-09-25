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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * This class can be used to track elapsed time.
 *
 * @implNote It's using the highest resolution monotonic time source available. See {@link System#nanoTime()}.
 */
public class Stopwatch
{
    private final long startTimeNano;
    private final Ticker ticker;

    /**
     * Creates a default stop watch that will use {@link System#nanoTime()} as a source.
     */
    public static Stopwatch start()
    {
        return new Stopwatch( System::nanoTime );
    }

    Stopwatch( Ticker ticker )
    {
        this.ticker = ticker;
        startTimeNano = ticker.get();
    }

    /**
     * Returned the elapsed time from the moment the stopwatch was created.
     * @return the elapsed time since the stopwatch was started.
     */
    public Duration elapsed()
    {
        return Duration.ofNanos( elapsed( NANOSECONDS ) );
    }

    /**
     * Returned the elapsed time from the moment the stopwatch was created.
     * @param unit the desired time unit.
     * @return the elapsed time since the stopwatch was started, in the provided unit.
     */
    public long elapsed( TimeUnit unit )
    {
        return unit.convert( ticker.get() - startTimeNano, NANOSECONDS );
    }

    /**
     * Test if a given duration have passed since the stopwatch started.
     *
     * @param timeout the timeout duration.
     * @return {@code true} if the timeout duration is greater or equals to the elapsed time.
     */
    public boolean hasTimedOut( Duration timeout )
    {
        return elapsed( NANOSECONDS ) >= timeout.toNanos();
    }

    /**
     * Test if a given duration have passed since the stopwatch started.
     *
     * @param duration the duration.
     * @param unit time unit the duration is specified in.
     * @return {@code true} if the timeout duration is greater or equals to the elapsed time.
     */
    public boolean hasTimedOut( long duration, TimeUnit unit )
    {
        return elapsed( NANOSECONDS ) >= unit.toNanos( duration );
    }

    /**
     * Represents a source of time.
     */
    interface Ticker
    {
        /**
         * @return current time in nanoseconds.
         */
        long get();
    }
}
