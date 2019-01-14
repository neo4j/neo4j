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
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.concurrent.TimeUnit;

/**
 * This class consists of {@code static} utility methods for operating
 * on clocks. These utilities include factory methods for different type of clocks.
 */
public class Clocks
{
    private static final Clock SYSTEM_CLOCK = Clock.systemUTC();

    private Clocks()
    {
        // non-instantiable
    }

    /**
     * Returns system clock.
     * @return system clock
     */
    public static Clock systemClock()
    {
        return SYSTEM_CLOCK;
    }

    /**
     * Returns clock that allow to get current nanos.
     * @return clock with nano time support
     */
    public static SystemNanoClock nanoClock()
    {
        return SystemNanoClock.INSTANCE;
    }

    /**
     * Return new fake clock instance.
     * @return fake clock
     */
    public static FakeClock fakeClock()
    {
        return new FakeClock();
    }

    /**
     * Return new fake clock instance.
     * @param initialTime initial fake clock time
     * @param unit initialTime fake clock time unit
     * @return fake clock
     */
    public static FakeClock fakeClock( long initialTime, TimeUnit unit )
    {
        return new FakeClock( initialTime, unit );
    }

    public static FakeClock fakeClock( TemporalAccessor initialTime )
    {
        return new FakeClock( initialTime.getLong( ChronoField.INSTANT_SECONDS ), TimeUnit.SECONDS )
                .forward( initialTime.getLong( ChronoField.NANO_OF_SECOND ), TimeUnit.NANOSECONDS );
    }

    /**
     * Returns a clock that ticks every time it is accessed
     * @param initialInstant initial time for clock
     * @param tickDuration amount of time of each tick
     * @return access tick clock
     */
    public static TickOnAccessClock tickOnAccessClock( Instant initialInstant, Duration tickDuration )
    {
        return new TickOnAccessClock( initialInstant, tickDuration );
    }
}
