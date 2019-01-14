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

/**
 * A {@link java.time.Clock} that ticks every time it is accessed.
 */
public class TickOnAccessClock extends Clock
{
    private Instant currentInstant;
    private final Duration tickDuration;

    TickOnAccessClock( Instant initialTime, Duration tickDuration )
    {
        this.currentInstant = initialTime;
        this.tickDuration = tickDuration;
    }

    @Override
    public ZoneId getZone()
    {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone( ZoneId zone )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant()
    {
        Instant instant = currentInstant;
        tick();
        return instant;
    }

    private void tick()
    {
        currentInstant = currentInstant.plus( tickDuration );
    }
}
