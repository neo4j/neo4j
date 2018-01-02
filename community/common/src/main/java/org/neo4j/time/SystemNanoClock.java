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
package org.neo4j.time;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * {@link Clock} that support nano time resolution.
 * @see Clocks
 */
public class SystemNanoClock extends Clock
{
    static final SystemNanoClock INSTANCE = new SystemNanoClock();

    protected SystemNanoClock()
    {
        // please use shared instance
    }

    @Override
    public ZoneId getZone()
    {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone( ZoneId zone )
    {
        throw new UnsupportedOperationException( "Zone update is not supported." );
    }

    @Override
    public Instant instant()
    {
        return Instant.now();
    }

    public long nanos()
    {
        return System.nanoTime();
    }
}
