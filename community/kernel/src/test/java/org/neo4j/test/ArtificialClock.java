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
package org.neo4j.test;

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Clock;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ArtificialClock implements Clock
{
    private volatile long currentTimeNanos;

    @Override
    public long currentTimeMillis()
    {
        return NANOSECONDS.toMillis( currentTimeNanos );
    }

    @Override
    public long nanoTime()
    {
        return currentTimeNanos;
    }

    public Progressor progressor( long time, TimeUnit unit )
    {
        return new Progressor( unit.toNanos( time ) );
    }

    public void progress( long time, TimeUnit unit )
    {
        progress( unit.toNanos( time ) );
    }

    private synchronized void progress( long nanos )
    {
        currentTimeNanos += nanos;
    }

    public class Progressor
    {
        private final long nanos;

        private Progressor( long nanos )
        {
            this.nanos = nanos;
        }

        public void tick()
        {
            progress( nanos );
        }
    }
}
