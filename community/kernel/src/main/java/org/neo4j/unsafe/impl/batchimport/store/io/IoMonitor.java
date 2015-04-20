/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.store.io;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.unsafe.impl.batchimport.IoThroughputStat;
import org.neo4j.unsafe.impl.batchimport.stats.Key;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

import static java.lang.System.currentTimeMillis;

/**
 * {@link Monitor} exposed as a {@link StatsProvider}.
 *
 * Assumes that I/O is busy all the time.
 */
public class IoMonitor implements StatsProvider, Monitor
{
    private volatile long startTime = currentTimeMillis(), endTime;
    private final AtomicLong totalWritten = new AtomicLong();

    @Override
    public void dataWritten( int bytes )
    {
        totalWritten.addAndGet( bytes );
    }

    public void reset()
    {
        startTime = currentTimeMillis();
        endTime = 0;
        totalWritten.set( 0 );
    }

    public void stop()
    {
        endTime = currentTimeMillis();
    }

    public long startTime()
    {
        return startTime;
    }

    public long totalBytesWritten()
    {
        return totalWritten.get();
    }

    @Override
    public Stat stat( Key key )
    {
        if ( key == Keys.io_throughput )
        {
            return new IoThroughputStat( startTime, endTime, totalWritten.get() );
        }
        return null;
    }

    @Override
    public Key[] keys()
    {
        return new Key[] { Keys.io_throughput };
    }
}
