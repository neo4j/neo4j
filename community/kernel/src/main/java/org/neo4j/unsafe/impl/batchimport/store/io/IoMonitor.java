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
package org.neo4j.unsafe.impl.batchimport.store.io;

import org.neo4j.unsafe.impl.batchimport.IoThroughputStat;
import org.neo4j.unsafe.impl.batchimport.stats.Key;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

import static java.lang.System.currentTimeMillis;

/**
 * {@link IoTracer} exposed as a {@link StatsProvider}.
 *
 * Assumes that I/O is busy all the time.
 */
public class IoMonitor implements StatsProvider
{
    private volatile long startTime = currentTimeMillis(), endTime;
    private final IoTracer tracer;
    private long resetPoint;

    public IoMonitor( IoTracer tracer )
    {
        this.tracer = tracer;
    }

    public void reset()
    {
        startTime = currentTimeMillis();
        endTime = 0;
        resetPoint = tracer.countBytesWritten();
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
        return tracer.countBytesWritten() - resetPoint;
    }

    @Override
    public Stat stat( Key key )
    {
        if ( key == Keys.io_throughput )
        {
            return new IoThroughputStat( startTime, endTime, totalBytesWritten() );
        }
        return null;
    }

    @Override
    public Key[] keys()
    {
        return new Key[] { Keys.io_throughput };
    }
}
