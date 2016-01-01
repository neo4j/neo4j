/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.helpers.Format;
import org.neo4j.unsafe.impl.batchimport.stats.GenericStatsProvider;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

import static java.lang.System.currentTimeMillis;

/**
 * {@link Monitor} exposed as a {@link StatsProvider}.
 *
 * Assumes that I/O is busy all the time.
 */
public class IoMonitor extends GenericStatsProvider implements Monitor
{
    private volatile long startTime = currentTimeMillis();
    private final AtomicLong totalWritten = new AtomicLong();

    public IoMonitor()
    {
        add( Keys.write_throughput, new Stat()
        {
            @Override
            public String asString()
            {
                long stat = asLong();
                return stat == -1 ? "??" : Format.bytes( stat ) + "/s";
            }

            @Override
            public long asLong()
            {
                long totalTime = currentTimeMillis()-startTime;
                int seconds = (int) (totalTime/1000);
                return seconds > 0 ? totalWritten.get()/seconds : -1;
            }
        } );
    }

    @Override
    public void dataWritten( int bytes )
    {
        totalWritten.addAndGet( bytes );
    }

    public void reset()
    {
        startTime = currentTimeMillis();
        totalWritten.set( 0 );
    }

    public long startTime()
    {
        return startTime;
    }

    public long totalBytesWritten()
    {
        return totalWritten.get();
    }
}
