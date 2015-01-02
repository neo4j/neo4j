/**
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
package org.neo4j.server.statistic;

import java.util.concurrent.atomic.AtomicLong;

/**
 * statistics-collector will keep n-statistic records
 *
 * @author tbaum
 * @since 31.05.11 20:23
 */
public class StatisticCollector
{
    private volatile long start = System.currentTimeMillis();
    private volatile StatisticData currentSize = new StatisticData();
    private volatile StatisticData currentDuration = new StatisticData();
    private final AtomicLong count = new AtomicLong( 0l );
    private StatisticRecord snapshot = createSnapshot();

    public StatisticRecord currentSnapshot()
    {
        return snapshot;
    }

    public synchronized StatisticRecord createSnapshot()
    {
        final StatisticData previousDuration, previousSize;
        final long timeStamp, period, previousCount, previousStart;

        synchronized (this)
        {
            previousDuration = currentDuration.copy();
            previousSize = currentSize.copy();
            previousCount = count.get();
            previousStart = start;

            currentDuration = new StatisticData();
            currentSize = new StatisticData();
            start = System.currentTimeMillis();
            count.set( 0l );

            timeStamp = start;
            period = ( start - previousStart );
        }

        return snapshot = new StatisticRecord( timeStamp, period, previousCount, previousDuration, previousSize );
    }

    /**
     * add one datapoint for statistics
     *
     * @param time duration of the request
     * @param size size in bytes of the request
     */
    public synchronized void update( final double time, final long size )
    {
        currentDuration.addValue( time );
        currentSize.addValue( size );
        count.incrementAndGet();
    }
}
