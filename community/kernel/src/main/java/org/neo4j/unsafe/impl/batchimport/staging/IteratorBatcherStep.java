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
package org.neo4j.unsafe.impl.batchimport.staging;

import java.util.Collection;

import org.neo4j.helpers.Format;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.stats.DetailLevel;
import org.neo4j.unsafe.impl.batchimport.stats.Key;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

import static java.lang.System.currentTimeMillis;

/**
 * Takes an Iterator and chops it up into batches downstream.
 */
public class IteratorBatcherStep<T> extends ProducerStep<T> implements StatsProvider
{
    private final InputIterator<T> data;
    private long startTime, endTime;

    public IteratorBatcherStep( StageControl control, String name, int batchSize, int movingAverageSize,
            InputIterator<T> data )
    {
        super( control, name, batchSize, movingAverageSize );
        this.data = data;
    }

    @Override
    protected T nextOrNull()
    {
        return data.hasNext() ? data.next() : null;
    }

    @Override
    public void start()
    {
        startTime = currentTimeMillis();
    }

    @Override
    public void close()
    {
        data.close();
    }

    @Override
    protected void addStatsProviders( Collection<StatsProvider> providers )
    {
        super.addStatsProviders( providers );
        providers.add( this );
    }

    @Override
    public Stat stat( Key key )
    {
        if ( key == Keys.io_throughput )
        {
            return new Stat()
            {
                @Override
                public String toString()
                {
                    long stat = asLong();
                    return stat == -1 ? "??" : Format.bytes( stat ) + "/s";
                }

                @Override
                public long asLong()
                {
                    long thisEndTime = endTime != 0 ? endTime : currentTimeMillis();
                    long totalTime = thisEndTime-startTime;
                    int seconds = (int) (totalTime/1000);
                    return seconds > 0 ? data.position()/seconds : -1;
                }

                @Override
                public DetailLevel detailLevel()
                {
                    return DetailLevel.IMPORTANT;
                }
            };
        }
        return null;
    }

    @Override
    protected void done()
    {
        endTime = currentTimeMillis();
    }

    @Override
    public Key[] keys()
    {
        return new Key[] { Keys.io_throughput };
    }
}
