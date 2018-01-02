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
package org.neo4j.unsafe.impl.batchimport.staging;

import java.util.Collection;

import org.neo4j.unsafe.impl.batchimport.IoThroughputStat;
import org.neo4j.unsafe.impl.batchimport.stats.Key;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

/**
 * {@link ProducerStep} that has built-in support for I/O stats.
 */
public abstract class IoProducerStep extends ProducerStep implements StatsProvider
{
    public IoProducerStep( StageControl control, Configuration config )
    {
        super( control, ">", config );
    }

    @Override
    protected void collectStatsProviders( Collection<StatsProvider> into )
    {
        super.collectStatsProviders( into );
        into.add( this );
    }

    @Override
    public Stat stat( Key key )
    {
        if ( key == Keys.io_throughput )
        {
            return new IoThroughputStat( startTime, endTime, position() );
        }
        return null;
    }

    @Override
    public Key[] keys()
    {
        return new Key[] { Keys.io_throughput };
    }

    /**
     * @return progress in terms of bytes.
     */
    protected abstract long position();
}
