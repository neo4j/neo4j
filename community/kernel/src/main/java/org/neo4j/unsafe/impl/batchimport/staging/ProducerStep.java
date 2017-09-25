/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.IoThroughputStat;
import org.neo4j.unsafe.impl.batchimport.stats.Key;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

/**
 * Step that generally sits first in a {@link Stage} and produces batches that will flow downstream
 * to other {@link Step steps}.
 */
public abstract class ProducerStep extends AbstractStep<Void> implements StatsProvider
{
    protected final int batchSize;

    public ProducerStep( StageControl control, Configuration config )
    {
        super( control, ">", config );
        this.batchSize = config.batchSize();
    }

    /**
     * Merely receives one call, like a start signal from the staging framework.
     */
    @Override
    public long receive( long ticket, Void batch )
    {
        // It's fine to not store a reference to this thread here because either it completes and exits
        // normally, notices a panic and exits via an exception.
        new Thread( name() )
        {
            @Override
            public void run()
            {
                assertHealthy();
                try
                {
                    process();
                    endOfUpstream();
                }
                catch ( Throwable e )
                {
                    issuePanic( e, false );
                }
            }
        }.start();
        return 0;
    }

    protected abstract void process();

    @SuppressWarnings( "unchecked" )
    protected void sendDownstream( Object batch )
    {
        long time = downstream.receive( doneBatches.getAndIncrement(), batch );
        downstreamIdleTime.add( time );
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
