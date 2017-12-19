/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.Timer;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.causalclustering.core.consensus.RaftMessageProcessingMonitor;
import org.neo4j.causalclustering.core.consensus.RaftMessages;

public class RaftMessageProcessingMetric implements RaftMessageProcessingMonitor
{
    private final AtomicLong delay = new AtomicLong( 0 );
    private Timer timer = new Timer();
    private Map<RaftMessages.Type,Timer> typeTimers = new EnumMap<>( RaftMessages.Type.class );

    public RaftMessageProcessingMetric()
    {
        for ( RaftMessages.Type type : RaftMessages.Type.values() )
        {
            typeTimers.put( type, new Timer() );
        }
    }

    public long delay()
    {
        return delay.get();
    }

    @Override
    public void setDelay( Duration delay )
    {
        this.delay.set( delay.toMillis() );
    }

    public Timer timer()
    {
        return timer;
    }

    public Timer timer( RaftMessages.Type type )
    {
        return typeTimers.get( type );
    }

    @Override
    public void updateTimer( RaftMessages.Type type, Duration duration )
    {
        long nanos = duration.toNanos();
        timer.update( nanos, TimeUnit.NANOSECONDS );
        typeTimers.get( type ).update( nanos, TimeUnit.NANOSECONDS );
    }
}
