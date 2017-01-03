/*
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
package org.neo4j.kernel.impl.locking;

import java.util.Arrays;

import org.neo4j.storageengine.api.lock.ResourceType;

/**
 * A {@link Locks.Tracer} that combines multiple {@linkplain Locks.Tracer tracers} into one, invoking each of them for
 * the {@linkplain #waitForLock(ResourceType, long...) wait events} received.
 * <p>
 * This is used for when there is a stack of queries in a transaction, or when a system-configured tracer combines with
 * the query specific tracers.
 */
final class CombinedTracer implements Locks.Tracer
{
    private final Locks.Tracer[] tracers;

    CombinedTracer( Locks.Tracer... tracers )
    {
        this.tracers = tracers;
    }

    @Override
    public Locks.WaitEvent waitForLock( ResourceType resourceType, long... resourceIds )
    {
        Locks.WaitEvent[] events = new Locks.WaitEvent[tracers.length];
        for ( int i = 0; i < events.length; i++ )
        {
            events[i] = tracers[i].waitForLock( resourceType, resourceIds );
        }
        return new CombinedEvent( events );
    }

    @Override
    public Locks.Tracer combine( Locks.Tracer tracer )
    {
        if ( tracer == NONE )
        {
            return this;
        }
        Locks.Tracer[] tracers;
        if ( tracer instanceof CombinedTracer )
        {
            Locks.Tracer[] those = ((CombinedTracer) tracer).tracers;
            tracers = Arrays.copyOf( this.tracers, this.tracers.length + those.length );
            System.arraycopy( those, 0, tracers, this.tracers.length, those.length );
        }
        else
        {
            tracers = Arrays.copyOf( this.tracers, this.tracers.length + 1 );
            tracers[this.tracers.length] = tracer;
        }
        return new CombinedTracer( tracers );
    }

    private static class CombinedEvent implements Locks.WaitEvent
    {
        private final Locks.WaitEvent[] events;

        CombinedEvent( Locks.WaitEvent[] events )
        {
            this.events = events;
        }

        @Override
        public void close()
        {
            for ( Locks.WaitEvent event : events )
            {
                event.close();
            }
        }
    }
}
