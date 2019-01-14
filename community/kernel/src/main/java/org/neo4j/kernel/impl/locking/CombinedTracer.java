/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * A {@link LockTracer} that combines multiple {@linkplain LockTracer tracers} into one, invoking each of them for
 * the {@linkplain LockTracer#waitForLock(boolean, ResourceType, long...) wait events} received.
 * <p>
 * This is used for when there is a stack of queries in a transaction, or when a system-configured tracer combines with
 * the query specific tracers.
 */
final class CombinedTracer implements LockTracer
{
    private final LockTracer[] tracers;

    CombinedTracer( LockTracer... tracers )
    {
        this.tracers = tracers;
    }

    @Override
    public LockWaitEvent waitForLock( boolean exclusive, ResourceType resourceType, long... resourceIds )
    {
        LockWaitEvent[] events = new LockWaitEvent[tracers.length];
        for ( int i = 0; i < events.length; i++ )
        {
            events[i] = tracers[i].waitForLock( exclusive, resourceType, resourceIds );
        }
        return new CombinedEvent( events );
    }

    @Override
    public LockTracer combine( LockTracer tracer )
    {
        if ( tracer == NONE )
        {
            return this;
        }
        LockTracer[] tracers;
        if ( tracer instanceof CombinedTracer )
        {
            LockTracer[] those = ((CombinedTracer) tracer).tracers;
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

    private static class CombinedEvent implements LockWaitEvent
    {
        private final LockWaitEvent[] events;

        CombinedEvent( LockWaitEvent[] events )
        {
            this.events = events;
        }

        @Override
        public void close()
        {
            for ( LockWaitEvent event : events )
            {
                event.close();
            }
        }
    }
}
