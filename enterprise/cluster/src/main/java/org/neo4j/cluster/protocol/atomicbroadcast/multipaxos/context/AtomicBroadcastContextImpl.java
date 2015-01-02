/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import java.util.concurrent.Executor;

import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastContext;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.logging.Logging;

class AtomicBroadcastContextImpl
    extends AbstractContextImpl
    implements AtomicBroadcastContext
{
    private Iterable<AtomicBroadcastListener> listeners = Listeners.newListeners();
    private final Executor executor;

    AtomicBroadcastContextImpl( org.neo4j.cluster.InstanceId me, CommonContextState commonState,
                                Logging logging,
                                Timeouts timeouts, Executor executor )
    {
        super( me, commonState, logging, timeouts );
        this.executor = executor;
    }

    @Override
    public void addAtomicBroadcastListener( AtomicBroadcastListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    @Override
    public void removeAtomicBroadcastListener( AtomicBroadcastListener listener )
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

    @Override
    public void receive( final Payload value )
    {
        Listeners.notifyListeners( listeners, executor, new Listeners.Notification<AtomicBroadcastListener>()
        {
            @Override
            public void notify( final AtomicBroadcastListener listener )
            {
                listener.receive( value );
            }
        } );
    }

    public AtomicBroadcastContextImpl snapshot( CommonContextState commonStateSnapshot, Logging logging,
                                                Timeouts timeouts, Executor executor )
    {
        return new AtomicBroadcastContextImpl( me, commonStateSnapshot, logging, timeouts, executor );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return 0;
    }
}
