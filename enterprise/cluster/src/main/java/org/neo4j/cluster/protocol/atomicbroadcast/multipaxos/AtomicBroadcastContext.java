/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.net.URI;
import java.util.concurrent.Executor;

import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.helpers.Listeners;

/**
 * Context shared by all Paxos state machines.
 */
public class AtomicBroadcastContext
{
    private final ClusterContext context;
    private Executor executor;

    private Iterable<AtomicBroadcastListener> listeners = Listeners.newListeners();

    public AtomicBroadcastContext( ClusterContext context, Executor executor )
    {
        this.context = context;
        this.executor = executor;
    }

    public void addAtomicBroadcastListener( AtomicBroadcastListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    public void removeAtomicBroadcastListener( AtomicBroadcastListener listener )
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

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

    public URI getCoordinator()
    {
        return context.getConfiguration().getElected( ClusterConfiguration.COORDINATOR );
    }

    public ClusterContext getClusterContext()
    {
        return context;
    }
}
