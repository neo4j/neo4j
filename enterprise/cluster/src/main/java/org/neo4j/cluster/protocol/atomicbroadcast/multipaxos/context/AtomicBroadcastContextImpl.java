/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import java.util.concurrent.Executor;

import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastContext;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastState;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.cluster.util.Quorums;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.LogProvider;

/**
 * Context for {@link AtomicBroadcastState} state machine.
 * <p/>
 * This holds the set of listeners for atomic broadcasts, and allows distribution of received values to those listeners.
 */
class AtomicBroadcastContextImpl
    extends AbstractContextImpl
    implements AtomicBroadcastContext
{
    private final Listeners<AtomicBroadcastListener> listeners = new Listeners<>();
    private final Executor executor;
    private final HeartbeatContext heartbeatContext;

    AtomicBroadcastContextImpl( org.neo4j.cluster.InstanceId me, CommonContextState commonState,
                                LogProvider logging,
                                Timeouts timeouts, Executor executor, HeartbeatContext heartbeatContext  )
    {
        super( me, commonState, logging, timeouts );
        this.executor = executor;
        this.heartbeatContext = heartbeatContext;
    }

    @Override
    public void addAtomicBroadcastListener( AtomicBroadcastListener listener )
    {
        listeners.add( listener );
    }

    @Override
    public void removeAtomicBroadcastListener( AtomicBroadcastListener listener )
    {
        listeners.remove( listener );
    }

    @Override
    public void receive( Payload value )
    {
        listeners.notify( executor, listener -> listener.receive( value ) );
    }

    public AtomicBroadcastContextImpl snapshot( CommonContextState commonStateSnapshot, LogProvider logging,
                                                Timeouts timeouts, Executor executor, HeartbeatContext heartbeatContext )
    {
        return new AtomicBroadcastContextImpl( me, commonStateSnapshot, logging, timeouts, executor, heartbeatContext );
    }

    @Override
    public boolean equals( Object o )
    {
        return this == o || !(o == null || getClass() != o.getClass());
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public boolean hasQuorum()
    {
        int availableMembers = (int) Iterables.count( heartbeatContext.getAlive() );
        int totalMembers = commonState.configuration().getMembers().size();
        return Quorums.isQuorum( availableMembers, totalMembers );
    }
}
