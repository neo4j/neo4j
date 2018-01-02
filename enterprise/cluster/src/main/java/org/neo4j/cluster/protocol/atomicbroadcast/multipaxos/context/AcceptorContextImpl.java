/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorContext;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstance;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorState;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.logging.LogProvider;

/**
 * Context for the {@link AcceptorState} distributed state machine.
 * <p/>
 * This holds the store for Paxos instances, as seen from the acceptor role point of view in Paxos.
 */
class AcceptorContextImpl
        extends AbstractContextImpl
        implements AcceptorContext
{
    private final AcceptorInstanceStore instanceStore;

    AcceptorContextImpl( org.neo4j.cluster.InstanceId me, CommonContextState commonState,
            LogProvider logging,
            Timeouts timeouts, AcceptorInstanceStore instanceStore)
    {
        super( me, commonState, logging, timeouts );
        this.instanceStore = instanceStore;
    }

    @Override
    public AcceptorInstance getAcceptorInstance( InstanceId instanceId )
    {
        return instanceStore.getAcceptorInstance( instanceId );
    }

    @Override
    public void promise( AcceptorInstance instance, long ballot )
    {
        instanceStore.promise( instance, ballot );
    }

    @Override
    public void accept( AcceptorInstance instance, Object value )
    {
        instanceStore.accept( instance, value );
    }

    @Override
    public void leave()
    {
        instanceStore.clear();
    }

    public AcceptorContextImpl snapshot( CommonContextState commonStateSnapshot, LogProvider logging, Timeouts timeouts,
                                         AcceptorInstanceStore instanceStore )
    {
        return new AcceptorContextImpl( me, commonStateSnapshot, logging, timeouts, instanceStore );
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

        AcceptorContextImpl that = (AcceptorContextImpl) o;

        if ( instanceStore != null ? !instanceStore.equals( that.instanceStore ) : that.instanceStore != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return instanceStore != null ? instanceStore.hashCode() : 0;
    }
}
