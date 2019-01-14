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
            Timeouts timeouts, AcceptorInstanceStore instanceStore )
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

        return instanceStore != null ? instanceStore.equals( that.instanceStore ) : that.instanceStore == null;
    }

    @Override
    public int hashCode()
    {
        return instanceStore != null ? instanceStore.hashCode() : 0;
    }
}
