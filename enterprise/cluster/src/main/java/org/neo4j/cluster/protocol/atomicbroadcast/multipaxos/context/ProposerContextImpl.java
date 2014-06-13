/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.net.URI;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.PaxosInstance;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.PaxosInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerContext;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.kernel.logging.Logging;

class ProposerContextImpl
        extends AbstractContextImpl
        implements ProposerContext
{
    public static final int MAX_CONCURRENT_INSTANCES = 10;

    // ProposerContext
    private final Deque<Message> pendingValues;
    private final Map<InstanceId, Message> bookedInstances;

    private final PaxosInstanceStore paxosInstances;

    ProposerContextImpl( org.neo4j.cluster.InstanceId me, CommonContextState commonState,
                         Logging logging,
                         Timeouts timeouts, PaxosInstanceStore paxosInstances )
    {
        super( me, commonState, logging, timeouts );
        this.paxosInstances = paxosInstances;
        pendingValues = new LinkedList<Message>(  );
        bookedInstances = new HashMap<InstanceId, Message>();
    }

    @Override
    public org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId newInstanceId()
    {
        // Never propose something lower than last received instance id
        if ( commonState.lastKnownLearnedInstanceInCluster() >= commonState.nextInstanceId() )
        {
            commonState.setNextInstanceId( commonState.lastKnownLearnedInstanceInCluster() + 1 );
        }

        return new InstanceId( commonState.getAndIncrementInstanceId() );
    }

    @Override
    public void leave()
    {
        pendingValues.clear();
        bookedInstances.clear();
        commonState.setNextInstanceId( 0 );

        paxosInstances.leave();
    }

    @Override
    public void bookInstance( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId, Message message )
    {
        if ( message.getPayload() == null )
        {
            throw new IllegalArgumentException( "null payload for booking instance: " + message );
        }
        bookedInstances.put( instanceId, message );
    }

    @Override
    public PaxosInstance getPaxosInstance( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId )
    {
        return paxosInstances.getPaxosInstance( instanceId );
    }

    @Override
    public void pendingValue( Message message )
    {
        pendingValues.offerFirst( message );
    }

    @Override
    public boolean hasPendingValues()
    {
        return !pendingValues.isEmpty();
    }

    @Override
    public Message popPendingValue()
    {
        return pendingValues.remove();
    }

    @Override
    public boolean canBookInstance()
    {
        return bookedInstances.size() < MAX_CONCURRENT_INSTANCES;
    }

    @Override
    public Message getBookedInstance( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId id )
    {
        return bookedInstances.get( id );
    }

    @Override
    public Message<ProposerMessage> unbookInstance( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId id )
    {
        return bookedInstances.remove( id );
    }

    @Override
    public int nrOfBookedInstances()
    {
        return bookedInstances.size();
    }

    @Override
    public int getMinimumQuorumSize( List<URI> acceptors )
    {
        // n >= 2f+1
        if ( acceptors.size() >= 2 * commonState.configuration().getAllowedFailures() + 1 )
        {
            return acceptors.size() - commonState.configuration().getAllowedFailures();
        }
        else
        {
            return acceptors.size();
        }
    }

    /**
     * This patches the booked instances that are pending in case the configuration of the cluster changes. This
     * should be called only when we learn a ConfigurationChangeState i.e. when we receive an accepted for
     * such a message. This won't "learn" the message, as in applying it on the cluster configuration, but will
     * just update properly the set of acceptors for pending instances.
     */
    @Override
    public void patchBookedInstances( ClusterMessage.ConfigurationChangeState value )
    {
        if ( value.getJoin() != null )
        {
            for ( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId : bookedInstances.keySet() )
            {
                PaxosInstance instance = paxosInstances.getPaxosInstance( instanceId );
                if ( instance.getAcceptors() != null )
                {
                    instance.getAcceptors().remove( commonState.configuration().getMembers().get( value.getJoin() ) );

                    getLogger( ProposerContext.class ).debug( "For booked instance " + instance +
                            " removed gone member "
                            + commonState.configuration().getMembers().get( value.getJoin() )
                            + " added joining member " +
                            value.getJoinUri() );

                    if ( !instance.getAcceptors().contains( value.getJoinUri() ) )
                    {
                        instance.getAcceptors().add( value.getJoinUri() );
                    }
                }
            }
        }
        else if ( value.getLeave() != null )
        {
            for ( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId : bookedInstances.keySet() )
            {
                PaxosInstance instance = paxosInstances.getPaxosInstance( instanceId );
                if ( instance.getAcceptors() != null )
                {
                    getLogger( ProposerContext.class ).debug( "For booked instance " + instance +
                            " removed leaving member "
                            + value.getLeave() + " (at URI " +
                            commonState.configuration().getMembers().get( value.getLeave() )
                            + ")" );
                    instance.getAcceptors().remove( commonState.configuration().getMembers().get( value.getLeave() ) );
                }
            }
        }
    }
}
