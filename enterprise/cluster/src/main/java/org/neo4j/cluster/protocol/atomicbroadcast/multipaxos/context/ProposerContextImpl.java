/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.helpers.collection.Iterables.limit;


class ProposerContextImpl
        extends AbstractContextImpl
        implements ProposerContext
{
    public static final int MAX_CONCURRENT_INSTANCES = 10;

    // ProposerContext
    private final Deque<Message> pendingValues;
    private final Map<InstanceId, Message> bookedInstances;

    private final PaxosInstanceStore paxosInstances;
    private HeartbeatContext heartbeatContext;

    ProposerContextImpl( org.neo4j.cluster.InstanceId me, CommonContextState commonState,
                         LogProvider logging,
                         Timeouts timeouts, PaxosInstanceStore paxosInstances,
                         HeartbeatContext heartbeatContext )
    {
        super( me, commonState, logging, timeouts );
        this.paxosInstances = paxosInstances;
        this.heartbeatContext = heartbeatContext;
        pendingValues = new LinkedList<>(  );
        bookedInstances = new HashMap<>();
    }

    private ProposerContextImpl( org.neo4j.cluster.InstanceId me, CommonContextState commonState, LogProvider logging,
                                 Timeouts timeouts, Deque<Message> pendingValues,
                                 Map<InstanceId, Message> bookedInstances, PaxosInstanceStore paxosInstances,
                                 HeartbeatContext heartbeatContext )
    {
        super( me, commonState, logging, timeouts );
        this.pendingValues = pendingValues;
        this.bookedInstances = bookedInstances;
        this.paxosInstances = paxosInstances;
        this.heartbeatContext = heartbeatContext;
    }

    @Override
    public InstanceId newInstanceId()
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
    public void bookInstance( InstanceId instanceId, Message message )
    {
        if ( message.getPayload() == null )
        {
            throw new IllegalArgumentException( "null payload for booking instance: " + message );
        }
        bookedInstances.put( instanceId, message );
    }

    @Override
    public PaxosInstance getPaxosInstance( InstanceId instanceId )
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
    public Message getBookedInstance( InstanceId id )
    {
        return bookedInstances.get( id );
    }

    @Override
    public Message<ProposerMessage> unbookInstance( InstanceId id )
    {
        return bookedInstances.remove( id );
    }

    @Override
    public int nrOfBookedInstances()
    {
        return bookedInstances.size();
    }

    @Override
    public List<URI> getAcceptors()
    {
        Iterable<URI> aliveMembers = Iterables.map( instanceId -> heartbeatContext.getUriForId( instanceId ), heartbeatContext.getAlive() );

        return asList( limit( (int) Math.min(Iterables.count( aliveMembers ), commonState.getMaxAcceptors()), aliveMembers ) );
    }

    @Override
    public int getMinimumQuorumSize( List<URI> acceptors )
    {
        return (acceptors.size() / 2) + 1;
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
            for ( InstanceId instanceId : bookedInstances.keySet() )
            {
                PaxosInstance instance = paxosInstances.getPaxosInstance( instanceId );
                if ( instance.getAcceptors() != null )
                {
                    instance.getAcceptors().remove( commonState.configuration().getMembers().get( value.getJoin()));

                    getLog( ProposerContext.class ).debug( "For booked instance " + instance +
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
            for ( InstanceId instanceId : bookedInstances.keySet() )
            {
                PaxosInstance instance = paxosInstances.getPaxosInstance( instanceId );
                if ( instance.getAcceptors() != null )
                {
                    getLog( ProposerContext.class ).debug( "For booked instance " + instance +
                            " removed leaving member "
                            + value.getLeave() + " (at URI " +
                            commonState.configuration().getMembers().get( value.getLeave() )
                            + ")" );
                    instance.getAcceptors().remove( commonState.configuration().getMembers().get(value.getLeave()));
                }
            }
        }
    }

    public ProposerContextImpl snapshot( CommonContextState commonStateSnapshot, LogProvider logging, Timeouts timeouts,
                                         PaxosInstanceStore paxosInstancesSnapshot, HeartbeatContext heartbeatContext )
    {
        return new ProposerContextImpl( me, commonStateSnapshot, logging, timeouts, new LinkedList<>( pendingValues ),
                new HashMap<>( bookedInstances ), paxosInstancesSnapshot, heartbeatContext );
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

        ProposerContextImpl that = (ProposerContextImpl) o;

        if ( bookedInstances != null ? !bookedInstances.equals( that.bookedInstances ) : that.bookedInstances != null )
        {
            return false;
        }
        if ( paxosInstances != null ? !paxosInstances.equals( that.paxosInstances ) : that.paxosInstances != null )
        {
            return false;
        }
        return pendingValues != null ? pendingValues.equals( that.pendingValues ) : that.pendingValues == null;
    }

    @Override
    public int hashCode()
    {
        int result = pendingValues != null ? pendingValues.hashCode() : 0;
        result = 31 * result + (bookedInstances != null ? bookedInstances.hashCode() : 0);
        result = 31 * result + (paxosInstances != null ? paxosInstances.hashCode() : 0);
        return result;
    }
}
