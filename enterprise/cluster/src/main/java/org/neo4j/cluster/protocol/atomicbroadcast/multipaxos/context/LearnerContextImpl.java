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

import org.neo4j.cluster.ClusterInstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.PaxosInstance;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.PaxosInstanceStore;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.kernel.logging.Logging;

class LearnerContextImpl
        extends AbstractContextImpl
        implements LearnerContext
{
    // LearnerContext
    private long lastDeliveredInstanceId = -1;
    private long lastLearnedInstanceId = -1;

    private final HeartbeatContext heartbeatContext;
    private final AcceptorInstanceStore instanceStore;
    private final ObjectInputStreamFactory objectInputStreamFactory;
    private final ObjectOutputStreamFactory objectOutputStreamFactory;
    private final PaxosInstanceStore paxosInstances;

    LearnerContextImpl( ClusterInstanceId me, CommonContextState commonState,
                        Logging logging,
                        Timeouts timeouts, PaxosInstanceStore paxosInstances,
                        AcceptorInstanceStore instanceStore,
                        ObjectInputStreamFactory objectInputStreamFactory,
                        ObjectOutputStreamFactory objectOutputStreamFactory,
                        HeartbeatContext heartbeatContext )
    {
        super( me, commonState, logging, timeouts );
        this.heartbeatContext = heartbeatContext;
        this.instanceStore = instanceStore;
        this.objectInputStreamFactory = objectInputStreamFactory;
        this.objectOutputStreamFactory = objectOutputStreamFactory;
        this.paxosInstances = paxosInstances;
    }

    private LearnerContextImpl( ClusterInstanceId me, CommonContextState commonState, Logging logging,
                                Timeouts timeouts, long lastDeliveredInstanceId, long lastLearnedInstanceId,
                                HeartbeatContext heartbeatContext,
                        AcceptorInstanceStore instanceStore, ObjectInputStreamFactory objectInputStreamFactory,
                        ObjectOutputStreamFactory objectOutputStreamFactory, PaxosInstanceStore paxosInstances )
    {
        super( me, commonState, logging, timeouts );
        this.lastDeliveredInstanceId = lastDeliveredInstanceId;
        this.lastLearnedInstanceId = lastLearnedInstanceId;
        this.heartbeatContext = heartbeatContext;
        this.instanceStore = instanceStore;
        this.objectInputStreamFactory = objectInputStreamFactory;
        this.objectOutputStreamFactory = objectOutputStreamFactory;
        this.paxosInstances = paxosInstances;
    }

    @Override
    public long getLastDeliveredInstanceId()
    {
        return lastDeliveredInstanceId;
    }

    @Override
    public void setLastDeliveredInstanceId( long lastDeliveredInstanceId )
    {
        this.lastDeliveredInstanceId = lastDeliveredInstanceId;
        instanceStore.lastDelivered( new InstanceId( lastDeliveredInstanceId ) );
    }

    @Override
    public long getLastLearnedInstanceId()
    {
        return lastLearnedInstanceId;
    }

    @Override
    public long getLastKnownLearnedInstanceInCluster()
    {
        return commonState.lastKnownLearnedInstanceInCluster();
    }

    @Override
    public void setLastKnownLearnedInstanceInCluster( long lastKnownLearnedInstanceInCluster, ClusterInstanceId serverId )
    {
        commonState.setLastKnownLearnedInstanceInCluster( lastKnownLearnedInstanceInCluster, serverId );
    }

    @Override
    public ClusterInstanceId getLastKnownAliveUpToDateInstance()
    {
        return commonState.lastKnownAliveUpToDateInstance();
    }

    @Override
    public void learnedInstanceId( long paxosInstanceId, ClusterInstanceId clusterInstanceId )
    {
        this.lastLearnedInstanceId = Math.max( lastLearnedInstanceId, paxosInstanceId );
        if ( lastLearnedInstanceId > commonState.lastKnownLearnedInstanceInCluster() )
        {
            commonState.setLastKnownLearnedInstanceInCluster( lastLearnedInstanceId, clusterInstanceId );
        }
    }

    @Override
    public boolean hasDeliveredAllKnownInstances()
    {
        return lastDeliveredInstanceId == commonState.lastKnownLearnedInstanceInCluster();
    }

    @Override
    public void leave()
    {
        lastDeliveredInstanceId = -1;
        lastLearnedInstanceId = -1;
        commonState.setLastKnownLearnedInstanceInCluster( -1, null );
    }

    @Override
    public PaxosInstance getPaxosInstance( InstanceId instanceId )
    {
        return paxosInstances.getPaxosInstance( instanceId );
    }

    @Override
    public AtomicBroadcastSerializer newSerializer()
    {
        return new AtomicBroadcastSerializer( objectInputStreamFactory, objectOutputStreamFactory );
    }

    @Override
    public Iterable<ClusterInstanceId> getAlive()
    {
        return heartbeatContext.getAlive();
    }

    @Override
    public void setNextInstanceId( long id )
    {
        commonState.setNextInstanceId( id );
    }

    public LearnerContextImpl snapshot( CommonContextState commonStateSnapshot, Logging logging, Timeouts timeouts,
                                        PaxosInstanceStore paxosInstancesSnapshot, AcceptorInstanceStore instanceStore,
                                        ObjectInputStreamFactory objectInputStreamFactory, ObjectOutputStreamFactory
            objectOutputStreamFactory, HeartbeatContextImpl snapshotHeartbeatContext )
    {
        return new LearnerContextImpl( me, commonStateSnapshot, logging, timeouts, lastDeliveredInstanceId,
                lastLearnedInstanceId, snapshotHeartbeatContext, instanceStore, objectInputStreamFactory,
                objectOutputStreamFactory, paxosInstancesSnapshot );
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

        LearnerContextImpl that = (LearnerContextImpl) o;

        if ( lastDeliveredInstanceId != that.lastDeliveredInstanceId )
        {
            return false;
        }
        if ( lastLearnedInstanceId != that.lastLearnedInstanceId )
        {
            return false;
        }
        if ( heartbeatContext != null ? !heartbeatContext.equals( that.heartbeatContext ) : that.heartbeatContext !=
                null )
        {
            return false;
        }
        if ( instanceStore != null ? !instanceStore.equals( that.instanceStore ) : that.instanceStore != null )
        {
            return false;
        }
        if ( paxosInstances != null ? !paxosInstances.equals( that.paxosInstances ) : that.paxosInstances != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (lastDeliveredInstanceId ^ (lastDeliveredInstanceId >>> 32));
        result = 31 * result + (int) (lastLearnedInstanceId ^ (lastLearnedInstanceId >>> 32));
        result = 31 * result + (heartbeatContext != null ? heartbeatContext.hashCode() : 0);
        result = 31 * result + (instanceStore != null ? instanceStore.hashCode() : 0);
        result = 31 * result + (paxosInstances != null ? paxosInstances.hashCode() : 0);
        return result;
    }
}
