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

import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerState;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.PaxosInstance;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.PaxosInstanceStore;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.logging.LogProvider;
import org.neo4j.kernel.impl.util.CappedLogger;

class LearnerContextImpl
        extends AbstractContextImpl
        implements LearnerContext
{
    // LearnerContext
    private long lastDeliveredInstanceId = -1;
    private long lastLearnedInstanceId = -1;

    /** To minimize logging, keep track of the latest learn miss, only log when it changes. */
    private final CappedLogger learnMissLogger;

    private final HeartbeatContext heartbeatContext;
    private final AcceptorInstanceStore instanceStore;
    private final ObjectInputStreamFactory objectInputStreamFactory;
    private final ObjectOutputStreamFactory objectOutputStreamFactory;
    private final PaxosInstanceStore paxosInstances;

    LearnerContextImpl( org.neo4j.cluster.InstanceId me, CommonContextState commonState,
                        LogProvider logging,
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
        this.learnMissLogger = new CappedLogger( logging.getLog( LearnerState.class ) )
                .setDuplicateFilterEnabled( true );
    }

    private LearnerContextImpl( org.neo4j.cluster.InstanceId me, CommonContextState commonState, LogProvider logging,
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
        this.learnMissLogger = new CappedLogger( logging.getLog( LearnerState.class ) ).setDuplicateFilterEnabled(
                true );
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
    public void setLastKnownLearnedInstanceInCluster( long lastKnownLearnedInstanceInCluster,
            org.neo4j.cluster.InstanceId instanceId )
    {
        commonState.setLastKnownLearnedInstanceInCluster( lastKnownLearnedInstanceInCluster, instanceId );
    }

    @Override
    public org.neo4j.cluster.InstanceId getLastKnownAliveUpToDateInstance()
    {
        return commonState.getLastKnownAliveUpToDateInstance();
    }

    @Override
    public void learnedInstanceId( long instanceId )
    {
        this.lastLearnedInstanceId = Math.max( lastLearnedInstanceId, instanceId );
        if ( lastLearnedInstanceId > commonState.lastKnownLearnedInstanceInCluster() )
        {
            commonState.setLastKnownLearnedInstanceInCluster( lastLearnedInstanceId, null );
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
    public Iterable<org.neo4j.cluster.InstanceId> getAlive()
    {
        return heartbeatContext.getAlive();
    }

    @Override
    public void setNextInstanceId( long id )
    {
        commonState.setNextInstanceId( id );
    }

    @Override
    public void notifyLearnMiss( InstanceId instanceId )
    {
        learnMissLogger.warn( "Did not have learned value for Paxos instance " + instanceId + ". " +
                              "This generally indicates that this instance has missed too many cluster events and is " +
                              "failing to catch up. If this error does not resolve soon it may become necessary to " +
                              "restart this cluster member so normal operation can resume." );
    }

    public LearnerContextImpl snapshot( CommonContextState commonStateSnapshot, LogProvider logging, Timeouts timeouts,
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
