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

import java.util.concurrent.Executor;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorContext;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.PaxosInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.election.ElectionContext;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.ElectionRole;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.logging.LogProvider;

/**
 * Context that implements all the context interfaces used by the Paxos state machines.
 * <p>
 * The design here is that all shared state is handled in a common class, {@link CommonContextState}, while all
 * state specific to some single context is contained within the specific context classes.
 */
public class MultiPaxosContext
{
    private final ClusterContextImpl clusterContext;
    private final ProposerContextImpl proposerContext;
    private final AcceptorContextImpl acceptorContext;
    private final LearnerContextImpl learnerContext;
    private final HeartbeatContextImpl heartbeatContext;
    private final ElectionContextImpl electionContext;
    private final AtomicBroadcastContextImpl atomicBroadcastContext;
    private final CommonContextState commonState;
    private final PaxosInstanceStore paxosInstances;

    public MultiPaxosContext( InstanceId me,
                              int maxAcceptors,
                              Iterable<ElectionRole> roles,
                              ClusterConfiguration configuration,
                              Executor executor,
                              LogProvider logging,
                              ObjectInputStreamFactory objectInputStreamFactory,
                              ObjectOutputStreamFactory objectOutputStreamFactory,
                              AcceptorInstanceStore instanceStore,
                              Timeouts timeouts,
                              ElectionCredentialsProvider electionCredentialsProvider )
    {
        commonState = new CommonContextState( configuration, maxAcceptors );
        paxosInstances = new PaxosInstanceStore();

        heartbeatContext = new HeartbeatContextImpl(me, commonState, logging, timeouts, executor );
        learnerContext = new LearnerContextImpl(me, commonState, logging, timeouts, paxosInstances, instanceStore, objectInputStreamFactory, objectOutputStreamFactory, heartbeatContext );
        clusterContext = new ClusterContextImpl(me, commonState, logging, timeouts, executor, objectOutputStreamFactory, objectInputStreamFactory, learnerContext, heartbeatContext );
        electionContext = new ElectionContextImpl( me, commonState, logging, timeouts, roles, clusterContext, heartbeatContext, electionCredentialsProvider );
        proposerContext = new ProposerContextImpl(me, commonState, logging, timeouts, paxosInstances, heartbeatContext );
        acceptorContext = new AcceptorContextImpl(me, commonState, logging, timeouts, instanceStore);
        atomicBroadcastContext = new AtomicBroadcastContextImpl(me, commonState, logging, timeouts, executor, heartbeatContext );

        heartbeatContext.setCircularDependencies( clusterContext, learnerContext );
    }

    private MultiPaxosContext( ProposerContextImpl proposerContext, AcceptorContextImpl acceptorContext,
                               LearnerContextImpl learnerContext, HeartbeatContextImpl heartbeatContext,
                               ElectionContextImpl electionContext, AtomicBroadcastContextImpl atomicBroadcastContext,
                               CommonContextState commonState, PaxosInstanceStore paxosInstances,
                               ClusterContextImpl clusterContext )
    {
        this.clusterContext = clusterContext;
        this.proposerContext = proposerContext;
        this.acceptorContext = acceptorContext;
        this.learnerContext = learnerContext;
        this.heartbeatContext = heartbeatContext;
        this.electionContext = electionContext;
        this.atomicBroadcastContext = atomicBroadcastContext;
        this.commonState = commonState;
        this.paxosInstances = paxosInstances;
    }

    public ClusterContext getClusterContext()
    {
        return clusterContext;
    }

    public ProposerContext getProposerContext()
    {
        return proposerContext;
    }

    public AcceptorContext getAcceptorContext()
    {
        return acceptorContext;
    }

    public LearnerContext getLearnerContext()
    {
        return learnerContext;
    }

    public HeartbeatContext getHeartbeatContext()
    {
        return heartbeatContext;
    }

    public ElectionContext getElectionContext()
    {
        return electionContext;
    }

    public AtomicBroadcastContextImpl getAtomicBroadcastContext()
    {
        return atomicBroadcastContext;
    }

    /** Create a state snapshot. The snapshot will not duplicate services, and expects the caller to duplicate
     * {@link AcceptorInstanceStore}, since that is externally provided.  */
    public MultiPaxosContext snapshot(LogProvider logging, Timeouts timeouts, Executor executor,
                                      AcceptorInstanceStore instanceStore,
                                      ObjectInputStreamFactory objectInputStreamFactory,
                                      ObjectOutputStreamFactory objectOutputStreamFactory,
                                      ElectionCredentialsProvider electionCredentialsProvider)
    {
        CommonContextState commonStateSnapshot = commonState.snapshot( logging.getLog( ClusterConfiguration.class ) );
        PaxosInstanceStore paxosInstancesSnapshot = paxosInstances.snapshot();

        HeartbeatContextImpl snapshotHeartbeatContext =
                heartbeatContext.snapshot( commonStateSnapshot, logging, timeouts, executor );
        LearnerContextImpl snapshotLearnerContext =
                learnerContext.snapshot( commonStateSnapshot, logging, timeouts, paxosInstancesSnapshot, instanceStore,
                        objectInputStreamFactory, objectOutputStreamFactory, snapshotHeartbeatContext );
        ClusterContextImpl snapshotClusterContext =
                clusterContext.snapshot( commonStateSnapshot, logging, timeouts, executor, objectOutputStreamFactory,
                        objectInputStreamFactory, snapshotLearnerContext, snapshotHeartbeatContext );
        ElectionContextImpl snapshotElectionContext =
                electionContext.snapshot( commonStateSnapshot, logging, timeouts, snapshotClusterContext,
                        snapshotHeartbeatContext, electionCredentialsProvider );
        ProposerContextImpl snapshotProposerContext =
                proposerContext.snapshot( commonStateSnapshot, logging, timeouts, paxosInstancesSnapshot, heartbeatContext );
        AcceptorContextImpl snapshotAcceptorContext =
                acceptorContext.snapshot( commonStateSnapshot, logging, timeouts, instanceStore );
        AtomicBroadcastContextImpl snapshotAtomicBroadcastContext =
                atomicBroadcastContext.snapshot( commonStateSnapshot, logging, timeouts, executor, snapshotHeartbeatContext );

        snapshotHeartbeatContext.setCircularDependencies( snapshotClusterContext, snapshotLearnerContext );

        return new MultiPaxosContext( snapshotProposerContext, snapshotAcceptorContext, snapshotLearnerContext,
                snapshotHeartbeatContext, snapshotElectionContext, snapshotAtomicBroadcastContext, commonStateSnapshot,
                paxosInstancesSnapshot, snapshotClusterContext

        );
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

        MultiPaxosContext that = (MultiPaxosContext) o;

        if ( !acceptorContext.equals( that.acceptorContext ) )
        {
            return false;
        }
        if ( !atomicBroadcastContext.equals( that.atomicBroadcastContext ) )
        {
            return false;
        }
        if ( !clusterContext.equals( that.clusterContext ) )
        {
            return false;
        }
        if ( !commonState.equals( that.commonState ) )
        {
            return false;
        }
        if ( !electionContext.equals( that.electionContext ) )
        {
            return false;
        }
        if ( !heartbeatContext.equals( that.heartbeatContext ) )
        {
            return false;
        }
        if ( !learnerContext.equals( that.learnerContext ) )
        {
            return false;
        }
        if ( !paxosInstances.equals( that.paxosInstances ) )
        {
            return false;
        }
        if ( !proposerContext.equals( that.proposerContext ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = clusterContext.hashCode();
        result = 31 * result + proposerContext.hashCode();
        result = 31 * result + acceptorContext.hashCode();
        result = 31 * result + learnerContext.hashCode();
        result = 31 * result + heartbeatContext.hashCode();
        result = 31 * result + electionContext.hashCode();
        result = 31 * result + atomicBroadcastContext.hashCode();
        result = 31 * result + commonState.hashCode();
        result = 31 * result + paxosInstances.hashCode();
        return result;
    }
}
