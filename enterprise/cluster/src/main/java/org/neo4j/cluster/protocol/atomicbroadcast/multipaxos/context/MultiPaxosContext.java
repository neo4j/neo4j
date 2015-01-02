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
import org.neo4j.cluster.protocol.election.ElectionRole;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.kernel.logging.Logging;

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

    public MultiPaxosContext( org.neo4j.cluster.InstanceId me,
                              Iterable<ElectionRole> roles,
                              ClusterConfiguration configuration,
                              Executor executor,
                              Logging logging,
                              ObjectInputStreamFactory objectInputStreamFactory,
                              ObjectOutputStreamFactory objectOutputStreamFactory,
                              AcceptorInstanceStore instanceStore,
                              Timeouts timeouts
    )
    {
        commonState = new CommonContextState(configuration);
        paxosInstances = new PaxosInstanceStore();

        heartbeatContext = new HeartbeatContextImpl(me, commonState, logging, timeouts, executor );
        learnerContext = new LearnerContextImpl(me, commonState, logging, timeouts, paxosInstances, instanceStore, objectInputStreamFactory, objectOutputStreamFactory, heartbeatContext );
        clusterContext = new ClusterContextImpl(me, commonState, logging, timeouts, executor, objectOutputStreamFactory, objectInputStreamFactory, learnerContext, heartbeatContext );
        electionContext = new ElectionContextImpl( me, commonState, logging, timeouts, roles, clusterContext, heartbeatContext );
        proposerContext = new ProposerContextImpl(me, commonState, logging, timeouts, paxosInstances );
        acceptorContext = new AcceptorContextImpl(me, commonState, logging, timeouts, instanceStore);
        atomicBroadcastContext = new AtomicBroadcastContextImpl(me, commonState, logging, timeouts, executor);

        heartbeatContext.setCircularDependencies( clusterContext, learnerContext );
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
}
