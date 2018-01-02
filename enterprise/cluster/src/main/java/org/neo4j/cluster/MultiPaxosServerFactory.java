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
package org.neo4j.cluster;

import java.net.URI;
import java.util.concurrent.Executor;

import org.neo4j.cluster.com.message.MessageSender;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorState;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastState;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerState;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerState;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context.MultiPaxosContext;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.protocol.cluster.ClusterState;
import org.neo4j.cluster.protocol.election.ClusterLeaveReelectionListener;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.ElectionMessage;
import org.neo4j.cluster.protocol.election.ElectionRole;
import org.neo4j.cluster.protocol.election.ElectionState;
import org.neo4j.cluster.protocol.election.HeartbeatReelectionListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatIAmAliveProcessor;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatJoinListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatLeftListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatMessage;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatRefreshProcessor;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatState;
import org.neo4j.cluster.protocol.snapshot.SnapshotContext;
import org.neo4j.cluster.protocol.snapshot.SnapshotMessage;
import org.neo4j.cluster.protocol.snapshot.SnapshotState;
import org.neo4j.cluster.statemachine.StateMachine;
import org.neo4j.cluster.statemachine.StateMachineRules;
import org.neo4j.cluster.timeout.TimeoutStrategy;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.LogProvider;

import static org.neo4j.cluster.com.message.Message.internal;

/**
 * Factory for MultiPaxos {@link ProtocolServer}s.
 */
public class MultiPaxosServerFactory
        implements ProtocolServerFactory
{
    private final ClusterConfiguration initialConfig;
    private final LogProvider logging;
    private StateMachines.Monitor stateMachinesMonitor;

    public MultiPaxosServerFactory( ClusterConfiguration initialConfig, LogProvider logging, StateMachines.Monitor stateMachinesMonitor )
    {
        this.initialConfig = initialConfig;
        this.logging = logging;
        this.stateMachinesMonitor = stateMachinesMonitor;
    }

    @Override
    public ProtocolServer newProtocolServer( InstanceId me, int maxAcceptors,
                                             TimeoutStrategy timeoutStrategy, MessageSource input,
                                             MessageSender output, AcceptorInstanceStore acceptorInstanceStore,
                                             ElectionCredentialsProvider electionCredentialsProvider,
                                             Executor stateMachineExecutor,
                                             ObjectInputStreamFactory objectInputStreamFactory,
                                             ObjectOutputStreamFactory objectOutputStreamFactory )
    {
        DelayedDirectExecutor executor = new DelayedDirectExecutor( logging );

        // Create state machines
        Timeouts timeouts = new Timeouts( timeoutStrategy );

        final MultiPaxosContext context = new MultiPaxosContext( me, maxAcceptors,
                Iterables.<ElectionRole, ElectionRole>iterable( new ElectionRole( ClusterConfiguration.COORDINATOR ) ),
                new ClusterConfiguration( initialConfig.getName(), logging,
                        initialConfig.getMemberURIs() ),
                executor, logging, objectInputStreamFactory, objectOutputStreamFactory, acceptorInstanceStore, timeouts,
                electionCredentialsProvider
        );

        SnapshotContext snapshotContext = new SnapshotContext( context.getClusterContext(),
                context.getLearnerContext() );

        return newProtocolServer( me, input, output, stateMachineExecutor, executor, timeouts,
                context, snapshotContext );
    }

    public ProtocolServer newProtocolServer( InstanceId me, MessageSource input, MessageSender output,
                                             Executor stateMachineExecutor, DelayedDirectExecutor executor,
                                             Timeouts timeouts,
                                             MultiPaxosContext context, SnapshotContext snapshotContext )
    {
        return constructSupportingInfrastructureFor( me, input, output, executor, timeouts, stateMachineExecutor,
                context, new StateMachine[]
                {
                        new StateMachine( context.getAtomicBroadcastContext(), AtomicBroadcastMessage.class,
                                AtomicBroadcastState.start, logging ),
                        new StateMachine( context.getAcceptorContext(), AcceptorMessage.class, AcceptorState.start,
                                logging ),
                        new StateMachine( context.getProposerContext(), ProposerMessage.class, ProposerState.start,
                                logging ),
                        new StateMachine( context.getLearnerContext(), LearnerMessage.class, LearnerState.start,
                                logging ),
                        new StateMachine( context.getHeartbeatContext(), HeartbeatMessage.class, HeartbeatState.start,
                                logging ),
                        new StateMachine( context.getElectionContext(), ElectionMessage.class, ElectionState.start,
                                logging ),
                        new StateMachine( snapshotContext, SnapshotMessage.class, SnapshotState.start, logging ),
                        new StateMachine( context.getClusterContext(), ClusterMessage.class, ClusterState.start,
                                logging )
                } );
    }

    /**
     * Sets up the supporting infrastructure and communication hooks for our state machines. This is here to support
     * an external requirement for assembling protocol servers given an existing set of state machines (used to prove
     * correctness).
     */
    public ProtocolServer constructSupportingInfrastructureFor( InstanceId me, MessageSource input,
                                                                MessageSender output, DelayedDirectExecutor executor,
                                                                Timeouts timeouts,
                                                                Executor stateMachineExecutor,
                                                                final MultiPaxosContext context,
                                                                StateMachine[] machines )
    {
        StateMachines stateMachines = new StateMachines( logging, stateMachinesMonitor, input,
                output, timeouts, executor, stateMachineExecutor, me );

        for ( StateMachine machine : machines )
        {
            stateMachines.addStateMachine( machine );
        }

        final ProtocolServer server = new ProtocolServer( me, stateMachines, logging );

        server.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                context.getClusterContext().setBoundAt( me );
            }
        } );

        stateMachines.addMessageProcessor( new HeartbeatRefreshProcessor( stateMachines.getOutgoing
                (), context.getClusterContext() ) );
        input.addMessageProcessor( new HeartbeatIAmAliveProcessor( stateMachines.getOutgoing(),
                context.getClusterContext() ) );

        Cluster cluster = server.newClient( Cluster.class );
        cluster.addClusterListener( new HeartbeatJoinListener( stateMachines.getOutgoing() ) );
        cluster.addClusterListener( new HeartbeatLeftListener( context.getHeartbeatContext(), logging ) );

        context.getHeartbeatContext().addHeartbeatListener( new HeartbeatReelectionListener(
                server.newClient( Election.class ), logging ) );
        context.getClusterContext().addClusterListener( new ClusterLeaveReelectionListener( server.newClient(
                Election.class ),
                logging
        ) );

        StateMachineRules rules = new StateMachineRules( stateMachines.getOutgoing() )
                .rule( ClusterState.start, ClusterMessage.create, ClusterState.entered,
                        internal( AtomicBroadcastMessage.entered ),
                        internal( ProposerMessage.join ),
                        internal( AcceptorMessage.join ),
                        internal( LearnerMessage.join ),
                        internal( HeartbeatMessage.join ),
                        internal( ElectionMessage.created ),
                        internal( SnapshotMessage.join ) )

                .rule( ClusterState.discovery, ClusterMessage.configurationResponse, ClusterState.joining,
                        internal( AcceptorMessage.join ),
                        internal( LearnerMessage.join ),
                        internal( AtomicBroadcastMessage.join ) )

                .rule( ClusterState.discovery, ClusterMessage.configurationResponse, ClusterState.entered,
                        internal( AtomicBroadcastMessage.entered ),
                        internal( ProposerMessage.join ),
                        internal( AcceptorMessage.join ),
                        internal( LearnerMessage.join ),
                        internal( HeartbeatMessage.join ),
                        internal( ElectionMessage.join ),
                        internal( SnapshotMessage.join ) )

                .rule( ClusterState.joining, ClusterMessage.configurationChanged, ClusterState.entered,
                        internal( AtomicBroadcastMessage.entered ),
                        internal( ProposerMessage.join ),
                        internal( AcceptorMessage.join ),
                        internal( LearnerMessage.join ),
                        internal( HeartbeatMessage.join ),
                        internal( ElectionMessage.join ),
                        internal( SnapshotMessage.join ) )

                .rule( ClusterState.joining, ClusterMessage.joinFailure, ClusterState.start,
                        internal( AtomicBroadcastMessage.leave ),
                        internal( AcceptorMessage.leave ),
                        internal( LearnerMessage.leave ),
                        internal( ProposerMessage.leave ) )

                .rule( ClusterState.entered, ClusterMessage.leave, ClusterState.start,
                        internal( AtomicBroadcastMessage.leave ),
                        internal( AcceptorMessage.leave ),
                        internal( LearnerMessage.leave ),
                        internal( HeartbeatMessage.leave ),
                        internal( SnapshotMessage.leave ),
                        internal( ElectionMessage.leave ),
                        internal( ProposerMessage.leave ) )

                .rule( ClusterState.entered, ClusterMessage.leave, ClusterState.start,
                        internal( AtomicBroadcastMessage.leave ),
                        internal( AcceptorMessage.leave ),
                        internal( LearnerMessage.leave ),
                        internal( HeartbeatMessage.leave ),
                        internal( ElectionMessage.leave ),
                        internal( SnapshotMessage.leave ),
                        internal( ProposerMessage.leave ) )

                .rule( ClusterState.leaving, ClusterMessage.configurationChanged, ClusterState.start,
                        internal( AtomicBroadcastMessage.leave ),
                        internal( AcceptorMessage.leave ),
                        internal( LearnerMessage.leave ),
                        internal( HeartbeatMessage.leave ),
                        internal( ElectionMessage.leave ),
                        internal( SnapshotMessage.leave ),
                        internal( ProposerMessage.leave ) )

                .rule( ClusterState.leaving, ClusterMessage.leaveTimedout, ClusterState.start,
                        internal( AtomicBroadcastMessage.leave ),
                        internal( AcceptorMessage.leave ),
                        internal( LearnerMessage.leave ),
                        internal( HeartbeatMessage.leave ),
                        internal( ElectionMessage.leave ),
                        internal( SnapshotMessage.leave ),
                        internal( ProposerMessage.leave ) );


        stateMachines.addStateTransitionListener( rules );

        return server;
    }
}
