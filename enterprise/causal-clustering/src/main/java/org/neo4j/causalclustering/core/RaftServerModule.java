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
package org.neo4j.causalclustering.core;

import java.util.function.Function;

import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.core.consensus.ConsensusModule;
import org.neo4j.causalclustering.core.consensus.ContinuousJob;
import org.neo4j.causalclustering.core.consensus.LeaderAvailabilityHandler;
import org.neo4j.causalclustering.core.consensus.RaftMessageMonitoringHandler;
import org.neo4j.causalclustering.core.consensus.RaftMessageNettyHandler;
import org.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
import org.neo4j.causalclustering.core.consensus.RaftProtocolServerInstaller;
import org.neo4j.causalclustering.core.consensus.RaftServer;
import org.neo4j.causalclustering.core.server.CoreServerModule;
import org.neo4j.causalclustering.core.state.RaftMessageApplier;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.logging.MessageLogger;
import org.neo4j.causalclustering.messaging.ComposableMessageHandler;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.causalclustering.messaging.LoggingInbound;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import org.neo4j.causalclustering.protocol.handshake.ProtocolRepository;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.util.Collections.singletonList;

class RaftServerModule
{
    private final PlatformModule platformModule;
    private final ConsensusModule consensusModule;
    private final IdentityModule identityModule;
    private final LocalDatabase localDatabase;
    private final MessageLogger<MemberId> messageLogger;
    private final LogProvider logProvider;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final TopologyService topologyService;
    private final RaftServer raftServer;

    RaftServerModule( PlatformModule platformModule, ConsensusModule consensusModule, IdentityModule identityModule, CoreServerModule coreServerModule,
            LocalDatabase localDatabase, NettyPipelineBuilderFactory pipelineBuilderFactory, MessageLogger<MemberId> messageLogger,
            CoreTopologyService topologyService )
    {
        this.platformModule = platformModule;
        this.consensusModule = consensusModule;
        this.identityModule = identityModule;
        this.localDatabase = localDatabase;
        this.messageLogger = messageLogger;
        this.logProvider = platformModule.logging.getInternalLogProvider();
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.topologyService = topologyService;

        LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> messageHandlerChain = createMessageHandlerChain( coreServerModule );

        raftServer = createRaftServer( coreServerModule, messageHandlerChain );
    }

    private RaftServer createRaftServer( CoreServerModule coreServerModule,
            LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> messageHandlerChain )
    {
        ProtocolRepository protocolRepository = new ProtocolRepository( Protocol.Protocols.values() );

        RaftMessageNettyHandler nettyHandler = new RaftMessageNettyHandler( logProvider );
        RaftProtocolServerInstaller raftProtocolServerInstaller = new RaftProtocolServerInstaller( nettyHandler, pipelineBuilderFactory, logProvider );
        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository =
                new ProtocolInstallerRepository<>( singletonList( raftProtocolServerInstaller ) );

        HandshakeServerInitializer handshakeServerInitializer = new HandshakeServerInitializer( logProvider, protocolRepository, Protocol.Identifier.RAFT,
                protocolInstallerRepository, pipelineBuilderFactory );
        RaftServer raftServer = new RaftServer( handshakeServerInitializer, platformModule.config,
                logProvider, platformModule.logging.getUserLogProvider() );

        LoggingInbound<ReceivedInstantClusterIdAwareMessage<?>> loggingRaftInbound =
                new LoggingInbound<>( nettyHandler, messageLogger, identityModule.myself() );
        loggingRaftInbound.registerHandler( messageHandlerChain );

        platformModule.life.add( raftServer ); // must start before core state so that it can trigger snapshot downloads when necessary
        platformModule.life.add( coreServerModule.createCoreLife( messageHandlerChain ) );
        platformModule.life.add( coreServerModule.catchupServer() ); // must start last and stop first, since it handles external requests
        platformModule.life.add( coreServerModule.downloadService() );

        return raftServer;
    }

    private LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> createMessageHandlerChain( CoreServerModule coreServerModule )
    {
        RaftMessageApplier messageApplier =
                new RaftMessageApplier( localDatabase, logProvider, consensusModule.raftMachine(), coreServerModule.downloadService(),
                        coreServerModule.commandApplicationProcess(), topologyService );

        ComposableMessageHandler monitoringHandler = RaftMessageMonitoringHandler.composable( platformModule.clock, platformModule.monitors );

        int queueSize = platformModule.config.get( CausalClusteringSettings.raft_in_queue_size );
        int maxBatch = platformModule.config.get( CausalClusteringSettings.raft_in_queue_max_batch );
        Function<Runnable, ContinuousJob> jobFactory = runnable ->
                new ContinuousJob( platformModule.jobScheduler.threadFactory( new JobScheduler.Group( "raft-batch-handler" ) ), runnable, logProvider );
        ComposableMessageHandler batchingMessageHandler = BatchingMessageHandler.composable( queueSize, maxBatch, jobFactory, logProvider );

        ComposableMessageHandler leaderAvailabilityHandler =
                LeaderAvailabilityHandler.composable( consensusModule.getLeaderAvailabilityTimers(), consensusModule.raftMachine()::term );

        ComposableMessageHandler clusterBindingHandler = ClusterBindingHandler.composable( logProvider );

        return clusterBindingHandler
                .compose( leaderAvailabilityHandler )
                .compose( batchingMessageHandler )
                .compose( monitoringHandler )
                .apply( messageApplier );
    }

    public RaftServer raftServer()
    {
        return raftServer;
    }
}
