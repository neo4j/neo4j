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
package org.neo4j.causalclustering.core;

import io.netty.channel.ChannelInboundHandler;

import java.util.Collection;
import java.util.function.Function;

import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.core.consensus.ConsensusModule;
import org.neo4j.causalclustering.core.consensus.ContinuousJob;
import org.neo4j.causalclustering.core.consensus.LeaderAvailabilityHandler;
import org.neo4j.causalclustering.core.consensus.RaftMessageMonitoringHandler;
import org.neo4j.causalclustering.core.consensus.RaftMessageNettyHandler;
import org.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
import org.neo4j.causalclustering.core.consensus.RaftProtocolServerInstaller;
import org.neo4j.causalclustering.core.server.CoreServerModule;
import org.neo4j.causalclustering.core.state.RaftMessageApplier;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.logging.MessageLogger;
import org.neo4j.causalclustering.messaging.ComposableMessageHandler;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.causalclustering.messaging.LoggingInbound;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import org.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.util.Collections.singletonList;

public class RaftServerModule
{
    public static final String RAFT_SERVER_NAME = "raft-server";

    private final PlatformModule platformModule;
    private final ConsensusModule consensusModule;
    private final MemberIdRepository memberIdRepository;
    private final ApplicationSupportedProtocols supportedApplicationProtocol;
    private final LocalDatabase localDatabase;
    private final MessageLogger<MemberId> messageLogger;
    private final LogProvider logProvider;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private CatchupAddressProvider.PrioritisingUpstreamStrategyBasedAddressProvider catchupAddressProvider;
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols;

    private RaftServerModule( PlatformModule platformModule, ConsensusModule consensusModule, MemberIdRepository memberIdRepository,
            CoreServerModule coreServerModule, LocalDatabase localDatabase, NettyPipelineBuilderFactory pipelineBuilderFactory,
            MessageLogger<MemberId> messageLogger, CatchupAddressProvider.PrioritisingUpstreamStrategyBasedAddressProvider catchupAddressProvider,
            ApplicationSupportedProtocols supportedApplicationProtocol,
            Collection<ModifierSupportedProtocols> supportedModifierProtocols, ChannelInboundHandler installedProtocolsHandler )
    {
        this.platformModule = platformModule;
        this.consensusModule = consensusModule;
        this.memberIdRepository = memberIdRepository;
        this.supportedApplicationProtocol = supportedApplicationProtocol;
        this.localDatabase = localDatabase;
        this.messageLogger = messageLogger;
        this.logProvider = platformModule.logging.getInternalLogProvider();
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.catchupAddressProvider = catchupAddressProvider;
        this.supportedModifierProtocols = supportedModifierProtocols;

        LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> messageHandlerChain = createMessageHandlerChain( coreServerModule );

        createRaftServer( coreServerModule, messageHandlerChain, installedProtocolsHandler );
    }

    static void createAndStart( PlatformModule platformModule, ConsensusModule consensusModule, MemberIdRepository memberIdRepository,
            CoreServerModule coreServerModule, LocalDatabase localDatabase, NettyPipelineBuilderFactory pipelineBuilderFactory,
            MessageLogger<MemberId> messageLogger, CatchupAddressProvider.PrioritisingUpstreamStrategyBasedAddressProvider addressProvider,
            ApplicationSupportedProtocols supportedApplicationProtocol,
            Collection<ModifierSupportedProtocols> supportedModifierProtocols, ChannelInboundHandler installedProtocolsHandler )
    {
        new RaftServerModule( platformModule, consensusModule, memberIdRepository, coreServerModule, localDatabase, pipelineBuilderFactory, messageLogger,
                        addressProvider, supportedApplicationProtocol, supportedModifierProtocols, installedProtocolsHandler );
    }

    private void createRaftServer( CoreServerModule coreServerModule, LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> messageHandlerChain,
            ChannelInboundHandler installedProtocolsHandler )
    {
        ApplicationProtocolRepository applicationProtocolRepository =
                new ApplicationProtocolRepository( Protocol.ApplicationProtocols.values(), supportedApplicationProtocol );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( Protocol.ModifierProtocols.values(), supportedModifierProtocols );

        RaftMessageNettyHandler nettyHandler = new RaftMessageNettyHandler( logProvider );
        RaftProtocolServerInstaller.Factory raftProtocolServerInstaller =
                new RaftProtocolServerInstaller.Factory( nettyHandler, pipelineBuilderFactory, logProvider );
        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository =
                new ProtocolInstallerRepository<>( singletonList( raftProtocolServerInstaller ), ModifierProtocolInstaller.allServerInstallers );

        HandshakeServerInitializer handshakeServerInitializer = new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                protocolInstallerRepository, pipelineBuilderFactory, logProvider );

        ListenSocketAddress raftListenAddress = platformModule.config.get( CausalClusteringSettings.raft_listen_address );
        Server raftServer = new Server( handshakeServerInitializer, installedProtocolsHandler, logProvider, platformModule.logging.getUserLogProvider(),
                raftListenAddress, RAFT_SERVER_NAME );
        platformModule.dependencies.satisfyDependency( raftServer ); // resolved in tests

        LoggingInbound<ReceivedInstantClusterIdAwareMessage<?>> loggingRaftInbound =
                new LoggingInbound<>( nettyHandler, messageLogger, memberIdRepository.myself() );
        loggingRaftInbound.registerHandler( messageHandlerChain );

        platformModule.life.add( raftServer ); // must start before core state so that it can trigger snapshot downloads when necessary
        platformModule.life.add( coreServerModule.createCoreLife( messageHandlerChain ) );
        platformModule.life.add( coreServerModule.catchupServer() ); // must start last and stop first, since it handles external requests
        coreServerModule.backupServer().ifPresent( platformModule.life::add );
        platformModule.life.add( coreServerModule.downloadService() );
    }

    private LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> createMessageHandlerChain( CoreServerModule coreServerModule )
    {
        RaftMessageApplier messageApplier =
                new RaftMessageApplier( localDatabase, logProvider, consensusModule.raftMachine(), coreServerModule.downloadService(),
                        coreServerModule.commandApplicationProcess(), catchupAddressProvider );

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
}
