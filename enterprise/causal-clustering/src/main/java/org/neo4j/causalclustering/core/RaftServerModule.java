/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
import org.neo4j.causalclustering.core.consensus.RaftServer;
import org.neo4j.causalclustering.core.server.CoreServerModule;
import org.neo4j.causalclustering.core.state.RaftMessageApplier;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppender;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.logging.MessageLogger;
import org.neo4j.causalclustering.messaging.ComposableMessageHandler;
import org.neo4j.causalclustering.messaging.CoreReplicatedContentMarshal;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.causalclustering.messaging.LoggingInbound;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public class RaftServerModule
{
    private final PlatformModule platformModule;
    private final ConsensusModule consensusModule;
    private final IdentityModule identityModule;
    private final LocalDatabase localDatabase;
    private final Monitors monitors;
    private final MessageLogger<MemberId> messageLogger;
    private final LogProvider logProvider;
    private final PipelineHandlerAppender pipelineHandlerAppender;

    RaftServerModule( PlatformModule platformModule, ConsensusModule consensusModule, IdentityModule identityModule, CoreServerModule coreServerModule,
            LocalDatabase localDatabase, PipelineHandlerAppender pipelineHandlerAppender, Monitors monitors, MessageLogger<MemberId> messageLogger )
    {
        this.platformModule = platformModule;
        this.consensusModule = consensusModule;
        this.identityModule = identityModule;
        this.localDatabase = localDatabase;
        this.monitors = monitors;
        this.messageLogger = messageLogger;
        this.logProvider = platformModule.logging.getInternalLogProvider();
        this.pipelineHandlerAppender = pipelineHandlerAppender;

        LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage> messageHandlerChain = createMessageHandlerChain( coreServerModule );

        createRaftServer( coreServerModule, messageHandlerChain );
    }

    private void createRaftServer( CoreServerModule coreServerModule, LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage> messageHandlerChain )
    {
        RaftServer raftServer = new RaftServer( new CoreReplicatedContentMarshal(), pipelineHandlerAppender, platformModule.config, logProvider,
                platformModule.logging.getUserLogProvider(), monitors, platformModule.clock );

        LoggingInbound<ReceivedInstantClusterIdAwareMessage> loggingRaftInbound = new LoggingInbound<>( raftServer,
                messageLogger, identityModule.myself() );
        loggingRaftInbound.registerHandler( messageHandlerChain );

        platformModule.life.add( raftServer ); // must start before core state so that it can trigger snapshot downloads when necessary
        platformModule.life.add( coreServerModule.createCoreLife( messageHandlerChain ) );
        platformModule.life.add( coreServerModule.catchupServer() ); // must start last and stop first, since it handles external requests
        platformModule.life.add( coreServerModule.downloadService() );
    }

    private LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage> createMessageHandlerChain( CoreServerModule coreServerModule )
    {
        RaftMessageApplier messageApplier = new RaftMessageApplier( localDatabase, logProvider,
                consensusModule.raftMachine(), coreServerModule.downloadService(), coreServerModule.commandApplicationProcess() );

        ComposableMessageHandler monitoringHandler = RaftMessageMonitoringHandler.composable( platformModule.clock, platformModule.monitors );

        int queueSize = platformModule.config.get( CausalClusteringSettings.raft_in_queue_size );
        int maxBatch = platformModule.config.get( CausalClusteringSettings.raft_in_queue_max_batch );
        Function<Runnable, ContinuousJob> jobFactory = ( Runnable runnable ) ->
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
