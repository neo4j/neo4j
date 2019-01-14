/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering;

import java.io.File;
import java.time.Duration;
import java.util.UUID;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.ConsensusModule;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.replication.ProgressTrackerImpl;
import org.neo4j.causalclustering.core.replication.RaftReplicator;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import org.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import org.neo4j.causalclustering.helper.ConstantTimeTimeoutStrategy;
import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;

public class ReplicationModule
{
    public static final String SESSION_TRACKER_NAME = "session-tracker";

    private final RaftReplicator replicator;
    private final ProgressTrackerImpl progressTracker;
    private final SessionTracker sessionTracker;

    public ReplicationModule( MemberId myself, PlatformModule platformModule, Config config,
            ConsensusModule consensusModule, Outbound<MemberId,RaftMessages.RaftMessage> outbound,
            File clusterStateDirectory, FileSystemAbstraction fileSystem, LogProvider logProvider )
    {
        LifeSupport life = platformModule.life;

        DurableStateStorage<GlobalSessionTrackerState> sessionTrackerStorage;
        sessionTrackerStorage = life.add( new DurableStateStorage<>( fileSystem, clusterStateDirectory,
                SESSION_TRACKER_NAME, new GlobalSessionTrackerState.Marshal( new MemberId.Marshal() ),
                config.get( CausalClusteringSettings.global_session_tracker_state_size ), logProvider ) );

        sessionTracker = new SessionTracker( sessionTrackerStorage );

        GlobalSession myGlobalSession = new GlobalSession( UUID.randomUUID(), myself );
        LocalSessionPool sessionPool = new LocalSessionPool( myGlobalSession );
        progressTracker = new ProgressTrackerImpl( myGlobalSession );

        long replicationLimit = config.get( CausalClusteringSettings.replication_total_size_limit );
        Duration initialBackoff = config.get( CausalClusteringSettings.replication_retry_timeout_base );
        Duration upperBoundBackoff = config.get( CausalClusteringSettings.replication_retry_timeout_limit );
        Duration leaderBackoff = config.get( CausalClusteringSettings.replication_leader_retry_timeout );

        TimeoutStrategy progressRetryStrategy = new ExponentialBackoffStrategy( initialBackoff, upperBoundBackoff );
        TimeoutStrategy leaderRetryStrategy = new ConstantTimeTimeoutStrategy( leaderBackoff );
        long availabilityTimeoutMillis = config.get( CausalClusteringSettings.replication_retry_timeout_base ).toMillis();
        replicator = new RaftReplicator(
                consensusModule.raftMachine(),
                myself,
                outbound,
                sessionPool,
                progressTracker,
                progressRetryStrategy,
                leaderRetryStrategy,
                availabilityTimeoutMillis,
                platformModule.availabilityGuard, logProvider, replicationLimit, platformModule.monitors );
    }

    public RaftReplicator getReplicator()
    {
        return replicator;
    }

    public ProgressTrackerImpl getProgressTracker()
    {
        return progressTracker;
    }

    public SessionTracker getSessionTracker()
    {
        return sessionTracker;
    }
}
