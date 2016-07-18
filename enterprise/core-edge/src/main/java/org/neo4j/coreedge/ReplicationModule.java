/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Supplier;

import org.neo4j.coreedge.raft.ConsensusModule;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.replication.ProgressTrackerImpl;
import org.neo4j.coreedge.raft.replication.RaftReplicator;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTrackerState;
import org.neo4j.coreedge.raft.replication.session.LocalSessionPool;
import org.neo4j.coreedge.raft.replication.tx.ExponentialBackoffStrategy;
import org.neo4j.coreedge.raft.state.DurableStateStorage;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ReplicationModule
{
    private final RaftReplicator replicator;
    private final ProgressTrackerImpl progressTracker;
    private final SessionTracker sessionTracker;

    public ReplicationModule( CoreMember myself, PlatformModule platformModule, Config config, ConsensusModule consensusModule,
            Outbound<CoreMember,RaftMessages.RaftMessage> loggingOutbound, File clusterStateDirectory,
            FileSystemAbstraction fileSystem, Supplier<DatabaseHealth> databaseHealthSupplier, LogProvider logProvider )
    {
        LifeSupport life = platformModule.life;

        DurableStateStorage<GlobalSessionTrackerState> sessionTrackerStorage;
        try
        {
            sessionTrackerStorage = life.add(
                    new DurableStateStorage<>( fileSystem, new File( clusterStateDirectory, "session-tracker-state" ),
                            "session-tracker",
                            new GlobalSessionTrackerState.Marshal( new CoreMember.CoreMemberMarshal() ),
                            config.get( CoreEdgeClusterSettings.global_session_tracker_state_size ),
                            databaseHealthSupplier, logProvider ) );

        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        sessionTracker = new SessionTracker( sessionTrackerStorage );

        GlobalSession myGlobalSession = new GlobalSession( UUID.randomUUID(), myself );
        LocalSessionPool sessionPool = new LocalSessionPool( myGlobalSession );
        progressTracker = new ProgressTrackerImpl( myGlobalSession );

        replicator = new RaftReplicator( consensusModule.raftInstance(), myself,
                loggingOutbound,
                sessionPool, progressTracker,
                new ExponentialBackoffStrategy( 10, SECONDS ) );

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
