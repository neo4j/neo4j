/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.diagnostics;

import java.time.Duration;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.consensus.membership.MembershipWaiter;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader;
import org.neo4j.causalclustering.discovery.HazelcastCoreTopologyService;
import org.neo4j.causalclustering.helper.Limiters;
import org.neo4j.causalclustering.identity.ClusterBinder;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

/**
 * Monitors major clustering events and logs them appropriately. The main intention
 * is for this class to make sure that the neo4j.log gets the most important events
 * logged in a way that is useful for end users and aligned across components.
 * <p>
 * In particular the startup should be logged in a way as to aid in debugging
 * common issues; e.g. around network connectivity.
 * <p>
 * This pattern also de-clutters implementing classes from specifics of logging (e.g.
 * formatting, dual-logging, rate limiting, ...) and encourages a structured interface.
 */
public class CoreMonitor implements ClusterBinder.Monitor, HazelcastCoreTopologyService.Monitor,
        PersistentSnapshotDownloader.Monitor, MembershipWaiter.Monitor
{
    private final Log debug;
    private final Log user;

    private final Consumer<Runnable> binderLimit = Limiters.rateLimiter( Duration.ofSeconds( 10 ) );
    private final Consumer<Runnable> waiterLimit = Limiters.rateLimiter( Duration.ofSeconds( 10 ) );

    public static void register( LogProvider debugLogProvider, LogProvider userLogProvider, Monitors monitors )
    {
        new CoreMonitor( debugLogProvider, userLogProvider, monitors );
    }

    private CoreMonitor( LogProvider debugLogProvider, LogProvider userLogProvider, Monitors monitors )
    {
        this.debug = debugLogProvider.getLog( getClass() );
        this.user = userLogProvider.getLog( getClass() );

        monitors.addMonitorListener( this );
    }

    @Override
    public void waitingForCoreMembers( int minimumCount )
    {
        binderLimit.accept( () -> {
            String message = "Waiting for a total of %d core members...";
            user.info( format( message, minimumCount ) );
        } );
    }

    @Override
    public void waitingForBootstrap()
    {
        binderLimit.accept( () -> user.info( "Waiting for bootstrap by other instance..." ) );
    }

    @Override
    public void bootstrapped( CoreSnapshot snapshot, ClusterId clusterId )
    {
        user.info( "This instance bootstrapped the cluster." );
        debug.info( format( "Bootstrapped with snapshot: %s and clusterId: %s", snapshot, clusterId ) );
    }

    @Override
    public void boundToCluster( ClusterId clusterId )
    {
        user.info( "Bound to cluster with id " + clusterId.uuid() );
    }

    @Override
    public void discoveredMember( SocketAddress socketAddress )
    {
        user.info( "Discovered core member at " + socketAddress );
    }

    @Override
    public void lostMember( SocketAddress socketAddress )
    {
        user.warn( "Lost core member at " + socketAddress );
    }

    @Override
    public void startedDownloadingSnapshot()
    {
        user.info( "Started downloading snapshot..." );
    }

    @Override
    public void downloadSnapshotComplete()
    {
        user.info( "Download of snapshot complete." );
    }

    @Override
    public void waitingToHearFromLeader()
    {
        waiterLimit.accept( () -> user.info( "Waiting to hear from leader..." ) );
    }

    @Override
    public void waitingToCatchupWithLeader( long localCommitIndex, long leaderCommitIndex )
    {
        waiterLimit.accept( () -> {
            long gap = leaderCommitIndex - localCommitIndex;
            user.info( "Waiting to catchup with leader... we are %d entries behind leader at %d.", gap,
                    leaderCommitIndex );
        } );
    }

    @Override
    public void joinedRaftGroup()
    {
        user.info( "Successfully joined the Raft group." );
    }
}
