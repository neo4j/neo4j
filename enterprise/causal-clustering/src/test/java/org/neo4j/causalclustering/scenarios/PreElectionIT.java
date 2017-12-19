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
package org.neo4j.causalclustering.scenarios;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.test.Race;
import org.neo4j.test.assertion.Assert;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class PreElectionIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 )
            .withSharedCoreParam( CausalClusteringSettings.leader_election_timeout, "10s" )
            .withSharedCoreParam( CausalClusteringSettings.enable_pre_voting, "true" );

    private Cluster cluster;

    @Before
    public void setUp() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldActuallyStartAClusterWithPreVoting() throws Exception
    {
        // pass
    }

    @Test
    public void shouldStartAnElectionIfAllServersHaveTimedOutOnHeartbeats() throws Exception
    {
        Collection<CompletableFuture<Void>> futures = new ArrayList<>( cluster.coreMembers().size() );

        // given
        long initialTerm = cluster.awaitLeader().raft().term();

        // when
        for ( CoreClusterMember member : cluster.coreMembers() )
        {
            if ( Role.FOLLOWER == member.raft().currentRole() )
            {
                futures.add( CompletableFuture.runAsync( Race.throwing( () -> member.raft().triggerElection( Clock.systemUTC() ) ) ) );
            }
        }

        // then
        Assert.assertEventually(
                "Should be on a new term following an election",
                () -> cluster.awaitLeader().raft().term(), not( equalTo( initialTerm ) ),
                1,
                TimeUnit.MINUTES );

        // cleanup
        for ( CompletableFuture<Void> future : futures )
        {
            future.cancel( false );
        }
    }

    @Test
    public void shouldNotStartAnElectionIfAMinorityOfServersHaveTimedOutOnHeartbeats() throws Exception
    {
        // given
        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( Role.FOLLOWER, 1, TimeUnit.MINUTES );

        // when
        follower.raft().triggerElection( Clock.systemUTC() );

        // then
        try
        {
            cluster.awaitCoreMemberWithRole( Role.CANDIDATE, 1, TimeUnit.MINUTES );
            fail( "Should not have started an election if less than a quorum have timed out" );
        }
        catch ( TimeoutException e )
        {
            // pass
        }
    }

    @Test
    public void shouldStartElectionIfLeaderRemoved() throws Exception
    {
        // given
        CoreClusterMember oldLeader = cluster.awaitLeader();

        // when
        cluster.removeCoreMember( oldLeader );

        // then
        CoreClusterMember newLeader = cluster.awaitLeader();

        assertThat( newLeader.serverId(), not( equalTo( oldLeader.serverId() ) ) );
    }
}
