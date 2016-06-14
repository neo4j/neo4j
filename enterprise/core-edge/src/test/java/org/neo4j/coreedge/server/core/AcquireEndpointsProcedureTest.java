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
package org.neo4j.coreedge.server.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hamcrest.MatcherAssert;
import org.junit.Test;

import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.ReadOnlyTopologyService;
import org.neo4j.coreedge.raft.LeaderLocator;
import org.neo4j.coreedge.raft.NoLeaderFoundException;
import org.neo4j.coreedge.server.BoltAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.NullLogProvider;

import static java.util.stream.Collectors.toList;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.server.core.DiscoverMembersProcedureTest.address;
import static org.neo4j.coreedge.server.core.DiscoverMembersProcedureTest.addresses;
import static org.neo4j.coreedge.server.core.DiscoverMembersProcedureTest.coreMemberAtRaftPort;
import static org.neo4j.helpers.collection.Iterators.asList;

public class AcquireEndpointsProcedureTest
{
    @Test
    public void shouldRecommendTheCoreLeaderForWriteAndEdgeForRead() throws Exception
    {
        // given
        final ReadOnlyTopologyService topologyService = mock( ReadOnlyTopologyService.class );

        Map<CoreMember, BoltAddress> coreMembers = new HashMap<>();
        CoreMember theLeader = coreMemberAtRaftPort( 9000 );
        coreMembers.put( theLeader, address( 0 ) );

        final ClusterTopology clusterTopology = new ClusterTopology( false, coreMembers, addresses( 1 ) );
        when( topologyService.currentTopology() ).thenReturn( clusterTopology );

        LeaderLocator<CoreMember> leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        AcquireEndpointsProcedure procedure = new AcquireEndpointsProcedure( topologyService, leaderLocator,
                NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( procedure.apply( null, new Object[0] ) );

        // then
        MatcherAssert.assertThat( members, containsInAnyOrder(
                new Object[]{address( 0 ).getAdvertisedAddress().toString(), "write"},
                new Object[]{address( 1 ).getAdvertisedAddress().toString(), "read"}
        ) );
    }

    @Test
    public void shouldOnlyRecommendOneReadServerEvenIfMultipleAreAvailable() throws Exception
    {
        // given
        final ReadOnlyTopologyService topologyService = mock( ReadOnlyTopologyService.class );

        Map<CoreMember, BoltAddress> coreMembers = new HashMap<>();
        CoreMember theLeader = coreMemberAtRaftPort( 9000 );
        coreMembers.put( theLeader, address( 0 ) );

        final ClusterTopology clusterTopology = new ClusterTopology( false, coreMembers, addresses( 1, 2, 3 ) );
        when( topologyService.currentTopology() ).thenReturn( clusterTopology );

        LeaderLocator<CoreMember> leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        AcquireEndpointsProcedure procedure = new AcquireEndpointsProcedure( topologyService, leaderLocator,
                NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( procedure.apply( null, new Object[0] ) );

        // then
        assertEquals( 1, members.stream().filter( row -> row[1].equals( "read" ) ).count() );
    }

    @Test
    public void shouldReturnCoreServerAsReadServerIfNoEdgeServersAvailable() throws Exception
    {
        // given
        final ReadOnlyTopologyService topologyService = mock( ReadOnlyTopologyService.class );

        Map<CoreMember, BoltAddress> coreMembers = new HashMap<>();
        CoreMember theLeader = coreMemberAtRaftPort( 9000 );
        coreMembers.put( theLeader, address( 0 ) );
        final ClusterTopology clusterTopology = new ClusterTopology( false, coreMembers, addresses() );

        when( topologyService.currentTopology() ).thenReturn( clusterTopology );

        LeaderLocator<CoreMember> leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        AcquireEndpointsProcedure procedure = new AcquireEndpointsProcedure( topologyService, leaderLocator,
                NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( procedure.apply( null, new Object[0] ) );

        // then
        List<Object[]> readAddresses = members.stream().filter( row -> row[1].equals( "read" ) ).collect( toList() );

        assertEquals( 1, readAddresses.size() );
        assertArrayEquals( readAddresses.get( 0 ), new Object[]{address( 0 ).getAdvertisedAddress().toString(), "read"} );
    }

    @Test
    public void shouldThrowExceptionIfThereIsNoLeader() throws Exception
    {
        // given
        LeaderLocator<CoreMember> leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenThrow( NoLeaderFoundException.class );

        AcquireEndpointsProcedure procedure = new AcquireEndpointsProcedure(
                mock( ReadOnlyTopologyService.class ), leaderLocator, NullLogProvider.getInstance() );

        // when
        try
        {
            procedure.apply( null, new Object[]{"bam"} );
        }
        catch ( ProcedureException e )
        {
            assertEquals( Status.Cluster.NoLeader, e.status() );
        }
    }
}
