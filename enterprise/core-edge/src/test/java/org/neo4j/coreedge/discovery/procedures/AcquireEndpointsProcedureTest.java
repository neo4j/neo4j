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
package org.neo4j.coreedge.discovery.procedures;

import org.hamcrest.MatcherAssert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.CoreAddresses;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.core.consensus.LeaderLocator;
import org.neo4j.coreedge.core.consensus.NoLeaderFoundException;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.logging.NullLogProvider;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.coreedge.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asList;

public class AcquireEndpointsProcedureTest
{
    private ClusterId clusterId = new ClusterId( UUID.randomUUID() );

    @Test
    public void shouldRecommendTheCoreLeaderForWriteAndEdgeForRead() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        MemberId theLeader = member( 0 );
        coreMembers.put( theLeader, DiscoverEndpointAcquisitionServersProcedureTest.coreAddresses( 0 ) );

        final ClusterTopology clusterTopology = new ClusterTopology( clusterId, false, coreMembers, DiscoverEndpointAcquisitionServersProcedureTest.addresses( 1 ) );
        when( topologyService.currentTopology() ).thenReturn( clusterTopology );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        AcquireEndpointsProcedure procedure = new AcquireEndpointsProcedure( topologyService, leaderLocator,
                NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( procedure.apply( null, new Object[0] ) );

        // then
        MatcherAssert.assertThat( members, containsInAnyOrder(
                new Object[]{DiscoverEndpointAcquisitionServersProcedureTest.coreAddresses( 0 ).getRaftServer().toString(), "WRITE"},
                new Object[]{DiscoverEndpointAcquisitionServersProcedureTest.coreAddresses( 1 ).getRaftServer().toString(), "READ"}
        ) );
    }

    @Test
    public void shouldOnlyRecommendOneReadServerEvenIfMultipleAreAvailable() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId, CoreAddresses> coreMembers = new HashMap<>();
        MemberId theLeader = member( 0 );
        coreMembers.put( theLeader, DiscoverEndpointAcquisitionServersProcedureTest.coreAddresses( 0 ) );

        final ClusterTopology clusterTopology = new ClusterTopology( clusterId, false, coreMembers, DiscoverEndpointAcquisitionServersProcedureTest.addresses( 1, 2, 3 ) );
        when( topologyService.currentTopology() ).thenReturn( clusterTopology );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        AcquireEndpointsProcedure procedure = new AcquireEndpointsProcedure( topologyService, leaderLocator,
                NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( procedure.apply( null, new Object[0] ) );

        // then
        assertEquals( 1, members.stream().filter( row -> row[1].equals( "READ" ) ).count() );
    }

    @Test
    public void shouldReturnCoreMemberAsReadServerIfNoEdgeServersAvailable() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId, CoreAddresses> coreMembers = new HashMap<>();
        MemberId theLeader = member( 0 );
        coreMembers.put( theLeader, DiscoverEndpointAcquisitionServersProcedureTest.coreAddresses( 0 ) );
        final ClusterTopology clusterTopology = new ClusterTopology( clusterId, false, coreMembers, DiscoverEndpointAcquisitionServersProcedureTest.addresses() );

        when( topologyService.currentTopology() ).thenReturn( clusterTopology );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        AcquireEndpointsProcedure procedure = new AcquireEndpointsProcedure( topologyService, leaderLocator,
                NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( procedure.apply( null, new Object[0] ) );

        // then
        assertThat( members, containsInAnyOrder(
                new Object[]{DiscoverEndpointAcquisitionServersProcedureTest.coreAddresses( 0 ).getRaftServer().toString(), "WRITE"},
                new Object[]{DiscoverEndpointAcquisitionServersProcedureTest.coreAddresses( 0 ).getRaftServer().toString(), "READ"}
        ) );
    }

    @Test
    public void shouldReturnNoWriteEndpointsIfThereIsNoLeader() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId, CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), DiscoverEndpointAcquisitionServersProcedureTest.coreAddresses( 0 ) );

        final ClusterTopology clusterTopology = new ClusterTopology( clusterId, false, coreMembers, DiscoverEndpointAcquisitionServersProcedureTest.addresses() );

        when( topologyService.currentTopology() ).thenReturn( clusterTopology );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenThrow( new NoLeaderFoundException() );

        AcquireEndpointsProcedure procedure = new AcquireEndpointsProcedure( topologyService, leaderLocator,
                NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( procedure.apply( null, new Object[0] ) );

        // then
        assertEquals( 1, members.stream().filter( row1 -> row1[1].equals( "READ" ) ).collect( toList() ).size() );
        assertEquals( 0, members.stream().filter( row1 -> row1[1].equals( "WRITE" ) ).collect( toList() ).size() );
    }

    @Test
    public void shouldReturnNoWriteEndpointsIfThereIsNoAddressForTheLeader() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId, CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), DiscoverEndpointAcquisitionServersProcedureTest.coreAddresses( 0 ) );

        final ClusterTopology clusterTopology = new ClusterTopology( clusterId, false, coreMembers, DiscoverEndpointAcquisitionServersProcedureTest.addresses() );

        when( topologyService.currentTopology() ).thenReturn( clusterTopology );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 1 ) );

        AcquireEndpointsProcedure procedure = new AcquireEndpointsProcedure( topologyService, leaderLocator,
                NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( procedure.apply( null, new Object[0] ) );

        // then
        assertEquals( 1, members.stream().filter( row1 -> row1[1].equals( "READ" ) ).collect( toList() ).size() );
        assertEquals( 0, members.stream().filter( row1 -> row1[1].equals( "WRITE" ) ).collect( toList() ).size() );
    }
}
