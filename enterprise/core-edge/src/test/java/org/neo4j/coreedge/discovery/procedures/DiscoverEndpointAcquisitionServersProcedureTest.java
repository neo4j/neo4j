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

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.CoreAddresses;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.EdgeAddresses;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.coreedge.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asList;

public class DiscoverEndpointAcquisitionServersProcedureTest
{
    private ClusterId clusterId = new ClusterId( UUID.randomUUID() );

    @Test
    public void shouldOnlyReturnCoreMembers() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );
        coreMembers.put( member( 1 ), coreAddresses( 1 ) );
        coreMembers.put( member( 2 ), coreAddresses( 2 ) );

        final ClusterTopology clusterTopology = new ClusterTopology( clusterId, false, coreMembers, addresses( 3, 4, 5 ) );
        when( coreTopologyService.currentTopology() ).thenReturn( clusterTopology );

        final DiscoverEndpointAcquisitionServersProcedure proc =
                new DiscoverEndpointAcquisitionServersProcedure( coreTopologyService, NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( proc.apply( null, new Object[0] ) );

        // then
        assertThat( members, containsInAnyOrder(
                new Object[]{coreAddresses( 0 ).getRaftServer().toString()},
                new Object[]{coreAddresses( 1 ).getRaftServer().toString()},
                new Object[]{coreAddresses( 2 ).getRaftServer().toString()} )
        );
    }

    @Test
    public void shouldReturnSelfIfOnlyMemberOfTheCluster() throws Exception
    {
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );

        final ClusterTopology clusterTopology = new ClusterTopology( clusterId, false, coreMembers, addresses( 3, 4, 5 ) );
        when( coreTopologyService.currentTopology() ).thenReturn( clusterTopology );
        final DiscoverEndpointAcquisitionServersProcedure proc =
                new DiscoverEndpointAcquisitionServersProcedure( coreTopologyService, NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( proc.apply( null, new Object[0] ) );

        // then
        assertArrayEquals( members.get( 0 ), new Object[]{coreAddresses( 0 ).getRaftServer().toString()} );
    }

    @Test
    public void shouldReturnLimitedNumberOfAddresses() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );
        coreMembers.put( member( 1 ), coreAddresses( 1 ) );
        coreMembers.put( member( 2 ), coreAddresses( 2 ) );

        final ClusterTopology clusterTopology = new ClusterTopology( clusterId, false, coreMembers, addresses( 3, 4, 5) );
        when( coreTopologyService.currentTopology() ).thenReturn( clusterTopology );

        final DiscoverEndpointAcquisitionServersProcedure proc =
                new DiscoverEndpointAcquisitionServersProcedure( coreTopologyService, NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( proc.apply( null, new Object[]{1} ) );

        // then
        assertEquals( 1, members.size() );
    }

    @Test
    public void shouldReturnAllAddressesWhenLimitInNotNumeric() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );
        coreMembers.put( member( 1 ), coreAddresses( 1 ) );
        coreMembers.put( member( 2 ), coreAddresses( 2 ) );

        final ClusterTopology clusterTopology = new ClusterTopology( clusterId, false, coreMembers, addresses( 3, 4, 5 ) );
        when( coreTopologyService.currentTopology() ).thenReturn( clusterTopology );

        final DiscoverEndpointAcquisitionServersProcedure proc =
                new DiscoverEndpointAcquisitionServersProcedure( coreTopologyService, NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( proc.apply( null, new Object[]{"not numeric"} ) );

        // then
        assertEquals( 3, members.size() );
    }

    static Set<EdgeAddresses> addresses( int... ids )
    {
        return Arrays.stream( ids )
                .mapToObj( DiscoverEndpointAcquisitionServersProcedureTest::edgeAddresses )
                .collect( Collectors.toSet() );
    }

    static CoreAddresses coreAddresses( int id )
    {
        AdvertisedSocketAddress advertisedSocketAddress = new AdvertisedSocketAddress( "localhost:" + (3000 + id) );
        return new CoreAddresses( advertisedSocketAddress, advertisedSocketAddress, advertisedSocketAddress );
    }

    static EdgeAddresses edgeAddresses( int id )
    {
        AdvertisedSocketAddress advertisedSocketAddress = new AdvertisedSocketAddress( "localhost:" + (3000 + id) );
        return new EdgeAddresses( advertisedSocketAddress );
    }
}
