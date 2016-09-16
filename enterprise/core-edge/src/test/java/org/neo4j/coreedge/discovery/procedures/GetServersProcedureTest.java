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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import org.neo4j.collection.RawIterator;
import org.neo4j.coreedge.core.consensus.LeaderLocator;
import org.neo4j.coreedge.core.consensus.NoLeaderFoundException;
import org.neo4j.coreedge.discovery.CoreAddresses;
import org.neo4j.coreedge.discovery.CoreTopology;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.EdgeAddresses;
import org.neo4j.coreedge.discovery.EdgeTopology;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.logging.NullLogProvider;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class GetServersProcedureTest
{
    private ClusterId clusterId = new ClusterId( UUID.randomUUID() );

    @Test
    public void shouldReturnCoreServersWithReadRouteAndSingleWriteActions() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 0 ) );

        Map<MemberId, CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );
        coreMembers.put( member( 1 ), coreAddresses( 1 ) );
        coreMembers.put( member( 2 ), coreAddresses( 2 ) );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.coreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, emptySet() ) );

        final GetServersProcedure proc =
                new GetServersProcedure( coreTopologyService, leaderLocator, NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( proc.apply( null, new Object[0] ) );

        // then
        assertThat( members, containsInAnyOrder(
                new Object[]{coreAddresses( 0 ).getRaftServer().toString(), "ROUTE", Long.MAX_VALUE},
                new Object[]{coreAddresses( 0 ).getRaftServer().toString(), "WRITE", Long.MAX_VALUE},
                new Object[]{coreAddresses( 0 ).getRaftServer().toString(), "READ", Long.MAX_VALUE},
                new Object[]{coreAddresses( 1 ).getRaftServer().toString(), "ROUTE", Long.MAX_VALUE},
                new Object[]{coreAddresses( 1 ).getRaftServer().toString(), "READ", Long.MAX_VALUE},
                new Object[]{coreAddresses( 2 ).getRaftServer().toString(), "ROUTE", Long.MAX_VALUE},
                new Object[]{coreAddresses( 2 ).getRaftServer().toString(), "READ", Long.MAX_VALUE} )
        );
    }

    @Test
    public void shouldReturnSelfIfOnlyMemberOfTheCluster() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 0 ) );

        Map<MemberId, CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.coreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, emptySet() ) );

        final GetServersProcedure proc =
                new GetServersProcedure( coreTopologyService, leaderLocator, NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( proc.apply( null, new Object[0] ) );

        // then
        assertThat( members, Matchers.containsInAnyOrder(
                new Object[]{coreAddresses( 0 ).getRaftServer().toString(), "ROUTE", Long.MAX_VALUE},
                new Object[]{coreAddresses( 0 ).getRaftServer().toString(), "WRITE", Long.MAX_VALUE},
                new Object[]{coreAddresses( 0 ).getRaftServer().toString(), "READ", Long.MAX_VALUE} ) );
    }

    @Test
    public void shouldRecommendTheCoreLeaderForWriteAndEdgeForRead() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId, CoreAddresses> coreMembers = new HashMap<>();
        MemberId theLeader = member( 0 );
        coreMembers.put( theLeader, coreAddresses( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, addresses( 1 ) ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        GetServersProcedure procedure = new GetServersProcedure( topologyService, leaderLocator,
                NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( procedure.apply( null, new Object[0] ) );

        // then
        MatcherAssert.assertThat( members, containsInAnyOrder(
                new Object[]{coreAddresses( 0 ).getRaftServer().toString(), "ROUTE", Long.MAX_VALUE},
                new Object[]{coreAddresses( 0 ).getRaftServer().toString(), "WRITE", Long.MAX_VALUE},
                new Object[]{coreAddresses( 0 ).getRaftServer().toString(), "READ", Long.MAX_VALUE},
                new Object[]{coreAddresses( 1 ).getRaftServer().toString(), "READ", Long.MAX_VALUE}
        ) );
    }

    @Test
    public void shouldReturnCoreMemberAsReadServerIfNoEdgeServersAvailable() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId, CoreAddresses> coreMembers = new HashMap<>();
        MemberId theLeader = member( 0 );
        coreMembers.put( theLeader, coreAddresses( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, addresses() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        GetServersProcedure procedure = new GetServersProcedure( topologyService, leaderLocator,
                NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( procedure.apply( null, new Object[0] ) );

        // then
        assertThat( members, containsInAnyOrder(
                new Object[]{coreAddresses( 0 ).getRaftServer().toString(), "WRITE", Long.MAX_VALUE},
                new Object[]{coreAddresses( 0 ).getRaftServer().toString(), "READ", Long.MAX_VALUE},
                new Object[]{coreAddresses( 0 ).getRaftServer().toString(), "ROUTE", Long.MAX_VALUE}
        ) );
    }

    @Test
    public void shouldReturnNoWriteEndpointsIfThereIsNoLeader() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId, CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, addresses() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenThrow( new NoLeaderFoundException() );

        GetServersProcedure procedure = new GetServersProcedure( topologyService, leaderLocator,
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
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, addresses() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 1 ) );

        GetServersProcedure procedure = new GetServersProcedure( topologyService, leaderLocator,
                NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( procedure.apply( null, new Object[0] ) );

        // then
        assertEquals( 1, members.stream().filter( row1 -> row1[1].equals( "READ" ) ).collect( toList() ).size() );
        assertEquals( 0, members.stream().filter( row1 -> row1[1].equals( "WRITE" ) ).collect( toList() ).size() );
    }

    static Set<EdgeAddresses> addresses( int... ids )
    {
        return Arrays.stream( ids )
                .mapToObj( GetServersProcedureTest::edgeAddresses )
                .collect( Collectors.toSet() );
    }

    static CoreAddresses coreAddresses( int id )
    {
        AdvertisedSocketAddress advertisedSocketAddress = new AdvertisedSocketAddress( "localhost", (3000 + id) );
        return new CoreAddresses( advertisedSocketAddress, advertisedSocketAddress, advertisedSocketAddress );
    }

    private static EdgeAddresses edgeAddresses( int id )
    {
        AdvertisedSocketAddress advertisedSocketAddress = new AdvertisedSocketAddress( "localhost", (3000 + id) );
        return new EdgeAddresses( advertisedSocketAddress );
    }

    @Test
    public void shouldDELETEME() throws Exception
    {
        // given
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId, CoreAddresses> coreMembers = new HashMap<>();
        MemberId theLeader = member( 0 );
        coreMembers.put( theLeader, coreAddresses( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, GetServersProcedureTest
                .addresses( 1 ) ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        GetServersProcedure procedure = new GetServersProcedure( topologyService, leaderLocator, getInstance() );

        // when

        // then
        RawIterator<Object[], ProcedureException> iterator = procedure.apply( null, new Object[0] );

        while ( iterator.hasNext() )
        {
            Object[] objects = iterator.next();
            for ( Object object : objects )
            {
                System.out.print( object + " " );
            }
            System.out.println();
        }

    }
}
