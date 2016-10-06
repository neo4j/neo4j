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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.core.consensus.LeaderLocator;
import org.neo4j.coreedge.core.consensus.NoLeaderFoundException;
import org.neo4j.coreedge.discovery.ClientConnectorAddresses;
import org.neo4j.coreedge.discovery.ClientConnectorAddresses.ConnectorUri;
import org.neo4j.coreedge.discovery.CoreAddresses;
import org.neo4j.coreedge.discovery.CoreTopology;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.EdgeAddresses;
import org.neo4j.coreedge.discovery.EdgeTopology;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.api.proc.FieldSignature;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.configuration.Config;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.cluster_routing_ttl;
import static org.neo4j.coreedge.discovery.ClientConnectorAddresses.Scheme.bolt;
import static org.neo4j.coreedge.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTMap;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class GetServersProcedureTest
{
    private ClusterId clusterId = new ClusterId( UUID.randomUUID() );
    private Config config = Config.defaults();

    @Test
    public void ttlShouldBeInSeconds() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, new HashMap<>() );
        when( coreTopologyService.coreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, emptySet() ) );

        // set the TTL in minutes
        config = config.augment( stringMap( cluster_routing_ttl.name(), "10m" ) );

        final GetServersProcedure proc =
                new GetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        List<Object[]> results = asList( proc.apply( null, new Object[0] ) );

        // then
        Object[] rows = results.get( 0 );
        long ttlInSeconds = (long) rows[0];
        assertEquals( 600, ttlInSeconds );
    }

    @Test
    public void shouldHaveCorrectSignature() throws Exception
    {
        // given
        final GetServersProcedure proc = new GetServersProcedure( null, null, config, getInstance() );

        // when
        ProcedureSignature signature = proc.signature();

        // then
        assertThat( signature.outputSignature(), containsInAnyOrder( new FieldSignature( "ttl", Neo4jTypes.NTInteger ),
                new FieldSignature( "servers", NTMap ) ) );
    }

    @Test
    public void shouldProvideReaderAndRouterForSingleCoreSetup() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.coreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, emptySet() ) );

        final GetServersProcedure proc =
                new GetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        List<Object[]> results = asList( proc.apply( null, new Object[0] ) );

        // then

        Object[] rows = results.get( 0 );

        long ttl = (long) rows[0];
        assertEquals( (long) config.get( cluster_routing_ttl ) / 1000, ttl );

        List<Map<String,Object[]>> servers = (List<Map<String,Object[]>>) rows[1];

        Map<String,Object[]> readServers = servers.get( 0 );
        assertThat( readServers.get( "role" ), equalTo( "READ" ) );
        assertThat( asList( readServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );

        Map<String,Object[]> routingServers = servers.get( 1 );
        assertThat( routingServers.get( "role" ), equalTo( "ROUTE" ) );
        assertThat( asList( routingServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );
    }

    @Test
    public void shouldReturnCoreServersWithReadRouteAndSingleWriteActions() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 0 ) );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );
        coreMembers.put( member( 1 ), coreAddresses( 1 ) );
        coreMembers.put( member( 2 ), coreAddresses( 2 ) );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.coreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, emptySet() ) );

        final GetServersProcedure proc =
                new GetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        final List<Object[]> results = asList( proc.apply( null, new Object[0] ) );

        // then
        Object[] rows = results.get( 0 );

        long ttl = (long) rows[0];
        assertEquals( (long) config.get( cluster_routing_ttl ) / 1000, ttl );

        List<Map<String,Object[]>> servers = (List<Map<String,Object[]>>) rows[1];

        Map<String,Object[]> writeServers = servers.get( 0 );
        assertThat( writeServers.get( "role" ), equalTo( "WRITE" ) );
        assertThat( asList( writeServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );

        Map<String,Object[]> readServers = servers.get( 1 );
        assertThat( readServers.get( "role" ), equalTo( "READ" ) );
        assertThat( asList( readServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString(),
                        coreAddresses( 1 ).getRaftServer().toString(),
                        coreAddresses( 2 ).getRaftServer().toString() ) );

        Map<String,Object[]> routingServers = servers.get( 2 );
        assertThat( routingServers.get( "role" ), equalTo( "ROUTE" ) );
        assertThat( asList( routingServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString(),
                        coreAddresses( 1 ).getRaftServer().toString(),
                        coreAddresses( 2 ).getRaftServer().toString() ) );
    }

    @Test
    public void shouldReturnSelfIfOnlyMemberOfTheCluster() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 0 ) );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.coreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, emptySet() ) );

        final GetServersProcedure proc =
                new GetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        final List<Object[]> results = asList( proc.apply( null, new Object[0] ) );

        // then
        Object[] rows = results.get( 0 );

        long ttl = (long) rows[0];
        assertEquals( (long) config.get( cluster_routing_ttl ) / 1000, ttl );

        List<Map<String,Object[]>> servers = (List<Map<String,Object[]>>) rows[1];

        Map<String,Object[]> writeServers = servers.get( 0 );
        assertThat( writeServers.get( "role" ), equalTo( "WRITE" ) );
        assertThat( asList( writeServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );

        Map<String,Object[]> readServers = servers.get( 1 );
        assertThat( readServers.get( "role" ), equalTo( "READ" ) );
        assertThat( asList( readServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );

        Map<String,Object[]> routingServers = servers.get( 2 );
        assertThat( routingServers.get( "role" ), equalTo( "ROUTE" ) );
        assertThat( asList( routingServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );
    }

    @Test
    public void shouldReturnTheCoreLeaderForWriteAndEdgesAndCoresForReads() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        MemberId theLeader = member( 0 );
        coreMembers.put( theLeader, coreAddresses( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, addresses( 1 ) ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        GetServersProcedure procedure =
                new GetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        final List<Object[]> results = asList( procedure.apply( null, new Object[0] ) );

        // then
        Object[] rows = results.get( 0 );

        long ttl = (long) rows[0];
        assertEquals( (long) config.get( cluster_routing_ttl ) / 1000, ttl );

        List<Map<String,Object[]>> servers = (List<Map<String,Object[]>>) rows[1];

        Map<String,Object[]> writeServers = servers.get( 0 );
        assertThat( writeServers.get( "role" ), equalTo( "WRITE" ) );
        assertThat( asList( writeServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );

        Map<String,Object[]> readServers = servers.get( 1 );
        assertThat( readServers.get( "role" ), equalTo( "READ" ) );
        assertThat( asList( readServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString(),
                        edgeAddresses( 1 ).getClientConnectorAddresses().getBoltAddress().toString() ) );

        Map<String,Object[]> routingServers = servers.get( 2 );
        assertThat( routingServers.get( "role" ), equalTo( "ROUTE" ) );
        assertThat( asList( routingServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );
    }

    @Test
    public void shouldReturnCoreMemberAsReadServerIfNoEdgeServersAvailable() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        MemberId theLeader = member( 0 );
        coreMembers.put( theLeader, coreAddresses( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, addresses() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        GetServersProcedure procedure =
                new GetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        final List<Object[]> results = asList( procedure.apply( null, new Object[0] ) );

        // then
        Object[] rows = results.get( 0 );

        long ttl = (long) rows[0];
        assertEquals( (long) config.get( cluster_routing_ttl ) / 1000, ttl );

        List<Map<String,Object[]>> servers = (List<Map<String,Object[]>>) rows[1];

        Map<String,Object[]> writeServers = servers.get( 0 );
        assertThat( writeServers.get( "role" ), equalTo( "WRITE" ) );
        assertThat( asList( writeServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );

        Map<String,Object[]> readServers = servers.get( 1 );
        assertThat( readServers.get( "role" ), equalTo( "READ" ) );
        assertThat( asList( readServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );

        Map<String,Object[]> routingServers = servers.get( 2 );
        assertThat( routingServers.get( "role" ), equalTo( "ROUTE" ) );
        assertThat( asList( routingServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );
    }

    @Test
    public void shouldReturnNoWriteEndpointsIfThereIsNoLeader() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, addresses() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenThrow( new NoLeaderFoundException() );

        GetServersProcedure procedure =
                new GetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        final List<Object[]> results = asList( procedure.apply( null, new Object[0] ) );

        // then
        Object[] rows = results.get( 0 );

        long ttl = (long) rows[0];
        assertEquals( (long) config.get( cluster_routing_ttl ) / 1000, ttl );

        List<Map<String,Object[]>> servers = (List<Map<String,Object[]>>) rows[1];

        Map<String,Object[]> readServers = servers.get( 0 );
        assertThat( readServers.get( "role" ), equalTo( "READ" ) );
        assertThat( asList( readServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );

        Map<String,Object[]> routingServers = servers.get( 1 );
        assertThat( routingServers.get( "role" ), equalTo( "ROUTE" ) );
        assertThat( asList( routingServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );
    }

    @Test
    public void shouldReturnNoWriteEndpointsIfThereIsNoAddressForTheLeader() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.edgeServers() ).thenReturn( new EdgeTopology( clusterId, addresses() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 1 ) );

        GetServersProcedure procedure =
                new GetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        final List<Object[]> results = asList( procedure.apply( null, new Object[0] ) );

        // then
        Object[] rows = results.get( 0 );

        long ttl = (long) rows[0];
        assertEquals( (long) config.get( cluster_routing_ttl)  / 1000, ttl );

        List<Map<String,Object[]>> servers = (List<Map<String,Object[]>>) rows[1];

        Map<String,Object[]> readServers = servers.get( 0 );
        assertThat( readServers.get( "role" ), equalTo( "READ" ) );
        assertThat( asList( readServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );

        Map<String,Object[]> routingServers = servers.get( 1 );
        assertThat( routingServers.get( "role" ), equalTo( "ROUTE" ) );
        assertThat( asList( routingServers.get( "addresses" ) ),
                containsInAnyOrder( coreAddresses( 0 ).getRaftServer().toString() ) );
    }

    static Set<EdgeAddresses> addresses( int... ids )
    {
        return Arrays.stream( ids ).mapToObj( GetServersProcedureTest::edgeAddresses ).collect( Collectors.toSet() );
    }

    static CoreAddresses coreAddresses( int id )
    {
        AdvertisedSocketAddress advertisedSocketAddress = new AdvertisedSocketAddress( "localhost", (3000 + id) );
        return new CoreAddresses( advertisedSocketAddress, advertisedSocketAddress,
                new ClientConnectorAddresses( singletonList( new ConnectorUri( bolt, advertisedSocketAddress ) ) ) );
    }

    private static EdgeAddresses edgeAddresses( int id )
    {
        AdvertisedSocketAddress advertisedSocketAddress = new AdvertisedSocketAddress( "localhost", (3000 + id) );
        return new EdgeAddresses(
                new ClientConnectorAddresses( singletonList( new ConnectorUri( bolt, advertisedSocketAddress ) ) ) );
    }
}
