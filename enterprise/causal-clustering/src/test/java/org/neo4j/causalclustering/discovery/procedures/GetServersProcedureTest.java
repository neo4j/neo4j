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
package org.neo4j.causalclustering.discovery.procedures;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.causalclustering.discovery.ClientConnectorAddresses.ConnectorUri;
import org.neo4j.causalclustering.discovery.CoreAddresses;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.ReadReplicaAddresses;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.FieldSignature;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.cluster_routing_ttl;
import static org.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.bolt;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTMap;
import static org.neo4j.logging.NullLogProvider.getInstance;

@RunWith( Parameterized.class )
public class GetServersProcedureTest
{
    private final ClusterId clusterId = new ClusterId( UUID.randomUUID() );

    @Parameterized.Parameter( 0 )
    public String ignored; // <- JUnit is happy only if this is here!
    @Parameterized.Parameter( 1 )
    public Config config;
    @Parameterized.Parameter( 2 )
    public boolean expectFollowersAsReadEndPoints;

    @Parameterized.Parameters( name = "{0}")
    public static Collection<Object[]> params()
    {
        return Arrays.asList(
                new Object[]{"with followers as read end points", Config.defaults().augment(
                        singletonMap( cluster_allow_reads_on_followers.name(), Settings.TRUE ) ), true },
                new Object[]{"no followers as read end points", Config.defaults().augment(
                        singletonMap( cluster_allow_reads_on_followers.name(), Settings.FALSE ) ), false }
        );
    }

    @Test
    public void ttlShouldBeInSeconds() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, new HashMap<>() );
        when( coreTopologyService.coreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( clusterId, emptySet() ) );

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
        when( coreTopologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( clusterId, emptySet() ) );

        final GetServersProcedure proc =
                new GetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( proc );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.readAddress( coreAddresses( 0 ).getRaftServer() );
        builder.routeAddress( coreAddresses( 0 ).getRaftServer() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnCoreServersWithRouteAllCoresButLeaderAsReadAndSingleWriteActions() throws Exception
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
        when( coreTopologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( clusterId, emptySet() ) );

        final GetServersProcedure proc =
                new GetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( proc );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( coreAddresses( 0 ).getRaftServer() );
        builder.readAddress( coreAddresses( 1 ).getRaftServer() );
        builder.readAddress( coreAddresses( 2 ).getRaftServer() );
        builder.routeAddress( coreAddresses( 0 ).getRaftServer() );
        builder.routeAddress( coreAddresses( 1 ).getRaftServer() );
        builder.routeAddress( coreAddresses( 2 ).getRaftServer() );

        assertEquals( builder.build(), clusterView );
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
        when( coreTopologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( clusterId, emptySet() ) );

        final GetServersProcedure proc =
                new GetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( proc );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( coreAddresses( 0 ).getRaftServer()  );
        builder.readAddress( coreAddresses( 0 ).getRaftServer() );
        builder.routeAddress( coreAddresses( 0 ).getRaftServer() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnTheCoreLeaderForWriteAndReadReplicasAndCoresForReads() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        MemberId theLeader = member( 0 );
        coreMembers.put( theLeader, coreAddresses( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( clusterId, addresses( 1 ) ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        GetServersProcedure procedure =
                new GetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( procedure );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( coreAddresses( 0 ).getRaftServer() );
        if ( expectFollowersAsReadEndPoints )
        {
            builder.readAddress( coreAddresses( 0 ).getRaftServer() );
        }
        builder.readAddress( readReplicaAddresses( 1 ).getClientConnectorAddresses().getBoltAddress() );
        builder.routeAddress( coreAddresses( 0 ).getRaftServer() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnCoreMemberAsReadServerIfNoReadReplicasAvailable() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        MemberId theLeader = member( 0 );
        coreMembers.put( theLeader, coreAddresses( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( clusterId, addresses() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        GetServersProcedure procedure =
                new GetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( procedure );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( coreAddresses( 0 ).getRaftServer() );
        builder.readAddress( coreAddresses( 0 ).getRaftServer() );
        builder.routeAddress( coreAddresses( 0 ).getRaftServer() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnNoWriteEndpointsIfThereIsNoLeader() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( clusterId, addresses() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenThrow( new NoLeaderFoundException() );

        GetServersProcedure procedure =
                new GetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( procedure );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.readAddress( coreAddresses( 0 ).getRaftServer() );
        builder.routeAddress( coreAddresses( 0 ).getRaftServer() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnNoWriteEndpointsIfThereIsNoAddressForTheLeader() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), coreAddresses( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( clusterId, addresses() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 1 ) );

        GetServersProcedure procedure =
                new GetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( procedure );

        // then

        ClusterView.Builder builder = new ClusterView.Builder();
        builder.readAddress( coreAddresses( 0 ).getRaftServer() );
        builder.routeAddress( coreAddresses( 0 ).getRaftServer() );

        assertEquals( builder.build(), clusterView );
    }

    @SuppressWarnings( "unchecked" )
    private ClusterView run( GetServersProcedure proc ) throws ProcedureException
    {
        final Object[] rows = asList( proc.apply( null, new Object[0] ) ).get( 0 );
        assertEquals( config.get( cluster_routing_ttl ) / 1000, /* ttl */(long) rows[0] );
        return ClusterView.parse( (List<Map<String,Object>>) rows[1] );
    }

    static Set<ReadReplicaAddresses> addresses( int... ids )
    {
        return Arrays.stream( ids ).mapToObj( GetServersProcedureTest::readReplicaAddresses ).collect( Collectors.toSet() );
    }

    static CoreAddresses coreAddresses( int id )
    {
        AdvertisedSocketAddress advertisedSocketAddress = new AdvertisedSocketAddress( "localhost", (3000 + id) );
        return new CoreAddresses( advertisedSocketAddress, advertisedSocketAddress,
                new ClientConnectorAddresses( singletonList( new ConnectorUri( bolt, advertisedSocketAddress ) ) ) );
    }

    private static ReadReplicaAddresses readReplicaAddresses( int id )
    {
        AdvertisedSocketAddress advertisedSocketAddress = new AdvertisedSocketAddress( "localhost", (3000 + id) );
        return new ReadReplicaAddresses(
                new ClientConnectorAddresses( singletonList( new ConnectorUri( bolt, advertisedSocketAddress ) ) ) );
    }

    private static class ClusterView
    {
        private final Map<GetServersProcedure.Type,Set<AdvertisedSocketAddress>> clusterView;

        private ClusterView( Map<GetServersProcedure.Type,Set<AdvertisedSocketAddress>> clusterView )
        {
            this.clusterView = clusterView;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ClusterView that = (ClusterView) o;
            return Objects.equals( clusterView, that.clusterView );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( clusterView );
        }

        @Override
        public String toString()
        {
            return "ClusterView{" + "clusterView=" + clusterView + '}';
        }

        static ClusterView parse( List<Map<String,Object>> result )
        {
            Map<GetServersProcedure.Type,Set<AdvertisedSocketAddress>> view = new HashMap<>();
            for ( Map<String,Object> single : result )
            {
                GetServersProcedure.Type role = GetServersProcedure.Type.valueOf( (String) single.get( "role" ) );
                Set<AdvertisedSocketAddress> addresses = parse( (Object[]) single.get( "addresses" ) );
                assertFalse( view.containsKey( role ) );
                view.put( role, addresses );
            }

            return new ClusterView( view );
        }

        private static Set<AdvertisedSocketAddress> parse( Object[] addresses )
        {
            List<AdvertisedSocketAddress> list =
                    Stream.of( addresses ).map( address -> parse( (String) address ) ).collect( toList() );
            Set<AdvertisedSocketAddress> set = new HashSet<>();
            set.addAll( list );
            assertEquals( list.size(), set.size() );
            return set;
        }

        private static AdvertisedSocketAddress parse( String address )
        {
            String[] split = address.split( ":" );
            assertEquals( 2, split.length );
            return new AdvertisedSocketAddress( split[0], Integer.valueOf( split[1] ) );
        }

        static class Builder
        {
            private final Map<GetServersProcedure.Type,Set<AdvertisedSocketAddress>> view = new HashMap<>();

            Builder readAddress( AdvertisedSocketAddress address )
            {
                addAddress( GetServersProcedure.Type.READ, address );
                return this;
            }

            Builder writeAddress( AdvertisedSocketAddress address )
            {
                addAddress( GetServersProcedure.Type.WRITE, address );
                return this;
            }

            Builder routeAddress( AdvertisedSocketAddress address )
            {
                addAddress( GetServersProcedure.Type.ROUTE, address );
                return this;
            }

            private void addAddress( GetServersProcedure.Type role, AdvertisedSocketAddress address )
            {
                Set<AdvertisedSocketAddress> advertisedSocketAddresses = view.get( role );
                if ( advertisedSocketAddresses == null )
                {
                    advertisedSocketAddresses = new HashSet<>();
                    view.put( role, advertisedSocketAddresses );
                }
                advertisedSocketAddresses.add( address );
            }

            public ClusterView build()
            {
                return new ClusterView( view );
            }
        }
    }
}
