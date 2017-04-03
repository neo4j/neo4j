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
package org.neo4j.causalclustering.load_balancing.procedure;

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
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.load_balancing.Role;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.FieldSignature;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;

import static java.util.Collections.emptyMap;
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
import static org.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
import static org.neo4j.causalclustering.discovery.TestTopology.adressesForCore;
import static org.neo4j.causalclustering.discovery.TestTopology.readReplicaInfoMap;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTInteger;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTMap;
import static org.neo4j.logging.NullLogProvider.getInstance;

@RunWith( Parameterized.class )
public class GetServersProcedureV1Test
{
    private final ClusterId clusterId = new ClusterId( UUID.randomUUID() );

    @Parameterized.Parameter( 0 )
    public String description;
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
        when( coreTopologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        // set the TTL in minutes
        config = config.augment( stringMap( cluster_routing_ttl.name(), "10m" ) );

        final LegacyGetServersProcedure proc =
                new LegacyGetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

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
        final LegacyGetServersProcedure proc = new LegacyGetServersProcedure( null, null, config, getInstance() );

        // when
        ProcedureSignature signature = proc.signature();

        // then
        assertThat( signature.outputSignature(), containsInAnyOrder(
                FieldSignature.outputField( "ttl", NTInteger ),
                FieldSignature.outputField( "servers", NTMap ) ) );
    }

    @Test
    public void shouldProvideReaderAndRouterForSingleCoreSetup() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), adressesForCore( 0 ) );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.coreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        final LegacyGetServersProcedure proc =
                new LegacyGetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( proc );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.readAddress( adressesForCore( 0 ).connectors().boltAddress() );
        builder.routeAddress( adressesForCore( 0 ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnCoreServersWithRouteAllCoresButLeaderAsReadAndSingleWriteActions() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 0 ) );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), adressesForCore( 0 ) );
        coreMembers.put( member( 1 ), adressesForCore( 1 ) );
        coreMembers.put( member( 2 ), adressesForCore( 2 ) );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.coreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        final LegacyGetServersProcedure proc =
                new LegacyGetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( proc );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( adressesForCore( 0 ).connectors().boltAddress() );
        builder.readAddress( adressesForCore( 1 ).connectors().boltAddress() );
        builder.readAddress( adressesForCore( 2 ).connectors().boltAddress() );
        builder.routeAddress( adressesForCore( 0 ).connectors().boltAddress() );
        builder.routeAddress( adressesForCore( 1 ).connectors().boltAddress() );
        builder.routeAddress( adressesForCore( 2 ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnSelfIfOnlyMemberOfTheCluster() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 0 ) );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), adressesForCore( 0 ) );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.coreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        final LegacyGetServersProcedure proc =
                new LegacyGetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( proc );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( adressesForCore( 0 ).connectors().boltAddress()  );
        builder.readAddress( adressesForCore( 0 ).connectors().boltAddress() );
        builder.routeAddress( adressesForCore( 0 ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnTheCoreLeaderForWriteAndReadReplicasAndCoresForReads() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        MemberId theLeader = member( 0 );
        coreMembers.put( theLeader, adressesForCore( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( readReplicaInfoMap( 1 ) ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        LegacyGetServersProcedure procedure =
                new LegacyGetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( procedure );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( adressesForCore( 0 ).connectors().boltAddress() );
        if ( expectFollowersAsReadEndPoints )
        {
            builder.readAddress( adressesForCore( 0 ).connectors().boltAddress() );
        }
        builder.readAddress( addressesForReadReplica( 1 ).connectors().boltAddress() );
        builder.routeAddress( adressesForCore( 0 ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnCoreMemberAsReadServerIfNoReadReplicasAvailable() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        MemberId theLeader = member( 0 );
        coreMembers.put( theLeader, adressesForCore( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        LegacyGetServersProcedure procedure =
                new LegacyGetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( procedure );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.writeAddress( adressesForCore( 0 ).connectors().boltAddress() );
        builder.readAddress( adressesForCore( 0 ).connectors().boltAddress() );
        builder.routeAddress( adressesForCore( 0 ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnNoWriteEndpointsIfThereIsNoLeader() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), adressesForCore( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenThrow( new NoLeaderFoundException() );

        LegacyGetServersProcedure procedure =
                new LegacyGetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( procedure );

        // then
        ClusterView.Builder builder = new ClusterView.Builder();
        builder.readAddress( adressesForCore( 0 ).connectors().boltAddress() );
        builder.routeAddress( adressesForCore( 0 ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @Test
    public void shouldReturnNoWriteEndpointsIfThereIsNoAddressForTheLeader() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), adressesForCore( 0 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( clusterId, false, coreMembers ) );
        when( topologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 1 ) );

        LegacyGetServersProcedure procedure =
                new LegacyGetServersProcedure( topologyService, leaderLocator, config, getInstance() );

        // when
        ClusterView clusterView = run( procedure );

        // then

        ClusterView.Builder builder = new ClusterView.Builder();
        builder.readAddress( adressesForCore( 0 ).connectors().boltAddress() );
        builder.routeAddress( adressesForCore( 0 ).connectors().boltAddress() );

        assertEquals( builder.build(), clusterView );
    }

    @SuppressWarnings( "unchecked" )
    private ClusterView run( LegacyGetServersProcedure proc ) throws ProcedureException
    {
        final Object[] rows = asList( proc.apply( null, new Object[0] ) ).get( 0 );
        assertEquals( config.get( cluster_routing_ttl ) / 1000, /* ttl */(long) rows[0] );
        return ClusterView.parse( (List<Map<String,Object>>) rows[1] );
    }

    private static class ClusterView
    {
        private final Map<Role,Set<AdvertisedSocketAddress>> clusterView;

        private ClusterView( Map<Role,Set<AdvertisedSocketAddress>> clusterView )
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
            Map<Role,Set<AdvertisedSocketAddress>> view = new HashMap<>();
            for ( Map<String,Object> single : result )
            {
                Role role = Role.valueOf( (String) single.get( "role" ) );
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
            private final Map<Role,Set<AdvertisedSocketAddress>> view = new HashMap<>();

            Builder readAddress( AdvertisedSocketAddress address )
            {
                addAddress( Role.READ, address );
                return this;
            }

            Builder writeAddress( AdvertisedSocketAddress address )
            {
                addAddress( Role.WRITE, address );
                return this;
            }

            Builder routeAddress( AdvertisedSocketAddress address )
            {
                addAddress( Role.ROUTE, address );
                return this;
            }

            private void addAddress( Role role, AdvertisedSocketAddress address )
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
