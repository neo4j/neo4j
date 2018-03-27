/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.upstream.strategies;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.AbstractTopologyService;
import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static co.unruly.matchers.OptionalMatchers.contains;
import static co.unruly.matchers.OptionalMatchers.empty;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.extractCatchupAddressesMap;
import static org.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategyTest.fakeCoreTopology;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class UserDefinedConfigurationStrategyTest
{
    @Test
    public void shouldPickTheFirstMatchingServerIfCore()
    {
        // given
        MemberId theCoreMemberId = new MemberId( UUID.randomUUID() );
        TopologyService topologyService =
                fakeTopologyService( fakeCoreTopology( theCoreMemberId ), fakeReadReplicaTopology( memberIDs( 100 ), this::noEastGroupGenerator ) );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = Config.defaults( CausalClusteringSettings.user_defined_upstream_selection_strategy, "groups(east); groups(core); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when

        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, contains( theCoreMemberId ) );
    }

    @Test
    public void shouldPickTheFirstMatchingServerIfReadReplica()
    {
        // given
        MemberId[] readReplicaIds = memberIDs( 100 );
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( new MemberId( UUID.randomUUID() ) ),
                fakeReadReplicaTopology( readReplicaIds, this::noEastGroupGenerator ) );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        String wantedGroup = noEastGroup.get( 1 );
        Config config = configWithFilter( "groups(" + wantedGroup + "); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when

        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, contains( isIn( readReplicaIds ) ) );
        assertThat( memberId.map( this::noEastGroupGenerator ), contains( equalTo( asSet( wantedGroup ) ) ) );
    }

    @Test
    public void shouldReturnEmptyIfNoMatchingServers()
    {
        // given
        MemberId[] readReplicaIds = memberIDs( 100 );
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( new MemberId( UUID.randomUUID() ) ),
                fakeReadReplicaTopology( readReplicaIds, this::noEastGroupGenerator ) );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        String wantedGroup = eastGroup;
        Config config = configWithFilter( "groups(" + wantedGroup + "); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when

        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, empty() );
    }

    @Test
    public void shouldReturnEmptyIfInvalidFilterSpecification()
    {
        // given
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( new MemberId( UUID.randomUUID() ) ),
                fakeReadReplicaTopology( memberIDs( 100 ), this::noEastGroupGenerator ) );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = configWithFilter( "invalid filter specification" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        //when

        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, empty() );
    }

    @Test
    public void shouldNotReturnSelf()
    {
        // given
        String wantedGroup = eastGroup;
        MemberId[] readReplicaIds = memberIDs( 1 );
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( new MemberId( UUID.randomUUID() ) ),
                fakeReadReplicaTopology( readReplicaIds, memberId -> asSet( wantedGroup ) ) );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = configWithFilter( "groups(" + wantedGroup + "); halt()" );

        strategy.inject( topologyService, config, NullLogProvider.getInstance(), readReplicaIds[0] );

        //when

        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, empty() );
    }

    private Config configWithFilter( String filter )
    {
        return Config.defaults( CausalClusteringSettings.user_defined_upstream_selection_strategy, filter );
    }

    static ReadReplicaTopology fakeReadReplicaTopology( MemberId... readReplicaIds )
    {
        return fakeReadReplicaTopology( readReplicaIds, ignored -> Collections.emptySet() );
    }

    static ReadReplicaTopology fakeReadReplicaTopology( MemberId[] readReplicaIds, Function<MemberId,Set<String>> groupGenerator )
    {
        assert readReplicaIds.length > 0;

        final AtomicInteger offset = new AtomicInteger( 10_000 );

        Function<MemberId,ReadReplicaInfo> toReadReplicaInfo = memberId -> readReplicaInfo( memberId, offset, groupGenerator );

        Map<MemberId,ReadReplicaInfo> readReplicas = Stream.of( readReplicaIds ).collect( Collectors.toMap( Function.identity(), toReadReplicaInfo ) );

        return new ReadReplicaTopology( readReplicas );
    }

    private static ReadReplicaInfo readReplicaInfo( MemberId memberId, AtomicInteger offset, Function<MemberId,Set<String>> groupGenerator )
    {
        return new ReadReplicaInfo( new ClientConnectorAddresses( singletonList(
                new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt,
                        new AdvertisedSocketAddress( "localhost", offset.getAndIncrement() ) ) ) ),
                new AdvertisedSocketAddress( "localhost", offset.getAndIncrement() ), groupGenerator.apply( memberId ), "default" );
    }

    static TopologyService fakeTopologyService( CoreTopology coreTopology, ReadReplicaTopology readReplicaTopology )
    {
        return new AbstractTopologyService()
        {
            private Map<MemberId,AdvertisedSocketAddress> catchupAddresses = extractCatchupAddressesMap( coreTopology, readReplicaTopology );

            @Override
            public CoreTopology allCoreServers()
            {
                return coreTopology;
            }

            @Override
            public ReadReplicaTopology allReadReplicas()
            {
                return readReplicaTopology;
            }

            @Override
            public Optional<AdvertisedSocketAddress> findCatchupAddress( MemberId upstream )
            {
                return Optional.ofNullable( catchupAddresses.get( upstream ) );
            }

            @Override
            public Map<MemberId,RoleInfo> allCoreRoles()
            {
                return emptyMap();
            }

            @Override
            public String localDBName()
            {
                return "default";
            }

            @Override
            public void init()
            {

            }

            @Override
            public void start()
            {

            }

            @Override
            public void stop()
            {

            }

            @Override
            public void shutdown()
            {

            }
        };
    }

    static MemberId[] memberIDs( int howMany )
    {
        return Stream.generate( () -> new MemberId( UUID.randomUUID() ) ).limit( howMany ).toArray( MemberId[]::new );
    }

    private final String northGroup = "north";
    private final String southGroup = "south";
    private final String westGroup = "west";
    private final String eastGroup = "east";
    private final List<String> noEastGroup = Arrays.asList( northGroup, southGroup, westGroup );

    private Set<String> noEastGroupGenerator( MemberId memberId )
    {
        int index = Math.abs( memberId.hashCode() ) % noEastGroup.size();
        return asSet( noEastGroup.get( index ) );
    }
}
