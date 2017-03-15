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
package org.neo4j.causalclustering.readreplica;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.extractCatchupAddressesMap;
import static org.neo4j.causalclustering.readreplica.ConnectToRandomCoreServerStrategyTest.fakeCoreTopology;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.MapUtil.stringMap;


public class UserDefinedConfigurationStrategyTest
{
    @Test
    public void shouldPickTheFirstMatchingServer() throws Exception
    {
        // given
        MemberId theCoreMemberId = new MemberId( UUID.randomUUID() );
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( theCoreMemberId ),
                fakeReadReplicaTopology( memberIDs( 100 ), new NoEastGroupGenerator() ) );

        UserDefinedConfigurationStrategy strategy = new UserDefinedConfigurationStrategy();
        Config config = Config.defaults()
                .with( stringMap( CausalClusteringSettings.user_defined_upstream_selection_strategy.name(),
                        "groups(east); groups(core); halt()" ) );

        strategy.setConfig( config );
        strategy.setTopologyService( topologyService );

        //when

        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertEquals( theCoreMemberId, memberId.get() );
    }

    private ReadReplicaTopology fakeReadReplicaTopology( MemberId[] readReplicaIds, NoEastGroupGenerator groupGenerator )
    {
        assert readReplicaIds.length > 0;

        Map<MemberId,ReadReplicaInfo> readReplicas = new HashMap<>();

        int offset = 0;

        for ( MemberId memberId : readReplicaIds )
        {
            readReplicas.put( memberId, new ReadReplicaInfo( new ClientConnectorAddresses( singletonList(
                    new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt,
                            new AdvertisedSocketAddress( "localhost", 11000 + offset ) ) ) ),
                    new AdvertisedSocketAddress( "localhost", 10000 + offset ), groupGenerator.get( memberId ) ) );

            offset++;
        }

        return new ReadReplicaTopology( readReplicas );
    }

    static TopologyService fakeTopologyService( CoreTopology coreTopology, ReadReplicaTopology readReplicaTopology )
    {
        return new TopologyService()
        {
            private Map<MemberId,AdvertisedSocketAddress> catchupAddresses =
                    extractCatchupAddressesMap( coreTopology, readReplicaTopology );

            @Override
            public CoreTopology coreServers()
            {
                return coreTopology;
            }

            @Override
            public ReadReplicaTopology readReplicas()
            {
                return readReplicaTopology;
            }

            @Override
            public Optional<AdvertisedSocketAddress> findCatchupAddress( MemberId upstream )
            {
                return Optional.ofNullable( catchupAddresses.get( upstream ) );
            }

            @Override
            public void init() throws Throwable
            {

            }

            @Override
            public void start() throws Throwable
            {

            }

            @Override
            public void stop() throws Throwable
            {

            }

            @Override
            public void shutdown() throws Throwable
            {

            }
        };
    }

    static MemberId[] memberIDs( int howMany )
    {
        MemberId[] result = new MemberId[howMany];

        for ( int i = 0; i < howMany; i++ )
        {
            result[i] = new MemberId( UUID.randomUUID() );
        }

        return result;
    }

    static ReadReplicaTopology fakeReadReplicaTopology( MemberId... readReplicaIds )
    {
        assert readReplicaIds.length > 0;

        Map<MemberId,ReadReplicaInfo> readReplicas = new HashMap<>();

        int offset = 0;

        for ( MemberId memberId : readReplicaIds )
        {
            readReplicas.put( memberId, new ReadReplicaInfo( new ClientConnectorAddresses( singletonList(
                    new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt,
                            new AdvertisedSocketAddress( "localhost", 11000 + offset ) ) ) ),
                    new AdvertisedSocketAddress( "localhost", 10000 + offset ) ) );

            offset++;
        }

        return new ReadReplicaTopology( readReplicas );
    }

    private static class NoEastGroupGenerator
    {
        private static final String[] SOME_ORDINALS = {"north", "south", "west"};
        private static final Random random = new Random();

        public Set<String> get( MemberId memberId )
        {
            return asSet( SOME_ORDINALS[random.nextInt( SOME_ORDINALS.length )] );
        }
    }
}
