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
import java.util.UUID;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.causalclustering.readreplica.ConnectToRandomCoreServerStrategyTest.fakeCoreTopology;
import static org.neo4j.causalclustering.readreplica.UserDefinedConfigurationStrategyTest.fakeTopologyService;
import static org.neo4j.causalclustering.readreplica.UserDefinedConfigurationStrategyTest.memberIDs;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ConnectRandomlyWithinServerGroupStrategyTest
{
    @Test
    public void shouldStayWithinOwnSingleServerGroup() throws Exception
    {
        // given
        final String myServerGroup = "my_server_group";

        Config configWithMyServerGroup = Config.defaults()
                .with( stringMap( CausalClusteringSettings.server_groups.name(), myServerGroup ) );

        MemberId[] myGroupMemberIds = memberIDs( 10 );
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( new MemberId( UUID.randomUUID() ) ),
                fakeReadReplicaTopology( myServerGroup, myGroupMemberIds, "your_server_group", 10 ) );

        ConnectRandomlyWithinServerGroupStrategy
                strategy = new ConnectRandomlyWithinServerGroupStrategy();
        strategy.setConfig( configWithMyServerGroup );
        strategy.setTopologyService( topologyService );

        // when
        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( asSet( myGroupMemberIds ), hasItem( memberId.get() ) );
    }

    @Test
    public void shouldSelectAnyFromMultipleServerGroups() throws Exception
    {
        // given
        final String myServerGroups = "a,b,c";

        Config configWithMyServerGroup = Config.defaults()
                .with( stringMap( CausalClusteringSettings.server_groups.name(), myServerGroups ) );

        MemberId[] myGroupMemberIds = memberIDs( 10 );
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( new MemberId( UUID.randomUUID() ) ),
                fakeReadReplicaTopology( myServerGroups, myGroupMemberIds, "x,y,z", 10 ) );

        ConnectRandomlyWithinServerGroupStrategy
                strategy = new ConnectRandomlyWithinServerGroupStrategy();
        strategy.setConfig( configWithMyServerGroup );
        strategy.setTopologyService( topologyService );

        // when
        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( asSet( myGroupMemberIds ), hasItem( memberId.get() ) );
    }

    private ReadReplicaTopology fakeReadReplicaTopology( String wanted, MemberId[] memberIds, String unwanted,
            int unwantedNumber )
    {
        Map<MemberId,ReadReplicaInfo> readReplicas = new HashMap<>();

        int offset = 0;

        for ( MemberId memberId : memberIds )
        {
            readReplicas.put( memberId, new ReadReplicaInfo( new ClientConnectorAddresses( singletonList(
                    new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt,
                            new AdvertisedSocketAddress( "localhost", 11000 + offset ) ) ) ),
                    new AdvertisedSocketAddress( "localhost", 10000 + offset ), asSet( wanted.split( "," ) ) ) );

            offset++;
        }

        for ( int i = 0; i < unwantedNumber; i++ )
        {
            readReplicas.put( new MemberId( UUID.randomUUID() ), new ReadReplicaInfo( new ClientConnectorAddresses(
                    singletonList( new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt,
                            new AdvertisedSocketAddress( "localhost", 11000 + offset ) ) ) ),
                    new AdvertisedSocketAddress( "localhost", 10000 + offset ), asSet( unwanted ) ) );

            offset++;
        }

        return new ReadReplicaTopology( readReplicas );
    }
}
