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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static co.unruly.matchers.OptionalMatchers.contains;
import static co.unruly.matchers.OptionalMatchers.empty;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isIn;
import static org.neo4j.causalclustering.readreplica.ConnectToRandomCoreServerStrategyTest.fakeCoreTopology;
import static org.neo4j.causalclustering.readreplica.UserDefinedConfigurationStrategyTest.fakeTopologyService;
import static org.neo4j.causalclustering.readreplica.UserDefinedConfigurationStrategyTest.memberIDs;

public class ConnectRandomlyToServerGroupStrategyImplTest
{
    @Test
    public void shouldStayWithinGivenSingleServerGroup() throws Exception
    {
        // given
        final List<String> myServerGroup = Collections.singletonList( "my_server_group" );

        MemberId[] myGroupMemberIds = memberIDs( 10 );
        TopologyService topologyService = getTopologyService( myServerGroup, myGroupMemberIds, Collections.singletonList( "your_server_group" ) );

        ConnectRandomlyToServerGroupImpl strategy = new ConnectRandomlyToServerGroupImpl( myServerGroup, topologyService, myGroupMemberIds[0] );

        // when
        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, contains( isIn( myGroupMemberIds ) ));
    }

    @Test
    public void shouldSelectAnyFromMultipleServerGroups() throws Exception
    {
        // given
        final List<String> myServerGroups = Arrays.asList( "a", "b", "c" );

        MemberId[] myGroupMemberIds = memberIDs( 10 );
        TopologyService topologyService = getTopologyService( myServerGroups, myGroupMemberIds, Arrays.asList( "x", "y", "z" ) );

        ConnectRandomlyToServerGroupImpl strategy = new ConnectRandomlyToServerGroupImpl( myServerGroups, topologyService, myGroupMemberIds[0] );

        // when
        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, contains( isIn( myGroupMemberIds ) ));
    }

    @Test
    public void shouldReturnEmptyIfNoGroupsInConfig() throws Exception
    {
        // given
        MemberId[] myGroupMemberIds = memberIDs( 10 );
        TopologyService topologyService =
                getTopologyService( Collections.singletonList( "my_server_group" ), myGroupMemberIds, Arrays.asList( "x", "y", "z" ) );
        ConnectRandomlyToServerGroupImpl strategy = new ConnectRandomlyToServerGroupImpl( Collections.emptyList(), topologyService, null );

        // when
        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, empty() );
    }

    @Test
    public void shouldReturnEmptyIfGroupOnlyContainsSelf() throws Exception
    {
        // given
        final List<String> myServerGroup = Collections.singletonList( "group" );

        MemberId[] myGroupMemberIds = memberIDs( 1 );
        TopologyService topologyService = getTopologyService( myServerGroup, myGroupMemberIds, Arrays.asList( "x", "y", "z" ) );

        ConnectRandomlyToServerGroupImpl strategy = new ConnectRandomlyToServerGroupImpl( myServerGroup, topologyService, myGroupMemberIds[0] );

        // when
        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, empty() );
    }

    static TopologyService getTopologyService( List<String> myServerGroups, MemberId[] myGroupMemberIds, List<String> unwanted )
    {
        return fakeTopologyService( fakeCoreTopology( new MemberId( UUID.randomUUID() ) ),
                fakeReadReplicaTopology( myServerGroups, myGroupMemberIds, unwanted, 10 ) );
    }

    static ReadReplicaTopology fakeReadReplicaTopology( List<String> wanted, MemberId[] memberIds, List<String> unwanted, int unwantedNumber )
    {
        Map<MemberId,ReadReplicaInfo> readReplicas = new HashMap<>();

        int offset = 0;

        for ( MemberId memberId : memberIds )
        {
            readReplicas.put( memberId, new ReadReplicaInfo( new ClientConnectorAddresses( singletonList(
                    new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt,
                            new AdvertisedSocketAddress( "localhost", 11000 + offset ) ) ) ), new AdvertisedSocketAddress( "localhost", 10000 + offset ),
                    new HashSet<>( wanted ) ) );

            offset++;
        }

        for ( int i = 0; i < unwantedNumber; i++ )
        {
            readReplicas.put( new MemberId( UUID.randomUUID() ), new ReadReplicaInfo( new ClientConnectorAddresses( singletonList(
                    new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt,
                            new AdvertisedSocketAddress( "localhost", 11000 + offset ) ) ) ), new AdvertisedSocketAddress( "localhost", 10000 + offset ),
                    new HashSet<>( unwanted ) ) );

            offset++;
        }

        return new ReadReplicaTopology( readReplicas );
    }
}
