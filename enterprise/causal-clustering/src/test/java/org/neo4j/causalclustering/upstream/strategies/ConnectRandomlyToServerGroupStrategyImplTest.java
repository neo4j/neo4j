/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.upstream.strategies;

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
import static org.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategyTest.fakeCoreTopology;
import static org.neo4j.causalclustering.upstream.strategies.UserDefinedConfigurationStrategyTest.fakeTopologyService;
import static org.neo4j.causalclustering.upstream.strategies.UserDefinedConfigurationStrategyTest.memberIDs;

public class ConnectRandomlyToServerGroupStrategyImplTest
{
    @Test
    public void shouldStayWithinGivenSingleServerGroup()
    {
        // given
        final List<String> myServerGroup = Collections.singletonList( "my_server_group" );

        MemberId[] myGroupMemberIds = memberIDs( 10 );
        TopologyService topologyService = getTopologyService( myServerGroup, myGroupMemberIds, Collections.singletonList( "your_server_group" ) );

        ConnectRandomlyToServerGroupImpl strategy = new ConnectRandomlyToServerGroupImpl( myServerGroup, topologyService, myGroupMemberIds[0] );

        // when
        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, contains( isIn( myGroupMemberIds ) ) );
    }

    @Test
    public void shouldSelectAnyFromMultipleServerGroups()
    {
        // given
        final List<String> myServerGroups = Arrays.asList( "a", "b", "c" );

        MemberId[] myGroupMemberIds = memberIDs( 10 );
        TopologyService topologyService = getTopologyService( myServerGroups, myGroupMemberIds, Arrays.asList( "x", "y", "z" ) );

        ConnectRandomlyToServerGroupImpl strategy = new ConnectRandomlyToServerGroupImpl( myServerGroups, topologyService, myGroupMemberIds[0] );

        // when
        Optional<MemberId> memberId = strategy.upstreamDatabase();

        // then
        assertThat( memberId, contains( isIn( myGroupMemberIds ) ) );
    }

    @Test
    public void shouldReturnEmptyIfNoGroupsInConfig()
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
    public void shouldReturnEmptyIfGroupOnlyContainsSelf()
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
                    new HashSet<>( wanted ), "default" ) );

            offset++;
        }

        for ( int i = 0; i < unwantedNumber; i++ )
        {
            readReplicas.put( new MemberId( UUID.randomUUID() ), new ReadReplicaInfo( new ClientConnectorAddresses( singletonList(
                    new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt,
                            new AdvertisedSocketAddress( "localhost", 11000 + offset ) ) ) ), new AdvertisedSocketAddress( "localhost", 10000 + offset ),
                    new HashSet<>( unwanted ), "default" ) );

            offset++;
        }

        return new ReadReplicaTopology( readReplicas );
    }
}
