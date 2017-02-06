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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.causalclustering.discovery.ClusterTopology;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaAddresses;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaTopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.readreplica.ConnectToRandomCoreServerTest.fakeCoreTopology;

public class TypicallyConnectToRandomReadReplicaTest
{
    @Test
    public void shouldConnectToCoreOneInTenTimesByDefault() throws Exception
    {
        // given
        MemberId theCoreMemberId = new MemberId( UUID.randomUUID() );
        ReadReplicaTopologyService readReplicaTopologyService =
                fakeTopologyService( fakeCoreTopology( theCoreMemberId ), fakeReadReplicaTopology( memberIDs( 100 ) ) );

        TypicallyConnectToRandomReadReplica connectionStrategy = new TypicallyConnectToRandomReadReplica();
        connectionStrategy.setDiscoveryService( readReplicaTopologyService );

        List<MemberId> responses = new ArrayList<>();

        // when
        for ( int i = 0; i < 10; i++ )
        {
            responses.add( connectionStrategy.upstreamDatabase().get() );
        }

        // then
        assertThat( responses, hasItem( theCoreMemberId ) );
    }

    private ReadReplicaTopologyService fakeTopologyService( CoreTopology coreTopology,
            ReadReplicaTopology readReplicaTopology )
    {
        return new ReadReplicaTopologyService()
        {
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
            public ClusterTopology allServers()
            {
                return new ClusterTopology( coreTopology, readReplicaTopology );
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

    private MemberId[] memberIDs( int howMany )
    {
        MemberId[] result = new MemberId[howMany];

        for ( int i = 0; i < howMany; i++ )
        {
            result[i] = new MemberId( UUID.randomUUID() );
        }

        return result;
    }

    private ReadReplicaTopology fakeReadReplicaTopology( MemberId... readReplicaIds )
    {
        assert readReplicaIds.length > 0;

        Map<MemberId,ReadReplicaAddresses> readReplicas = new HashMap<>();

        int offset = 0;

        for ( MemberId memberId : readReplicaIds )
        {
            readReplicas.put( memberId, new ReadReplicaAddresses( new ClientConnectorAddresses( singletonList(
                    new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt,
                            new AdvertisedSocketAddress( "localhost", 11000 + offset ) ) ) ),
                    new AdvertisedSocketAddress( "localhost", 10000 + offset ) ) );

            offset++;
        }

        return new ReadReplicaTopology( readReplicas );
    }
}
