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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class ConnectToRandomCoreServerStrategyTest
{
    @Test
    public void shouldConnectToRandomCoreServer() throws Exception
    {
        // given
        MemberId memberId1 = new MemberId( UUID.randomUUID() );
        MemberId memberId2 = new MemberId( UUID.randomUUID() );
        MemberId memberId3 = new MemberId( UUID.randomUUID() );

        TopologyService topologyService = mock( TopologyService.class );
        when( topologyService.localCoreServers() ).thenReturn( fakeCoreTopology( memberId1, memberId2, memberId3 ) );

        ConnectToRandomCoreServerStrategy connectionStrategy = new ConnectToRandomCoreServerStrategy();
        connectionStrategy.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), null );

        // when
        Optional<MemberId> memberId = connectionStrategy.upstreamDatabase();

        // then
        assertTrue( memberId.isPresent() );
        assertThat( memberId.get(), anyOf( equalTo( memberId1 ), equalTo( memberId2 ), equalTo( memberId3 ) ) );
    }

    @Test
    public void filtersSelf() throws UpstreamDatabaseSelectionException
    {
        // given
        MemberId myself = new MemberId( new UUID( 1234, 5678 ) );
        Config config = Config.defaults();
        String groupName = "groupName";

        // and
        ConnectToRandomCoreServerStrategy connectToRandomCoreServerStrategy = new ConnectToRandomCoreServerStrategy();
        connectToRandomCoreServerStrategy.inject( new TopologyServiceThatPrioritisesItself( myself, groupName ), config, NullLogProvider.getInstance(),
                myself );

        // when
        Optional<MemberId> found = connectToRandomCoreServerStrategy.upstreamDatabase();

        // then
        Assert.assertTrue( found.isPresent() );
        Assert.assertNotEquals( myself, found );
    }

    static CoreTopology fakeCoreTopology( MemberId... memberIds )
    {
        assert memberIds.length > 0;

        ClusterId clusterId = new ClusterId( UUID.randomUUID() );
        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();

        int offset = 0;

        for ( MemberId memberId : memberIds )
        {
            coreMembers.put( memberId,
                    new CoreServerInfo( new AdvertisedSocketAddress( "localhost", 5000 + offset ), new AdvertisedSocketAddress( "localhost", 6000 + offset ),
                            new ClientConnectorAddresses( singletonList( new ClientConnectorAddresses.ConnectorUri( ClientConnectorAddresses.Scheme.bolt,
                                    new AdvertisedSocketAddress( "localhost", 7000 + offset ) ) ) ), asSet( "core" ), "default" ) );

            offset++;
        }

        return new CoreTopology( clusterId, false, coreMembers );
    }
}
