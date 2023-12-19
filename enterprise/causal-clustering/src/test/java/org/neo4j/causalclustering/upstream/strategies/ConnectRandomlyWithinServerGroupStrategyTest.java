/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.upstream.strategies;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static co.unruly.matchers.OptionalMatchers.contains;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.upstream.strategies.UserDefinedConfigurationStrategyTest.memberIDs;

public class ConnectRandomlyWithinServerGroupStrategyTest
{
    @Test
    public void shouldUseServerGroupsFromConfig()
    {
        // given
        final String myServerGroup = "my_server_group";
        Config configWithMyServerGroup = Config.defaults( CausalClusteringSettings.server_groups, myServerGroup );
        MemberId[] myGroupMemberIds = memberIDs( 10 );
        TopologyService topologyService =
                ConnectRandomlyToServerGroupStrategyImplTest.getTopologyService( Collections.singletonList( myServerGroup ), myGroupMemberIds,
                        Collections.singletonList( "your_server_group" ) );

        ConnectRandomlyWithinServerGroupStrategy strategy = new ConnectRandomlyWithinServerGroupStrategy();
        strategy.inject( topologyService, configWithMyServerGroup, NullLogProvider.getInstance(), myGroupMemberIds[0] );

        // when
        Optional<MemberId> result = strategy.upstreamDatabase();

        // then
        assertThat( result, contains( isIn( myGroupMemberIds ) ) );
    }

    @Test
    public void filtersSelf()
    {
        // given
        String groupName = "groupName";
        Config config = Config.defaults();
        config.augment( CausalClusteringSettings.server_groups, groupName );

        // and
        ConnectRandomlyWithinServerGroupStrategy connectRandomlyWithinServerGroupStrategy = new ConnectRandomlyWithinServerGroupStrategy();
        MemberId myself = new MemberId( new UUID( 123, 456 ) );
        connectRandomlyWithinServerGroupStrategy.inject( new TopologyServiceThatPrioritisesItself( myself, groupName ), config, NullLogProvider.getInstance(),
                myself );

        // when
        Optional<MemberId> result = connectRandomlyWithinServerGroupStrategy.upstreamDatabase();

        // then
        Assert.assertTrue( result.isPresent() );
        Assert.assertNotEquals( myself, result.get() );
    }
}
