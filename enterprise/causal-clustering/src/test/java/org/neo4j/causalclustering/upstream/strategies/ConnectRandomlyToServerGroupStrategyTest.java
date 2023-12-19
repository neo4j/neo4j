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

import org.junit.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static co.unruly.matchers.OptionalMatchers.contains;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.upstream.strategies.UserDefinedConfigurationStrategyTest.memberIDs;

public class ConnectRandomlyToServerGroupStrategyTest
{

    @Test
    public void shouldConnectToGroupDefinedInStrategySpecificConfig()
    {
        // given
        final String targetServerGroup = "target_server_group";
        Config configWithTargetServerGroup = Config.defaults( CausalClusteringSettings.connect_randomly_to_server_group_strategy, targetServerGroup );
        MemberId[] targetGroupMemberIds = memberIDs( 10 );
        TopologyService topologyService =
                ConnectRandomlyToServerGroupStrategyImplTest.getTopologyService( Collections.singletonList( targetServerGroup ), targetGroupMemberIds,
                        Collections.singletonList( "your_server_group" ) );

        ConnectRandomlyToServerGroupStrategy strategy = new ConnectRandomlyToServerGroupStrategy();
        strategy.inject( topologyService, configWithTargetServerGroup, NullLogProvider.getInstance(), targetGroupMemberIds[0] );

        // when
        Optional<MemberId> result = strategy.upstreamDatabase();

        // then
        assertThat( result, contains( isIn( targetGroupMemberIds ) ) );
    }

    @Test
    public void doesNotConnectToSelf()
    {
        // given
        ConnectRandomlyToServerGroupStrategy connectRandomlyToServerGroupStrategy = new ConnectRandomlyToServerGroupStrategy();
        MemberId myself = new MemberId( new UUID( 1234, 5678 ) );

        // and
        LogProvider logProvider = NullLogProvider.getInstance();
        Config config = Config.defaults();
        config.augment( CausalClusteringSettings.connect_randomly_to_server_group_strategy, "firstGroup" );
        TopologyService topologyService = new TopologyServiceThatPrioritisesItself( myself, "firstGroup" );
        connectRandomlyToServerGroupStrategy.inject( topologyService, config, logProvider, myself );

        // when
        Optional<MemberId> found = connectRandomlyToServerGroupStrategy.upstreamDatabase();

        // then
        assertTrue( found.isPresent() );
        assertNotEquals( myself, found.get() );
    }
}
