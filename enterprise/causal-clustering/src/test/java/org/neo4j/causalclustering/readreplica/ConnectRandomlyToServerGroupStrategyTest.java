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
package org.neo4j.causalclustering.readreplica;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static co.unruly.matchers.OptionalMatchers.contains;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.causalclustering.readreplica.UserDefinedConfigurationStrategyTest.memberIDs;

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
}
