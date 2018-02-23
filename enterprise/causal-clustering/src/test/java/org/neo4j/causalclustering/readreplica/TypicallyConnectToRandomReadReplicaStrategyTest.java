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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.causalclustering.readreplica.ConnectToRandomCoreServerStrategyTest.fakeCoreTopology;
import static org.neo4j.causalclustering.readreplica.UserDefinedConfigurationStrategyTest.fakeReadReplicaTopology;
import static org.neo4j.causalclustering.readreplica.UserDefinedConfigurationStrategyTest.fakeTopologyService;
import static org.neo4j.causalclustering.readreplica.UserDefinedConfigurationStrategyTest.memberIDs;

public class TypicallyConnectToRandomReadReplicaStrategyTest
{
    @Test
    public void shouldConnectToCoreOneInTenTimesByDefault()
    {
        // given
        MemberId theCoreMemberId = new MemberId( UUID.randomUUID() );
        TopologyService topologyService =
                fakeTopologyService( fakeCoreTopology( theCoreMemberId ), fakeReadReplicaTopology( memberIDs( 100 ) ) );

        TypicallyConnectToRandomReadReplicaStrategy connectionStrategy =
                new TypicallyConnectToRandomReadReplicaStrategy();
        connectionStrategy.inject( topologyService, null, NullLogProvider.getInstance(), null );

        List<MemberId> responses = new ArrayList<>();

        // when
        for ( int i = 0; i < 10; i++ )
        {
            responses.add( connectionStrategy.upstreamDatabase().get() );
        }

        // then
        assertThat( responses, hasItem( theCoreMemberId ) );
    }
}
