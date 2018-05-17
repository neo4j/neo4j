/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategyTest.fakeCoreTopology;
import static org.neo4j.causalclustering.upstream.strategies.UserDefinedConfigurationStrategyTest.fakeReadReplicaTopology;
import static org.neo4j.causalclustering.upstream.strategies.UserDefinedConfigurationStrategyTest.fakeTopologyService;
import static org.neo4j.causalclustering.upstream.strategies.UserDefinedConfigurationStrategyTest.memberIDs;

public class TypicallyConnectToRandomReadReplicaStrategyTest
{
    @Test
    public void shouldConnectToCoreOneInTenTimesByDefault()
    {
        // given
        MemberId theCoreMemberId = new MemberId( UUID.randomUUID() );
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( theCoreMemberId ), fakeReadReplicaTopology( memberIDs( 100 ) ) );

        Config config = mock( Config.class );
        when( config.get( CausalClusteringSettings.database ) ).thenReturn( "default" );

        TypicallyConnectToRandomReadReplicaStrategy connectionStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 2 );
        connectionStrategy.inject( topologyService, config, NullLogProvider.getInstance(), null );

        List<MemberId> responses = new ArrayList<>();

        // when
        for ( int i = 0; i < 3; i++ )
        {
            for ( int j = 0; j < 2; j++ )
            {
                responses.add( connectionStrategy.upstreamDatabase().get() );
            }
            assertThat( responses, hasItem( theCoreMemberId ) );
            responses.clear();
        }

        // then
    }
}
