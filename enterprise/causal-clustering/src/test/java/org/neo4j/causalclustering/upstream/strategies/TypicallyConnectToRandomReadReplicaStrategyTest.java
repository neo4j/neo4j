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

import org.hamcrest.core.IsNot;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategyTest.fakeCoreTopology;
import static org.neo4j.causalclustering.upstream.strategies.UserDefinedConfigurationStrategyTest.fakeReadReplicaTopology;
import static org.neo4j.causalclustering.upstream.strategies.UserDefinedConfigurationStrategyTest.fakeTopologyService;
import static org.neo4j.causalclustering.upstream.strategies.UserDefinedConfigurationStrategyTest.memberIDs;
import static org.neo4j.cypher.internal.codegen.CompiledConversionUtils.not;

public class TypicallyConnectToRandomReadReplicaStrategyTest
{
    MemberId myself = new MemberId( new UUID( 1234, 5678 ) );

    @Test
    public void shouldConnectToCoreOneInTenTimesByDefault()
    {
        // given
        MemberId theCoreMemberId = new MemberId( UUID.randomUUID() );
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( theCoreMemberId ), fakeReadReplicaTopology( memberIDs( 100 ) ) );

        Config config = mock( Config.class );
        when( config.get( CausalClusteringSettings.database ) ).thenReturn( "default" );

        TypicallyConnectToRandomReadReplicaStrategy connectionStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 2 );
        connectionStrategy.inject( topologyService, config, NullLogProvider.getInstance(), myself );

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

    @Test
    public void filtersSelf()
    {
        // given
        String groupName = "groupName";
        Config config = Config.defaults();

        TypicallyConnectToRandomReadReplicaStrategy typicallyConnectToRandomReadReplicaStrategy = new TypicallyConnectToRandomReadReplicaStrategy();
        typicallyConnectToRandomReadReplicaStrategy.inject( new TopologyServiceThatPrioritisesItself( myself, groupName ), config,
                NullLogProvider.getInstance(), myself );

        // when
        Optional<MemberId> found = typicallyConnectToRandomReadReplicaStrategy.upstreamDatabase();

        // then
        assertTrue( found.isPresent() );
        assertNotEquals( myself, found );
    }

    @Test
    public void onCounterTriggerFiltersSelf()
    {
        // given counter always triggers to get a core member
        TypicallyConnectToRandomReadReplicaStrategy connectionStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 1 );

        // and requesting core member will return self and another member
        MemberId otherCoreMember = new MemberId( new UUID( 12, 34 ) );
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( myself, otherCoreMember ), fakeReadReplicaTopology( memberIDs( 2 ) ) );
        connectionStrategy.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), myself );

        // when
        Optional<MemberId> found = connectionStrategy.upstreamDatabase();

        // then
        assertTrue( found.isPresent() );
        assertNotEquals( myself, found.get() );
    }

    @Test
    public void randomCoreDoesNotReturnSameCoreTwice()
    {
        // given counter always core member
        TypicallyConnectToRandomReadReplicaStrategy connectionStrategy = new TypicallyConnectToRandomReadReplicaStrategy( 1 );

        // and
        MemberId firstOther = new MemberId( new UUID( 12, 34 ) );
        MemberId secondOther = new MemberId( new UUID( 56, 78 ) );
        TopologyService topologyService = fakeTopologyService( fakeCoreTopology( myself, firstOther, secondOther ), fakeReadReplicaTopology( memberIDs( 2 ) ) );
        connectionStrategy.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), myself );

        // when we collect enough results to feel confident of random values
        List<MemberId> found = IntStream.range( 0, 20 )
                .mapToObj( i -> connectionStrategy.upstreamDatabase() )
                .filter( Optional::isPresent )
                .map( Optional::get )
                .collect( Collectors.toList() );

        // then
        assertFalse( found.contains( myself ) );
        assertTrue( found.contains( firstOther ) );
        assertTrue( found.contains( secondOther ) );
    }
}
