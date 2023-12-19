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
package org.neo4j.causalclustering.upstream;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategy;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.iterable;

public class UpstreamDatabaseStrategySelectorTest
{
    private MemberId dummyMemberId = new MemberId( UUID.randomUUID() );

    @Test
    public void shouldReturnTheMemberIdFromFirstSucessfulStrategy() throws Exception
    {
        // given
        UpstreamDatabaseSelectionStrategy badOne = mock( UpstreamDatabaseSelectionStrategy.class );
        when( badOne.upstreamDatabase() ).thenReturn( Optional.empty() );

        UpstreamDatabaseSelectionStrategy anotherBadOne = mock( UpstreamDatabaseSelectionStrategy.class );
        when( anotherBadOne.upstreamDatabase() ).thenReturn( Optional.empty() );

        UpstreamDatabaseSelectionStrategy goodOne = mock( UpstreamDatabaseSelectionStrategy.class );
        MemberId theMemberId = new MemberId( UUID.randomUUID() );
        when( goodOne.upstreamDatabase() ).thenReturn( Optional.of( theMemberId ) );

        UpstreamDatabaseStrategySelector selector =
                new UpstreamDatabaseStrategySelector( badOne, iterable( goodOne, anotherBadOne ), NullLogProvider.getInstance() );

        // when
        MemberId result = selector.bestUpstreamDatabase();

        // then
        assertEquals( theMemberId, result );
    }

    @Test
    public void shouldDefaultToRandomCoreServerIfNoOtherStrategySpecified() throws Exception
    {
        // given
        TopologyService topologyService = mock( TopologyService.class );
        MemberId memberId = new MemberId( UUID.randomUUID() );
        when( topologyService.localCoreServers() ).thenReturn(
                new CoreTopology( new ClusterId( UUID.randomUUID() ), false, mapOf( memberId, mock( CoreServerInfo.class ) ) ) );

        ConnectToRandomCoreServerStrategy defaultStrategy = new ConnectToRandomCoreServerStrategy();
        defaultStrategy.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), null );

        UpstreamDatabaseStrategySelector selector = new UpstreamDatabaseStrategySelector( defaultStrategy );

        // when
        MemberId instance = selector.bestUpstreamDatabase();

        // then
        assertEquals( memberId, instance );
    }

    @Test
    public void shouldUseSpecifiedStrategyInPreferenceToDefault() throws Exception
    {
        // given
        TopologyService topologyService = mock( TopologyService.class );
        MemberId memberId = new MemberId( UUID.randomUUID() );
        when( topologyService.localCoreServers() ).thenReturn(
                new CoreTopology( new ClusterId( UUID.randomUUID() ), false, mapOf( memberId, mock( CoreServerInfo.class ) ) ) );

        ConnectToRandomCoreServerStrategy shouldNotUse = mock( ConnectToRandomCoreServerStrategy.class );

        UpstreamDatabaseSelectionStrategy mockStrategy = mock( UpstreamDatabaseSelectionStrategy.class );
        when( mockStrategy.upstreamDatabase() ).thenReturn( Optional.of( new MemberId( UUID.randomUUID() ) ) );

        UpstreamDatabaseStrategySelector selector =
                new UpstreamDatabaseStrategySelector( shouldNotUse, iterable( mockStrategy ), NullLogProvider.getInstance() );

        // when
        selector.bestUpstreamDatabase();

        // then
        verifyZeroInteractions( shouldNotUse );
    }

    @Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
    public static class DummyUpstreamDatabaseSelectionStrategy extends UpstreamDatabaseSelectionStrategy
    {
        private MemberId memberId;

        public DummyUpstreamDatabaseSelectionStrategy()
        {
            super( "dummy" );
        }

        @Override
        public Optional<MemberId> upstreamDatabase()
        {
            return Optional.ofNullable( memberId );
        }

        public void setMemberId( MemberId memberId )
        {
            this.memberId = memberId;
        }
    }

    @Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
    public static class AnotherDummyUpstreamDatabaseSelectionStrategy extends UpstreamDatabaseSelectionStrategy
    {
        public AnotherDummyUpstreamDatabaseSelectionStrategy()
        {
            super( "another-dummy" );
        }

        @Override
        public Optional<MemberId> upstreamDatabase()
        {
            return Optional.of( new MemberId( UUID.randomUUID() ) );
        }
    }

    @Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
    public static class YetAnotherDummyUpstreamDatabaseSelectionStrategy extends UpstreamDatabaseSelectionStrategy
    {
        public YetAnotherDummyUpstreamDatabaseSelectionStrategy()
        {
            super( "yet-another-dummy" );
        }

        @Override
        public Optional<MemberId> upstreamDatabase()
        {
            return Optional.of( new MemberId( UUID.randomUUID() ) );
        }
    }

    private Map<MemberId,CoreServerInfo> mapOf( MemberId memberId, CoreServerInfo coreServerInfo )
    {
        HashMap<MemberId,CoreServerInfo> map = new HashMap<>();

        map.put( memberId, coreServerInfo );

        return map;
    }
}
