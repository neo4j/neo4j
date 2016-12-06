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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.neo4j.causalclustering.discovery.CoreAddresses;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.Service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.iterable;

public class UpstreamDatabaseStrategySelectorTest
{
    private MemberId dummyMemberId = new MemberId( UUID.randomUUID() );

    @Test
    public void shouldReturnTheMemberIdFromFirstStrategyThatDoesNotThrowAnException() throws Exception
    {
        // given
        UpstreamDatabaseSelectionStrategy badOne = mock( UpstreamDatabaseSelectionStrategy.class );
        when( badOne.upstreamDatabase() ).thenThrow( new UpstreamDatabaseSelectionException( "bad times" ) );

        UpstreamDatabaseSelectionStrategy anotherBadOne = mock( UpstreamDatabaseSelectionStrategy.class );
        when( anotherBadOne.upstreamDatabase() )
                .thenThrow( new UpstreamDatabaseSelectionException( "more bad times" ) );

        UpstreamDatabaseSelectionStrategy goodOne = mock( UpstreamDatabaseSelectionStrategy.class );
        MemberId theMemberId = new MemberId( UUID.randomUUID() );
        when( goodOne.upstreamDatabase() ).thenReturn( theMemberId );

        UpstreamDatabaseStrategySelector selector =
                new UpstreamDatabaseStrategySelector( badOne, iterable( goodOne, anotherBadOne ) );

        // when
        MemberId result = selector.bestUpstreamDatabase();

        // then
        assertEquals( theMemberId, result );
    }

    @Test
    public void shouldDefaultToRandomCoreServerAndLogIfCannotLoadClassFromConfig() throws Exception
    {
        // given
        TopologyService topologyService = mock( TopologyService.class );
        MemberId memberId = new MemberId( UUID.randomUUID() );
        when( topologyService.coreServers() ).thenReturn( new CoreTopology( new ClusterId( UUID.randomUUID() ), false,
                mapOf( memberId, mock( CoreAddresses.class ) ) ) );

        ConnectToRandomUpstreamCoreServer defaultStrategy = new ConnectToRandomUpstreamCoreServer();
        defaultStrategy.setDiscoveryService( topologyService );

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
        when( topologyService.coreServers() ).thenReturn( new CoreTopology( new ClusterId( UUID.randomUUID() ), false,
                mapOf( memberId, mock( CoreAddresses.class ) ) ) );

        ConnectToRandomUpstreamCoreServer defaultStrategy = new ConnectToRandomUpstreamCoreServer();
        defaultStrategy.setDiscoveryService( topologyService );

        DummyUpstreamDatabaseSelectionStrategy stub = new DummyUpstreamDatabaseSelectionStrategy();
        stub.setMemberId( memberId );

        UpstreamDatabaseStrategySelector selector =
                new UpstreamDatabaseStrategySelector( defaultStrategy, iterable( stub ) );

        // when
        MemberId instance = selector.bestUpstreamDatabase();

        // then
        assertEquals( memberId, instance );
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
        public MemberId upstreamDatabase() throws UpstreamDatabaseSelectionException
        {
            return memberId;
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
        public MemberId upstreamDatabase() throws UpstreamDatabaseSelectionException
        {
            return new MemberId( UUID.randomUUID() );
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
        public MemberId upstreamDatabase() throws UpstreamDatabaseSelectionException
        {
            return new MemberId( UUID.randomUUID() );
        }
    }

    private Map<MemberId,CoreAddresses> mapOf( MemberId memberId, CoreAddresses coreAddresses )
    {
        HashMap<MemberId,CoreAddresses> map = new HashMap<>();

        map.put( memberId, coreAddresses );

        return map;
    }
}
