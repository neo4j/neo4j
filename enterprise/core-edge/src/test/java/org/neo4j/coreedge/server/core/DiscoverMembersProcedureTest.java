/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.server.core;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.BoltAddress;
import org.neo4j.kernel.api.exceptions.ProcedureException;

import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.server.core.EnterpriseCoreEditionModule.ConsistencyLevel.RYOW_CORE;
import static org.neo4j.coreedge.server.core.EnterpriseCoreEditionModule.ConsistencyLevel.RYOW_EDGE;
import static org.neo4j.helpers.collection.Iterators.asList;

public class DiscoverMembersProcedureTest
{
    @Test
    public void shouldDiscoverCoreMachinesBoltAddresses() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        final ClusterTopology clusterTopology = mock( ClusterTopology.class );
        when( coreTopologyService.currentTopology() ).thenReturn( clusterTopology );

        when( clusterTopology.boltCoreMembers() ).thenReturn( boltCoreMembers( 1, 2, 3 ) );

        final DiscoverMembersProcedure proc = new DiscoverMembersProcedure( coreTopologyService );

        // when
        final List<Object[]> members = asList( proc.apply( null, new Object[]{RYOW_CORE.name()} ) );

        // then
        assertThat( members, containsInAnyOrder(
                new Object[]{"localhost:3001"},
                new Object[]{"localhost:3002"},
                new Object[]{"localhost:3003"} ) );
    }

    @Test
    public void shouldDiscoverEdgeMachinesBoltAddresses() throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        final ClusterTopology clusterTopology = mock( ClusterTopology.class );
        when( coreTopologyService.currentTopology() ).thenReturn( clusterTopology );

        when( clusterTopology.edgeMembers() ).thenReturn( edgeMembers( 1, 2, 3, 4, 5 ) );

        final DiscoverMembersProcedure proc = new DiscoverMembersProcedure( coreTopologyService );

        // when
        final List<Object[]> members = asList( proc.apply( null, new
                Object[]{RYOW_EDGE.name()} ) );

        // then
        assertThat( members, containsInAnyOrder(
                new Object[]{"localhost:3001"},
                new Object[]{"localhost:3002"},
                new Object[]{"localhost:3003"},
                new Object[]{"localhost:3004"},
                new Object[]{"localhost:3005"} ) );
    }

    @Test
    public void shouldThrowExceptionForUnknownConsistencyLevel() throws Exception
    {
        // given
        try
        {
            // when
            new DiscoverMembersProcedure( null ).apply( null, new Object[]{"FOOBAR"} );
            fail( "ProcedureException should have been thrown" );
        }
        catch ( ProcedureException expected )
        {
            // then
            assertThat( expected.getMessage(), containsString( RYOW_CORE.name() ) );
            assertThat( expected.getMessage(), containsString( RYOW_EDGE.name() ) );
        }
    }

    private Set<BoltAddress> edgeMembers( int... ids )
    {
        final HashSet<BoltAddress> edgeMembers = new HashSet<>();

        for ( int id : ids )
        {
            edgeMembers.add( new BoltAddress( new AdvertisedSocketAddress( "localhost:" + (3000 + id) ) ) );
        }

        return edgeMembers;
    }

    private Set<BoltAddress> boltCoreMembers( int... ids )
    {
        final HashSet<BoltAddress> coreMembers = new HashSet<>();

        for ( int id : ids )
        {
            coreMembers.add( new BoltAddress( new AdvertisedSocketAddress( "localhost:" + (3000 + id) ) ) );
        }

        return coreMembers;
    }
}
