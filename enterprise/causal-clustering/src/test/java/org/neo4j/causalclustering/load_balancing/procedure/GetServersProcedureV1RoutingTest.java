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
package org.neo4j.causalclustering.load_balancing.procedure;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class GetServersProcedureV1RoutingTest
{
    private ClusterId clusterId = new ClusterId( UUID.randomUUID() );
    private Config config = Config.defaults();

    @ParameterizedTest
    @ValueSource( ints = {1, 2} )
    public void shouldReturnEndpointsInDifferentOrders( int serverClass ) throws Exception
    {
        // given
        final CoreTopologyService coreTopologyService = mock( CoreTopologyService.class );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( member( 0 ) );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( member( 0 ), addressesForCore( 0 ) );
        coreMembers.put( member( 1 ), addressesForCore( 1 ) );
        coreMembers.put( member( 2 ), addressesForCore( 2 ) );

        final CoreTopology clusterTopology = new CoreTopology( clusterId, false, coreMembers );
        when( coreTopologyService.coreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        final LegacyGetServersProcedure proc =
                new LegacyGetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        Object[] endpoints = getEndpoints( proc, serverClass );

        //then
        Object[] endpointsInDifferentOrder = getEndpoints( proc, serverClass );
        for ( int i = 0; i < 100; i++ )
        {
            if ( Arrays.deepEquals( endpointsInDifferentOrder, endpoints ) )
            {
                endpointsInDifferentOrder = getEndpoints( proc, serverClass );
            }
            else
            {
                //Different order of servers, no need to retry.
                break;
            }
        }
        assertFalse( Arrays.deepEquals( endpoints, endpointsInDifferentOrder ) );
    }

    private Object[] getEndpoints( LegacyGetServersProcedure proc, int serverClass )
            throws org.neo4j.kernel.api.exceptions.ProcedureException
    {
        List<Object[]> results = asList( proc.apply( null, new Object[0], null ) );
        Object[] rows = results.get( 0 );
        List<Map<String,Object[]>> servers = (List<Map<String,Object[]>>) rows[1];
        Map<String,Object[]> endpoints = servers.get( serverClass );
        return endpoints.get( "addresses" );
    }
}
