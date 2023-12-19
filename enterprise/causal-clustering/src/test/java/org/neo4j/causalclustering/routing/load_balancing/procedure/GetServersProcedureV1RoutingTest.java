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
package org.neo4j.causalclustering.routing.load_balancing.procedure;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.Arrays;
import java.util.Collection;
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
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.configuration.Config;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertFalse;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.logging.NullLogProvider.getInstance;

@RunWith( Parameterized.class )
public class GetServersProcedureV1RoutingTest
{
    @Parameters
    public static Collection<Object> data()
    {
        return Arrays.asList( 1, 2 );
    } //the write endpoints are always index 0

    @Parameter
    public int serverClass;

    private ClusterId clusterId = new ClusterId( UUID.randomUUID() );
    private Config config = Config.defaults();

    @Test
    public void shouldReturnEndpointsInDifferentOrders() throws Exception
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
        when( coreTopologyService.localCoreServers() ).thenReturn( clusterTopology );
        when( coreTopologyService.localReadReplicas() ).thenReturn( new ReadReplicaTopology( emptyMap() ) );

        final LegacyGetServersProcedure proc =
                new LegacyGetServersProcedure( coreTopologyService, leaderLocator, config, getInstance() );

        // when
        Object[] endpoints = getEndpoints( proc );

        //then
        Object[] endpointsInDifferentOrder = getEndpoints( proc );
        for ( int i = 0; i < 100; i++ )
        {
            if ( Arrays.deepEquals( endpointsInDifferentOrder, endpoints ) )
            {
                endpointsInDifferentOrder = getEndpoints( proc );
            }
            else
            {
                //Different order of servers, no need to retry.
                break;
            }
        }
        assertFalse( Arrays.deepEquals( endpoints, endpointsInDifferentOrder ) );
    }

    private Object[] getEndpoints( LegacyGetServersProcedure proc )
            throws ProcedureException
    {
        List<Object[]> results = asList( proc.apply( null, new Object[0], null ) );
        Object[] rows = results.get( 0 );
        List<Map<String,Object[]>> servers = (List<Map<String,Object[]>>) rows[1];
        Map<String,Object[]> endpoints = servers.get( serverClass );
        return endpoints.get( "addresses" );
    }
}
