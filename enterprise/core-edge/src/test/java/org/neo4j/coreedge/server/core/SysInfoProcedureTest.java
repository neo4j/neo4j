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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;

import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.CoreAddresses;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.EdgeAddresses;
import org.neo4j.coreedge.raft.LeaderLocator;
import org.neo4j.coreedge.server.CoreMember;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.server.core.DiscoverMembersProcedureTest.addresses;
import static org.neo4j.coreedge.server.core.DiscoverMembersProcedureTest.coreAddresses;
import static org.neo4j.helpers.collection.Iterators.asList;

public class SysInfoProcedureTest
{
    @Test
    public void shouldRecommendTheCoreLeaderForWriteAndEdgeForRead() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<CoreMember,CoreAddresses> coreMembers = new HashMap<>();
        CoreMember theLeader = new CoreMember( UUID.randomUUID() );
        CoreMember follower1 = new CoreMember( UUID.randomUUID() );
        CoreMember follower2 = new CoreMember( UUID.randomUUID() );

        coreMembers.put( theLeader, coreAddresses( 0 ) );
        coreMembers.put( follower1, coreAddresses( 1 ) );
        coreMembers.put( follower2, coreAddresses( 2 ) );

        Set<EdgeAddresses> edges = addresses( 4 );

        final ClusterTopology clusterTopology = new ClusterTopology( false, coreMembers, edges );
        when( topologyService.currentTopology() ).thenReturn( clusterTopology );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        SysInfoProcedure procedure = new SysInfoProcedure( topologyService, leaderLocator );

        // when
        final List<Object[]> members = asList( procedure.apply( null, new Object[0] ) );

        // then
        assertThat( members, containsInAnyOrder(
                new Object[]{theLeader.getUuid().toString(), "localhost:3000", "leader"},
                new Object[]{follower1.getUuid().toString(), "localhost:3001", "follower"},
                new Object[]{follower2.getUuid().toString(), "localhost:3002", "follower"},
                new Object[]{null, "localhost:3004", "read_replica"}
        ) );
    }
}
