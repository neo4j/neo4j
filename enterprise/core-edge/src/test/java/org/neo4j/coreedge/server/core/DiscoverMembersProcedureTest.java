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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.ReadOnlyTopologyService;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.BoltAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.Iterators.asList;

public class DiscoverMembersProcedureTest
{
    @Test
    public void shouldOnlyReturnCoreMembers() throws Exception
    {
        // given
        final ReadOnlyTopologyService coreTopologyService = mock( ReadOnlyTopologyService.class );

        Map<CoreMember, BoltAddress> coreMembers = new HashMap<>();
        coreMembers.put( coreMemberAtRaftPort( 9000 ), address( 1 ) );
        coreMembers.put( coreMemberAtRaftPort( 9001 ), address( 2 ) );
        coreMembers.put( coreMemberAtRaftPort( 9002 ), address( 3 ) );

        final ClusterTopology clusterTopology = new ClusterTopology( false, coreMembers, addresses( 4, 5, 6 ) );
        when( coreTopologyService.currentTopology() ).thenReturn( clusterTopology );

        final DiscoverMembersProcedure proc = new DiscoverMembersProcedure( coreTopologyService, NullLogProvider
                .getInstance() );

        // when
        final List<Object[]> members = asList( proc.apply( null, new Object[0] ) );

        // then
        assertThat( members, containsInAnyOrder(
                new Object[]{address( 1 ).getAdvertisedAddress().toString()},
                new Object[]{address( 2 ).getAdvertisedAddress().toString()},
                new Object[]{address( 3 ).getAdvertisedAddress().toString()} ) );
    }

    @Test
    public void shouldReturnSelfIfOnlyMemberOfTheCluster() throws Exception
    {
        final ReadOnlyTopologyService coreTopologyService = mock( ReadOnlyTopologyService.class );

        Map<CoreMember, BoltAddress> coreMembers = new HashMap<>();
        coreMembers.put( coreMemberAtRaftPort( 9000 ), address( 1 ) );

        final ClusterTopology clusterTopology = new ClusterTopology( false, coreMembers, addresses( 4, 5, 6 ) );
        when( coreTopologyService.currentTopology() ).thenReturn( clusterTopology );
        final DiscoverMembersProcedure proc = new DiscoverMembersProcedure( coreTopologyService, NullLogProvider
                .getInstance() );

        // when
        final List<Object[]> members = asList( proc.apply( null, new Object[0] ) );

        // then
        assertArrayEquals( members.get( 0 ), new Object[]{address( 1 ).getAdvertisedAddress().toString()} );
    }

    @Test
    public void shouldReturnLimitedNumberOfAddresses() throws Exception
    {
        // given
        final ReadOnlyTopologyService coreTopologyService = mock( ReadOnlyTopologyService.class );

        Map<CoreMember, BoltAddress> coreMembers = new HashMap<>();
        coreMembers.put( coreMemberAtRaftPort( 9000 ), address( 1 ) );
        coreMembers.put( coreMemberAtRaftPort( 9001 ), address( 2 ) );
        coreMembers.put( coreMemberAtRaftPort( 9002 ), address( 3 ) );

        final ClusterTopology clusterTopology = new ClusterTopology( false, coreMembers, addresses( 4, 5, 6 ) );
        when( coreTopologyService.currentTopology() ).thenReturn( clusterTopology );

        final DiscoverMembersProcedure proc = new DiscoverMembersProcedure( coreTopologyService, NullLogProvider
                .getInstance() );

        // when
        final List<Object[]> members = asList( proc.apply( null, new Object[]{1} ) );

        // then
        assertEquals( 1, members.size() );
    }

    @Test
    public void shouldReturnAllAddressesForStupidLimit() throws Exception
    {
        // given
        final ReadOnlyTopologyService coreTopologyService = mock( ReadOnlyTopologyService.class );

        Map<CoreMember, BoltAddress> coreMembers = new HashMap<>(  );
        coreMembers.put( coreMemberAtRaftPort( 9000 ), address( 1 ) );
        coreMembers.put( coreMemberAtRaftPort( 9001 ), address( 2 ) );
        coreMembers.put( coreMemberAtRaftPort( 9002 ), address( 3 ) );

        final ClusterTopology clusterTopology = new ClusterTopology( false, coreMembers, addresses( 4, 5, 6 ) );
        when( coreTopologyService.currentTopology() ).thenReturn( clusterTopology );

        final DiscoverMembersProcedure proc = new DiscoverMembersProcedure( coreTopologyService, NullLogProvider
                .getInstance() );

        // when
        final List<Object[]> members = asList( proc.apply( null, new Object[]{"bam"} ) );

        // then
        assertEquals( 3, members.size() );
    }

    static CoreMember coreMemberAtRaftPort( int raftPort )
    {
        return new CoreMember(
                null,
                new AdvertisedSocketAddress( "127.0.0.1:" + raftPort ) );
    }

    static Set<BoltAddress> addresses( int... ids )
    {
        return Arrays.stream( ids ).mapToObj( DiscoverMembersProcedureTest::address ).collect( Collectors.toSet() );
    }

    static BoltAddress address( int id )
    {
        return new BoltAddress( new AdvertisedSocketAddress( "localhost:" + (3000 + id) ) );
    }
}
