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
package org.neo4j.kernel.ha.cluster.member;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.store.StoreId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterMembersTest
{
    private final ObservedClusterMembers observedClusterMembers = mock( ObservedClusterMembers.class );
    private final HighAvailabilityMemberStateMachine stateMachine = mock( HighAvailabilityMemberStateMachine.class );
    private final ClusterMembers clusterMembers = new ClusterMembers( observedClusterMembers, stateMachine );

    @Test
    public void currentInstanceStateUpdated()
    {
        ClusterMember currentInstance = createClusterMember( 1, HighAvailabilityModeSwitcher.UNKNOWN );

        when( observedClusterMembers.getAliveMembers() ).thenReturn( Collections.singletonList( currentInstance ) );
        when( observedClusterMembers.getCurrentMember() ).thenReturn( currentInstance );
        when( stateMachine.getCurrentState() ).thenReturn( HighAvailabilityMemberState.MASTER );

        ClusterMember self = clusterMembers.getCurrentMember();
        assertEquals( HighAvailabilityModeSwitcher.MASTER, self.getHARole() );
    }

    @Test
    public void aliveMembersWithValidCurrentInstanceState()
    {
        ClusterMember currentInstance = createClusterMember( 1, HighAvailabilityModeSwitcher.UNKNOWN );
        ClusterMember otherInstance = createClusterMember( 2, HighAvailabilityModeSwitcher.SLAVE );
        List<ClusterMember> members = Arrays.asList( currentInstance, otherInstance );

        when( observedClusterMembers.getAliveMembers() ).thenReturn( members );
        when( observedClusterMembers.getCurrentMember() ).thenReturn( currentInstance );
        when( stateMachine.getCurrentState() ).thenReturn( HighAvailabilityMemberState.MASTER );

        Iterable<ClusterMember> currentMembers = clusterMembers.getAliveMembers();

        assertEquals( "Only active members should be available", 2, Iterables.count( currentMembers ) );
        assertEquals( 1, countInstancesWithRole( currentMembers, HighAvailabilityModeSwitcher.MASTER ) );
        assertEquals( 1, countInstancesWithRole( currentMembers, HighAvailabilityModeSwitcher.SLAVE ) );
    }

    @Test
    public void observedStateDoesNotKnowCurrentInstance()
    {
        ClusterMember currentInstance = createClusterMember( 1, HighAvailabilityModeSwitcher.SLAVE );
        ClusterMember otherInstance = createClusterMember( 2, HighAvailabilityModeSwitcher.MASTER );
        List<ClusterMember> members = Arrays.asList( currentInstance, otherInstance );

        when( observedClusterMembers.getMembers() ).thenReturn( members );
        when( observedClusterMembers.getCurrentMember() ).thenReturn( null );
        when( stateMachine.getCurrentState() ).thenReturn( HighAvailabilityMemberState.SLAVE );

        assertNull( clusterMembers.getCurrentMember() );
        assertEquals( members, clusterMembers.getMembers() );
    }

    @Test
    public void incorrectlyObservedCurrentInstanceStateUpdated()
    {
        ClusterMember currentInstance = createClusterMember( 1, HighAvailabilityModeSwitcher.SLAVE );
        ClusterMember otherInstance = createClusterMember( 2, HighAvailabilityModeSwitcher.MASTER );
        List<ClusterMember> members = Arrays.asList( currentInstance, otherInstance );

        when( observedClusterMembers.getMembers() ).thenReturn( members );
        when( observedClusterMembers.getCurrentMember() ).thenReturn( currentInstance );
        when( stateMachine.getCurrentState() ).thenReturn( HighAvailabilityMemberState.MASTER );

        Iterable<ClusterMember> currentMembers = clusterMembers.getMembers();

        assertEquals( "All members should be available", 2, Iterables.count( currentMembers ) );
        assertEquals( 2, countInstancesWithRole( currentMembers, HighAvailabilityModeSwitcher.MASTER ) );
    }

    @Test
    public void currentMemberHasCorrectRoleWhenInPendingState()
    {
        ClusterMember member = createClusterMember( 1, HighAvailabilityModeSwitcher.MASTER );

        when( observedClusterMembers.getCurrentMember() ).thenReturn( member );
        when( stateMachine.getCurrentState() ).thenReturn( HighAvailabilityMemberState.PENDING );

        assertEquals( HighAvailabilityModeSwitcher.UNKNOWN, clusterMembers.getCurrentMemberRole() );
    }

    @Test
    public void currentMemberHasCorrectRoleWhenInToSlaveState()
    {
        ClusterMember member = createClusterMember( 1, HighAvailabilityModeSwitcher.MASTER );

        when( observedClusterMembers.getCurrentMember() ).thenReturn( member );
        when( stateMachine.getCurrentState() ).thenReturn( HighAvailabilityMemberState.TO_SLAVE );

        assertEquals( HighAvailabilityModeSwitcher.UNKNOWN, clusterMembers.getCurrentMemberRole() );
    }

    @Test
    public void currentMemberHasCorrectRoleWhenInToMasterState()
    {
        ClusterMember member = createClusterMember( 1, HighAvailabilityModeSwitcher.MASTER );

        when( observedClusterMembers.getCurrentMember() ).thenReturn( member );
        when( stateMachine.getCurrentState() ).thenReturn( HighAvailabilityMemberState.TO_MASTER );

        assertEquals( HighAvailabilityModeSwitcher.UNKNOWN, clusterMembers.getCurrentMemberRole() );
    }

    private static int countInstancesWithRole( Iterable<ClusterMember> currentMembers, String role )
    {
        int counter = 0;
        for ( ClusterMember clusterMember : currentMembers )
        {
            if ( role.equals( clusterMember.getHARole() ) )
            {
                counter++;
            }
        }
        return counter;
    }

    private static ClusterMember createClusterMember( int id, String role )
    {
        ClusterMember member = new ClusterMember( new InstanceId( id ) );
        return member.availableAs( role, null, StoreId.DEFAULT );
    }
}
