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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import org.junit.Test;

import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;

import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ClusterProtocolAtomicbroadcastTestUtil.ids;
import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ClusterProtocolAtomicbroadcastTestUtil.members;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AtomicBroadcastContextImplTest
{
    @Test
    public void shouldHasQuorumWhenTwoMachinesAliveInAClusterWithThreeMachines()
    {
        //Given
        HeartbeatContext heartbeatContext = mock( HeartbeatContext.class );
        CommonContextState commonState = mock( CommonContextState.class );
        ClusterConfiguration configuration = mock(ClusterConfiguration.class);

        when( heartbeatContext.getAlive() ).thenReturn( ids( 2 ) );
        when( commonState.configuration() ).thenReturn( configuration );
        when( configuration.getMembers() ).thenReturn( members( 3 ) );

        AtomicBroadcastContextImpl context = new AtomicBroadcastContextImpl( null, commonState, null, null, null,
                heartbeatContext ); // we do not care about other args
        //When
        boolean hasQuorum = context.hasQuorum();
        //Then
        assertTrue( hasQuorum );
    }

    @Test
    public void shouldHasNoQuorumWhenOneMachineAliveInAClusterWithThreeMachines()
    {
        //Given
        HeartbeatContext heartbeatContext = mock( HeartbeatContext.class );
        CommonContextState commonState = mock( CommonContextState.class );
        ClusterConfiguration configuration = mock( ClusterConfiguration.class );

        when( heartbeatContext.getAlive() ).thenReturn( ids( 1 ) );
        when( commonState.configuration() ).thenReturn( configuration );
        when( configuration.getMembers() ).thenReturn( members( 3 ) );

        AtomicBroadcastContextImpl context = new AtomicBroadcastContextImpl( null, commonState, null, null, null,
                heartbeatContext ); // we do not care about other args
        //When
        boolean hasQuorum = context.hasQuorum();
        //Then
        assertFalse( hasQuorum );
    }

    @Test
    public void shouldHasQuorumWhenOneMachineAliveInAClusterWithOneMachine()
    {
        //Given
        HeartbeatContext heartbeatContext = mock( HeartbeatContext.class );
        CommonContextState commonState = mock( CommonContextState.class );
        ClusterConfiguration configuration = mock( ClusterConfiguration.class );

        when( heartbeatContext.getAlive() ).thenReturn( ids( 1 ) );
        when( commonState.configuration() ).thenReturn( configuration );
        when( configuration.getMembers() ).thenReturn( members( 1 ) );

        AtomicBroadcastContextImpl context = new AtomicBroadcastContextImpl( null, commonState, null, null, null,
                heartbeatContext ); // we do not care about other args
        //When
        boolean hasQuorum = context.hasQuorum();
        //Then
        assertTrue( hasQuorum );
    }
}
