/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.cluster.ClusterInstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class CommonContextStateTest
{

    @Test
    public void shouldOnlyAllowHigherLastLearnedInstanceId() throws Exception
    {
        // Given
        CommonContextState state = new CommonContextState( mock( ClusterConfiguration.class) );

        // When
        state.setLastKnownLearnedInstanceInCluster( 1, new ClusterInstanceId( 2 ) );
        state.setLastKnownLearnedInstanceInCluster( 0, new ClusterInstanceId( 3 ) );

        // Then
        assertThat( state.lastKnownLearnedInstanceInCluster(), equalTo( 1l ) );
    }

    @Test
    public void shouldTrackLastKnownUpToDateAliveInstance() throws Exception
    {
        // Given
        CommonContextState state = new CommonContextState( mock( ClusterConfiguration.class) );

        // When
        state.setLastKnownLearnedInstanceInCluster( 1, new ClusterInstanceId( 2 ) );
        state.setLastKnownLearnedInstanceInCluster( 1, new ClusterInstanceId( 3 ) );
        state.setLastKnownLearnedInstanceInCluster( 0, new ClusterInstanceId( 4 ) );

        // Then
        assertThat( state.lastKnownLearnedInstanceInCluster(), equalTo( 1l ) );
        assertThat( state.lastKnownAliveUpToDateInstance(), equalTo( new ClusterInstanceId( 3 ) ));
    }

    @Test
    public void settingLastLearnedInstanceToNegativeOneShouldAlwaysWin() throws Exception
    {
        // Given
        CommonContextState state = new CommonContextState( mock( ClusterConfiguration.class) );

        // When
        state.setLastKnownLearnedInstanceInCluster( 1, new ClusterInstanceId( 2 ) );
        state.setLastKnownLearnedInstanceInCluster( -1, null );

        // Then
        assertThat( state.lastKnownLearnedInstanceInCluster(), equalTo( -1l ) );
        assertThat( state.lastKnownAliveUpToDateInstance(), equalTo( new ClusterInstanceId( 2 ) ));
    }

}
