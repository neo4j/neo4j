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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context.MultiPaxosContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LearnerContextTest
{
    private final LogProvider logProvider = new AssertableLogProvider();

    @Test
    public void shouldOnlyAllowHigherLastLearnedInstanceId()
    {
        // Given

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext mpCtx = new MultiPaxosContext( null, Iterables.empty(),
                mock( ClusterConfiguration.class ), null, NullLogProvider.getInstance(),
                null, null, null, null, null,
                config );
        LearnerContext state = mpCtx.getLearnerContext();

        // When
        state.setLastKnownLearnedInstanceInCluster( 1, new InstanceId( 2 ) );
        state.setLastKnownLearnedInstanceInCluster( 0, new InstanceId( 3 ) );

        // Then
        assertThat( state.getLastKnownLearnedInstanceInCluster(), equalTo( 1L ) );
    }

    @Test
    public void shouldTrackLastKnownUpToDateAliveInstance()
    {
        // Given

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext mpCtx = new MultiPaxosContext( null, Iterables.empty(),
                mock( ClusterConfiguration.class ), null, NullLogProvider.getInstance(),
                null, null, null, null, null,
                config );
        LearnerContext state = mpCtx.getLearnerContext();

        // When
        state.setLastKnownLearnedInstanceInCluster( 1, new InstanceId( 2 ) );
        state.setLastKnownLearnedInstanceInCluster( 1, new InstanceId( 3 ) );
        state.setLastKnownLearnedInstanceInCluster( 0, new InstanceId( 4 ) );

        // Then
        assertThat( state.getLastKnownLearnedInstanceInCluster(), equalTo( 1L ) );
        assertThat( state.getLastKnownAliveUpToDateInstance(), equalTo( new InstanceId( 3 ) ));
    }

    @Test
    public void settingLastLearnedInstanceToNegativeOneShouldAlwaysWin()
    {
        // Given
        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext mpCtx = new MultiPaxosContext( null, Iterables.empty(),
                mock( ClusterConfiguration.class ), null, NullLogProvider.getInstance(),
                null, null, null, null,
                null, config );
        LearnerContext state = mpCtx.getLearnerContext();

        // When
        state.setLastKnownLearnedInstanceInCluster( 1, new InstanceId( 2 ) );
        state.setLastKnownLearnedInstanceInCluster( -1, null );

        // Then
        assertThat( state.getLastKnownLearnedInstanceInCluster(), equalTo( -1L ) );
        assertThat( state.getLastKnownAliveUpToDateInstance(), equalTo( new org.neo4j.cluster.InstanceId( 2 ) ));
    }
}
