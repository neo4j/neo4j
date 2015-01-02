/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Test;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.election.IntegerElectionCredentials;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentials;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BiasedWinnerStrategyTest
{
    @Test
    public void shouldLogElectionProcessWhenNodeIsDemoted()
    {
        // given
        InstanceId instanceOne = new InstanceId( 1 );

        ClusterContext clusterContext = mock( ClusterContext.class );

        StringLogger logger = mock( StringLogger.class );
        when( clusterContext.getLogger( BiasedWinnerStrategy.class ) ).thenReturn( logger );


        // when
        Collection<Vote> votes = Collections.emptyList();

        BiasedWinnerStrategy strategy = BiasedWinnerStrategy.demotion( clusterContext, instanceOne );
        strategy.pickWinner( votes );

        // then
        verify( logger ).debug( "Election: received votes [], eligible votes [] (Instance #1 has been demoted)" );
    }

    @Test
    public void shouldLogElectionProcessWhenNodeIsPromoted()
    {
        // given
        InstanceId instanceOne = new InstanceId( 1 );

        ClusterContext clusterContext = mock( ClusterContext.class );

        StringLogger logger = mock( StringLogger.class );
        when( clusterContext.getLogger( BiasedWinnerStrategy.class ) ).thenReturn( logger );


        // when
        Collection<Vote> votes = Collections.emptyList();

        BiasedWinnerStrategy strategy = BiasedWinnerStrategy.promotion( clusterContext, instanceOne );
        strategy.pickWinner( votes );

        // then
        verify( logger ).debug( "Election: received votes [], eligible votes [] (Instance #1 has been promoted)" );
    }

    @Test
    public void shouldPickTheBiasedInstanceAsTheWinner()
    {
        // given
        InstanceId instanceOne = new InstanceId( 1 );
        InstanceId instanceTwo = new InstanceId( 2 );

        ClusterContext clusterContext = mock( ClusterContext.class );

        StringLogger logger = mock( StringLogger.class );
        when( clusterContext.getLogger( BiasedWinnerStrategy.class ) ).thenReturn( logger );

        // when
        Collection<Vote> votes = Arrays.asList(
                new Vote( instanceOne, new IntegerElectionCredentials( 100 ) ),
                new Vote( instanceTwo, new IntegerElectionCredentials( 200 ) ) );

        BiasedWinnerStrategy strategy = BiasedWinnerStrategy.promotion( clusterContext, instanceOne );
        InstanceId winner = strategy.pickWinner( votes );

        // then
        assertEquals( instanceOne, winner );
    }

    @Test
    public void shouldNotPickAWinnerIfTheBiasedInstanceHasBeenDemoted()
    {
        // given
        InstanceId instanceOne = new InstanceId( 1 );

        ClusterContext clusterContext = mock( ClusterContext.class );

        StringLogger logger = mock( StringLogger.class );
        when( clusterContext.getLogger( BiasedWinnerStrategy.class ) ).thenReturn( logger );

        // when
        Collection<Vote> votes = Arrays.asList(
                new Vote( instanceOne, new IntegerElectionCredentials( 100 ) ) );

        BiasedWinnerStrategy strategy = BiasedWinnerStrategy.demotion( clusterContext, instanceOne );
        InstanceId winner = strategy.pickWinner( votes );

        // then
        assertNull( winner );
    }

    @Test
    public void shouldNotPickAWinnerIfAllVotesAreForIneligibleCandidates()
    {
        // given
        InstanceId instanceOne = new InstanceId( 1 );
        InstanceId instanceTwo = new InstanceId( 2 );

        ClusterContext clusterContext = mock( ClusterContext.class );

        StringLogger logger = mock( StringLogger.class );
        when( clusterContext.getLogger( BiasedWinnerStrategy.class ) ).thenReturn( logger );

        // when
        Collection<Vote> votes = Arrays.asList(
                new Vote( instanceOne, new NotElectableElectionCredentials() ),
                new Vote( instanceTwo, new NotElectableElectionCredentials() ) );

        BiasedWinnerStrategy strategy = BiasedWinnerStrategy.promotion( clusterContext, instanceOne );
        org.neo4j.cluster.InstanceId winner = strategy.pickWinner( votes );

        // then
        assertNull( winner );
    }

}
