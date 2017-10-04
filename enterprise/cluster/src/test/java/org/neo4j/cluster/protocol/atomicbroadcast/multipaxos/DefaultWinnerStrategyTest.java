/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.election.IntegerElectionCredentials;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentials;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultWinnerStrategyTest
{
    @Test
    public void shouldLogElectionProcess()
    {
        // given
        ClusterContext clusterContext = mock( ClusterContext.class );

        final Log log = mock( Log.class );
        LogProvider logProvider = new LogProvider()
        {
            @Override
            public Log getLog( Class loggingClass )
            {
                return log;
            }

            @Override
            public Log getLog( String name )
            {
                return log;
            }
        };

        when( clusterContext.getLog( DefaultWinnerStrategy.class ) ).thenReturn( logProvider.getLog( DefaultWinnerStrategy.class ) );

        // when
        Collection<Vote> votes = Collections.emptyList();

        DefaultWinnerStrategy strategy = new DefaultWinnerStrategy( clusterContext );
        strategy.pickWinner( votes );

        // then
        verify( log ).debug( "Election: received votes [], eligible votes []" );
    }

    @Test
    public void shouldNotPickAWinnerIfAllVotesAreForIneligibleCandidates()
    {
        // given
        InstanceId instanceOne = new InstanceId( 1 );
        InstanceId instanceTwo = new InstanceId( 2 );

        ClusterContext clusterContext = mock( ClusterContext.class );

        final Log log = mock( Log.class );
        LogProvider logProvider = new LogProvider()
        {
            @Override
            public Log getLog( Class loggingClass )
            {
                return log;
            }

            @Override
            public Log getLog( String name )
            {
                return log;
            }
        };

        when( clusterContext.getLog( DefaultWinnerStrategy.class ) ).thenReturn( logProvider.getLog( DefaultWinnerStrategy.class ) );

        // when
        Collection<Vote> votes = Arrays.asList(
                new Vote( instanceOne, new NotElectableElectionCredentials() ),
                new Vote( instanceTwo, new NotElectableElectionCredentials() ) );

        DefaultWinnerStrategy strategy = new DefaultWinnerStrategy( clusterContext );
        org.neo4j.cluster.InstanceId winner = strategy.pickWinner( votes );

        // then
        assertNull( winner );
    }

    @Test
    public void theWinnerShouldHaveTheBestVoteCredentials() throws Exception
    {
        // given
        InstanceId instanceOne = new InstanceId( 1 );
        InstanceId instanceTwo = new InstanceId( 2 );

        ClusterContext clusterContext = mock( ClusterContext.class );

        final Log log = mock( Log.class );
        LogProvider logProvider = new LogProvider()
        {
            @Override
            public Log getLog( Class loggingClass )
            {
                return log;
            }

            @Override
            public Log getLog( String name )
            {
                return log;
            }
        };

        when( clusterContext.getLog( DefaultWinnerStrategy.class ) ).thenReturn( logProvider.getLog( DefaultWinnerStrategy.class ) );

        // when
        Collection<Vote> votes = Arrays.asList(
                new Vote( instanceOne, new IntegerElectionCredentials( 1 ) ),
                new Vote( instanceTwo, new IntegerElectionCredentials( 2 ) ) );

        DefaultWinnerStrategy strategy = new DefaultWinnerStrategy( clusterContext );
        org.neo4j.cluster.InstanceId winner = strategy.pickWinner( votes );

        // then
        assertEquals( instanceTwo, winner );
    }
}
