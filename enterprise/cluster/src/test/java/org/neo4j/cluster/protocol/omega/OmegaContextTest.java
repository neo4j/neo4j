/*
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
package org.neo4j.cluster.protocol.omega;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.omega.state.EpochNumber;
import org.neo4j.cluster.protocol.omega.state.State;
import org.neo4j.cluster.protocol.omega.state.View;

public class OmegaContextTest
{
    @Test
    public void testOrderingOfEpochNumberOnSerialNum()
    {
        List<EpochNumber> toSort = new LinkedList<EpochNumber>();
        // Creates a list in order 5,4,6,3,7,2,8,1,9
        for ( int i = 1; i < 10; i++ )
        {
            // The sign code is lame, but i couldn't figure a branch free way
            EpochNumber epoch = new EpochNumber( 5 + ((i + 1) / 2) * (i % 2 == 0 ? -1 : 1), i );
            toSort.add( epoch );
        }

        Collections.sort( toSort );

        for ( int i = 1; i < toSort.size(); i++ )
        {
            EpochNumber prev = toSort.get( i - 1 );
            EpochNumber current = toSort.get( i );
            assertTrue( prev.getSerialNum() < current.getSerialNum() );
        }
    }

    @Test
    public void testOrderingOfEpochNumberOnProcessId()
    {
        List<EpochNumber> toSort = new LinkedList<EpochNumber>();
        // Creates a list in order 5,4,6,3,7,2,8,1,9
        for ( int i = 1; i < 10; i++ )
        {
            EpochNumber epoch = new EpochNumber( 1, 5 + ((i + 1) / 2) * (i % 2 == 0 ? -1 : 1) );
            toSort.add( epoch );
        }

        Collections.sort( toSort );

        for ( int i = 1; i < toSort.size(); i++ )
        {
            EpochNumber prev = toSort.get( i - 1 );
            EpochNumber current = toSort.get( i );
            assertTrue( prev.getProcessId() < current.getProcessId() );
        }
    }

    @Test
    public void testOrderingEqualEpochs()
    {
        assertEquals( 0, new EpochNumber().compareTo( new EpochNumber() ) );
    }

    @Test
    public void testOrderingOfStateOnEpochNum()
    {
        List<State> toSort = new LinkedList<State>();
        // Creates a list in order 5,4,6,3,7,2,8,1,9
        for ( int i = 1; i < 10; i++ )
        {
            EpochNumber epoch = new EpochNumber( 5 + ((i + 1) / 2) * (i % 2 == 0 ? -1 : 1), 1 );
            State state = new State( epoch, 1 );
            toSort.add( state );
        }

        Collections.sort( toSort );

        for ( int i = 1; i < toSort.size(); i++ )
        {
            State prev = toSort.get( i - 1 );
            State current = toSort.get( i );
            assertTrue( prev.getEpochNum().getSerialNum() < current.getEpochNum().getSerialNum() );
        }
    }

    @Test
    public void testBasicRefreshRound()
    {
        OmegaContext context = new OmegaContext( Mockito.mock( ClusterContext.class ) );
        assertEquals( -1, context.getAckCount( 0 ) );
        int refreshRoundOne = context.startRefreshRound();
        assertEquals( 0, context.getAckCount( refreshRoundOne ) );
        context.ackReceived( refreshRoundOne );
        assertEquals( 1, context.getAckCount( refreshRoundOne ) );
        context.roundDone( refreshRoundOne );
        assertEquals( -1, context.getAckCount( refreshRoundOne ) );
    }

    @Test
    public void testTwoParallelRefreshRounds()
    {
        OmegaContext context = new OmegaContext( Mockito.mock( ClusterContext.class ) );
        int refreshRoundOne = context.startRefreshRound();
        context.ackReceived( refreshRoundOne );
        int refreshRoundTwo = context.startRefreshRound();
        context.ackReceived( refreshRoundOne );
        context.ackReceived( refreshRoundTwo );
        assertEquals( 2, context.getAckCount( refreshRoundOne ) );
        assertEquals( 1, context.getAckCount( refreshRoundTwo ) );
        context.roundDone( refreshRoundOne );
        assertEquals( -1, context.getAckCount( refreshRoundOne ) );
        assertEquals( 1, context.getAckCount( refreshRoundTwo ) );
    }

    @Test
    public void testFirstAndSecondCollectionRound() throws URISyntaxException
    {
        OmegaContext context = new OmegaContext( Mockito.mock( ClusterContext.class ) );
        int firstCollectionRound = context.startCollectionRound();
        assertEquals( 1, firstCollectionRound );
        assertEquals( Collections.emptyMap(), context.getPreviousViewForCollectionRound( firstCollectionRound ) );
        assertEquals( 0, context.getStatusResponsesForRound( firstCollectionRound ) );

        State state1 = new State( new EpochNumber(), 1 );
        State state2 = new State( new EpochNumber(), 1 );
        URI uri1 = new URI( "neo4j://server1" );
        URI uri2 = new URI( "neo4j://server2" );

        Map<URI, State> registry = new HashMap<URI, State>();
        registry.put( uri1, state1 );
        registry.put( uri2, state2 );

        Map<URI, State> emptyView = Collections.emptyMap();

        context.responseReceivedForRound( firstCollectionRound, uri1, emptyView );
        // Really checking the invariants on the COLLECT phase of the algo is a test case on its own, below
        assertEquals( 1, context.getStatusResponsesForRound( firstCollectionRound ) );
        context.responseReceivedForRound( firstCollectionRound, uri2, emptyView );
        assertEquals( 2, context.getStatusResponsesForRound( firstCollectionRound ) );
        context.collectionRoundDone( firstCollectionRound );

        int secondCollectionRound = context.startCollectionRound();
        assertEquals( secondCollectionRound, firstCollectionRound + 1 );
        assertEquals( context.getViews(), context.getPreviousViewForCollectionRound( secondCollectionRound ) );
    }

    @Test
    public void testCollectInvariantsHoldAfterTwoCollectResponses() throws URISyntaxException
    {
        URI uri1 = new URI( "neo4j://server1" );
        URI uri2 = new URI( "neo4j://server2" );
        URI uri3 = new URI( "neo4j://server3" );
        URI uri4 = new URI( "neo4j://server4" );

        // Also, assume this is the first round
        Map<URI, View> finalViews = new HashMap<URI, View>();
        finalViews.put( uri1, new View( new State( new EpochNumber( 1 ), 3 ), false ) );
        finalViews.put( uri2, new View( new State( new EpochNumber( 2 ), 3 ), false ) );
        finalViews.put( uri3, new View( new State( new EpochNumber( 0 ), 5 ), false ) );

        Map<URI, State> registryFrom1 = new HashMap<URI, State>();
        registryFrom1.put( uri1, new State( new EpochNumber( 1 ), 3 ) );
        registryFrom1.put( uri2, new State( new EpochNumber( 2 ), 3 ) );
        registryFrom1.put( uri3, new State( new EpochNumber( 0 ), 5 ) );

        Map<URI, State> registryFrom2 = new HashMap<URI, State>();
        registryFrom2.put( uri1, new State( new EpochNumber( 1 ), 3 ) );
        registryFrom2.put( uri2, new State( new EpochNumber( 1 ), 3 ) );
        registryFrom2.put( uri3, new State( new EpochNumber( 0 ), 4 ) );

        OmegaContext context = new OmegaContext( Mockito.mock( ClusterContext.class ) );

        int collectionRound = context.startCollectionRound();
        context.responseReceivedForRound( collectionRound,  uri1, registryFrom1 );
        checkConsolidatedViews( context.getViews(), registryFrom1 );
        context.responseReceivedForRound( collectionRound, uri2, registryFrom2 );
        checkConsolidatedViews( context.getViews(), registryFrom2 );
        // Now we have collected responses from a majority. Check that the views have been expired properly
        context.collectionRoundDone( collectionRound );
        // We know the rest of the servers provide a maximum of 3 instances - our views must be at least as large
        checkUpdatedViews( context.getPreviousViewForCollectionRound( collectionRound ), context.getViews() );

        assertEquals( finalViews, context.getViews() );

        // Time for the second round. This is the expected
        finalViews.put( uri1, new View( new State( new EpochNumber( 1 ), 3 ) ) );
        finalViews.put( uri2, new View( new State( new EpochNumber( 2 ), 3 ) ) );
        finalViews.put( uri3, new View( new State( new EpochNumber( 4 ), 10 ), false ) );
        finalViews.put( uri4, new View( new State( new EpochNumber( 1 ), 2 ), false ) );

        registryFrom1.put( uri1, new State( new EpochNumber( 1 ), 3 ) );
        registryFrom1.put( uri2, new State( new EpochNumber( 1 ), 3 ) );
        registryFrom1.put( uri3, new State( new EpochNumber( 4 ), 10 ) );

        registryFrom2.put( uri1, new State( new EpochNumber( 1 ), 3 ) );
        registryFrom2.put( uri2, new State( new EpochNumber( 1 ), 3 ) );
        registryFrom2.put( uri3, new State( new EpochNumber( 3 ), 9 ) );
        // and all of the sudden, we have seen a new instance
        registryFrom2.put( uri4, new State( new EpochNumber( 1 ), 2 ) );

        collectionRound = context.startCollectionRound();
        context.responseReceivedForRound( collectionRound, uri1, registryFrom1 );
        checkConsolidatedViews( context.getViews(), registryFrom1 );
        context.responseReceivedForRound( collectionRound, uri2, registryFrom2 );
        checkConsolidatedViews( context.getViews(), registryFrom2 );
        // Now we have collected responses from a majority. Check that the views have been expired properly
        context.collectionRoundDone( collectionRound );
        // We know the rest of the servers provide a maximum of 3 instances - our views must be at least as large
        checkUpdatedViews( context.getPreviousViewForCollectionRound( collectionRound ), context.getViews() );

        assertEquals( finalViews, context.getViews() );
    }

    private void checkConsolidatedViews( Map<URI, View> collectorViews, Map<URI, State> collectedRegistry )
    {
        for ( Map.Entry<URI, View> view : collectorViews.entrySet() )
        {
            URI uri = view.getKey();
            State viewedState = view.getValue().getState();
            assertTrue( viewedState.compareTo( collectedRegistry.get( uri ) ) >= 0 );
        }
    }

    private void checkUpdatedViews( Map<URI, View> oldViews, Map<URI, View> newViews )
    {
        for ( Map.Entry<URI, View> view : newViews.entrySet() )
        {
            URI uri = view.getKey();
            View newView = view.getValue();
            View oldView = oldViews.get( uri );
            if ( oldView == null )
            {
                assertFalse( newView.isExpired() );
                continue;
            }
            if ( newView.getState().compareTo( oldView.getState() ) <= 0 )
            {
                assertTrue( newView.isExpired() );
            }
            if ( newView.getState().getEpochNum().compareTo( oldView.getState().getEpochNum() ) > 0 )
            {
                assertFalse( newView.isExpired() );
            }
        }
    }
}
