/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.statistics;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Provider;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.MapUtil.map;

public class HeuristicsTestSupport
{

    public static StoreReadLayer generateStore() throws EntityNotFoundException
    {
        return generateStore( 1.0 );
    }

    public static StoreReadLayer generateStore( double liveNodes ) throws EntityNotFoundException
    {
        StoreReadLayer store = mock( StoreReadLayer.class );
        when( store.highestNodeIdInUse() ).thenReturn( 1000l );
        mockNodeLiveness( liveNodes, store );

        when( store.nodeGetLabels( anyLong() ) ).then( answerWithDistribution(
                20, ids( 0 ),
                80, ids( 1 ) ) );
        when( store.nodeGetRelationshipTypes( anyLong() ) ).then( answerWithDistribution(
                40, ids( 0 ),
                60, ids( 1 ) ) );
        when( store.nodeGetDegree( anyLong(), eq( Direction.INCOMING ), anyInt() ) ).then( answerWithDistribution(
                10, degree( 10 ),
                30, degree( 40 ),
                90, degree( 50 ) ) );
        when( store.nodeGetDegree( anyLong(), eq( Direction.OUTGOING ), anyInt() ) ).then( answerWithDistribution(
                10, degree( 1 ),
                20, degree( 4 ),
                85, degree( 5 ) ) );

        return store;
    }

    private static void mockNodeLiveness( double liveNodes, StoreReadLayer store )
    {
        int alive = (int) (liveNodes * 100);
        int dead = 100 - alive;

        // smallest percentile first
        if ( alive < dead )
        {
            when( store.nodeExists( anyLong() ) ).then( answerWithDistribution( alive, value( true ), dead,
                    value( false ) ) );
        }
        else
        {
            when( store.nodeExists( anyLong() ) ).then( answerWithDistribution( dead, value( false ), alive,
                    value( true ) ) );
        }
    }

    private static Answer<?> answerWithDistribution( Object... alternatingPercentileAndProvider )
    {
        final Random rand = new Random();
        final Map<String, Object> probabilities = map( alternatingPercentileAndProvider );

        final int[] percentiles = new int[probabilities.size()];
        Object[] raw = probabilities.keySet().toArray();
        for ( int i = 0; i < raw.length; i++ )
        {
            percentiles[i] = (int) raw[i];
        }
        Arrays.sort( percentiles );

        return new Answer<Object>()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                float r = rand.nextInt( 100 );
                for ( int i = 0; i < percentiles.length; i++ )
                {
                    if ( r <= percentiles[i] )
                    {
                        return ((Provider<?>) probabilities.get( percentiles[i] )).instance();
                    }
                }
                return ((Provider<?>) probabilities.get( percentiles[percentiles.length - 1] )).instance();
            }
        };
    }

    private static Provider<PrimitiveIntIterator> ids( final int... ids )
    {
        return new Provider<PrimitiveIntIterator>()
        {
            @Override
            public PrimitiveIntIterator instance()
            {
                return PrimitiveIntCollections.iterator( ids );
            }
        };
    }

    private static Provider<Integer> degree( final int degree )
    {
        return value( degree );
    }


    private static <V> Provider<V> value( final V value )
    {
        return new Provider<V>()
        {
            @Override
            public V instance()
            {
                return value;
            }
        };
    }
}
