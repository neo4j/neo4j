/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.heuristics;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.heuristics.HeuristicsData;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.PrimitiveIntIterator;
import org.neo4j.kernel.impl.util.statistics.LabelledDistribution;
import org.neo4j.kernel.impl.util.statistics.RollingAverage;
import org.neo4j.test.TargetDirectory;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.neo4j.helpers.collection.MapUtil.map;

public class RuntimeHeuristicsServiceTest
{
    @Rule
    public TargetDirectory.TestDirectory dir = TargetDirectory.cleanTestDirForTest( getClass() );

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Test
    public void shouldGatherLabelDistribution() throws Throwable
    {
        // Given
        double equalityTolerance = 0.2d;
        HeuristicsCollector collector = new HeuristicsCollector(
                new RollingAverage.Parameters( RollingAverage.Parameters.DEFAULT_WINDOW_SIZE, equalityTolerance )
        );
        RuntimeHeuristicsService service = new RuntimeHeuristicsService( collector, generateStore(), null );
        HeuristicsData heuristics = service.heuristics();

        // When
        service.run();
        service.run();

        // Then
        assertThat( heuristics.labelDistribution(), equalTo(
                new LabelledDistribution<Integer>( equalityTolerance )
                        .record(asList(0), 20)
                        .record(asList(1), 80)
                        .recalculate()
        ) );
    }

    @Test
    public void shouldGatherRelationshipTypeAndDirectionDistribution() throws Exception
    {
        // Given
        double equalityTolerance = 0.2d;
        HeuristicsCollector collector = new HeuristicsCollector(
            new RollingAverage.Parameters( RollingAverage.Parameters.DEFAULT_WINDOW_SIZE, equalityTolerance )
        );
        RuntimeHeuristicsService service = new RuntimeHeuristicsService( collector, generateStore(), null );
        HeuristicsData heuristics = service.heuristics();

        // When
        service.run();
        service.run();

        // Then
        assertThat( heuristics.relationshipTypeDistribution(),
                equalTo(new LabelledDistribution<Integer>( equalityTolerance )
                        .record(asList(0), 40)
                        .record(asList(1), 60)
                        .recalculate()) );
    }

    @Test
    public void shouldGatherRelationshipDegreeByLabelDistribution() throws Exception
    {
        // Given
        RuntimeHeuristicsService service = new RuntimeHeuristicsService( generateStore(), null );
        HeuristicsData heuristics = service.heuristics();

        // When
        service.run();
        service.run();

        // Then
        assertThat( heuristics.degree( 1, 0, Direction.INCOMING ), closeTo( 44.0, 10.0 ));
        assertThat( heuristics.degree( 1, 0, Direction.OUTGOING ), closeTo( 4.4,   1.0 ));
    }

    @Test
    public void shouldGatherLiveNodes() throws Throwable
    {
        // Given
        RuntimeHeuristicsService service = new RuntimeHeuristicsService( generateStore( 0.6 ), null );
        HeuristicsData heuristics = service.heuristics();

        // When
        service.run();
        service.run();

        // Then
        assertThat( heuristics.liveNodesRatio(), closeTo( 0.6, 0.1 ) );
    }

    @Test
    public void shouldGatherMaxNodes() throws Throwable
    {
        // Given
        RuntimeHeuristicsService service = new RuntimeHeuristicsService( generateStore(), null );
        HeuristicsData heuristics = service.heuristics();

        // When
        service.run();
        service.run();

        // Then
        assertThat( heuristics.maxAddressableNodes(), equalTo( 1000L ) );
    }

    @Test
    public void shouldSerializeAndDeserialize() throws Exception
    {
        // Given
        StoreReadLayer store = generateStore();
        RuntimeHeuristicsService service = new RuntimeHeuristicsService( store, null );
        service.run();

        // When
        service.save(fs, new File(dir.directory(), "somefile"));

        // Then
        assertThat( RuntimeHeuristicsService.load( fs, new File( dir.directory(), "somefile" ), store, null ), equalTo( service ));
    }

    @Test
    public void shouldSerializeTwiceAndDeserialize() throws Exception
    {
        // Given
        StoreReadLayer store = generateStore();
        RuntimeHeuristicsService service = new RuntimeHeuristicsService( store, null );
        service.run();

        // When
        service.save(fs, new File(dir.directory(), "somefile"));
        service.save(fs, new File(dir.directory(), "somefile"));

        // Then
        assertThat( RuntimeHeuristicsService.load( fs, new File( dir.directory(), "somefile" ), store, null ), equalTo( service ));
    }

    private StoreReadLayer generateStore() throws EntityNotFoundException
    {
        return generateStore( 1.0 );
    }

    private StoreReadLayer generateStore( double liveNodes ) throws EntityNotFoundException
    {
        StoreReadLayer store = mock( StoreReadLayer.class );
        when(store.highestNodeIdInUse()).thenReturn( 1000l );
        mockNodeLiveness(liveNodes, store);

        when(store.nodeGetLabels( anyLong() )).then( answerWithDistribution(
                20, ids(0),
                80, ids(1)) );
        when(store.nodeGetRelationshipTypes( anyLong() )).then( answerWithDistribution(
                40, ids(0),
                60, ids(1)));
        when(store.nodeGetDegree( anyLong(), eq( Direction.INCOMING), anyInt() ) ).then( answerWithDistribution(
                10, degree(10),
                30, degree(40),
                90, degree(50) ) );
        when(store.nodeGetDegree( anyLong(), eq(Direction.OUTGOING), anyInt() )).then(  answerWithDistribution(
                10, degree(1),
                20, degree(4),
                85, degree(5) ) );

        return store;
    }

    private void mockNodeLiveness(double liveNodes, StoreReadLayer store) {
        int alive = (int) (liveNodes * 100);
        int dead = 100-alive;

        // smallest percentile first
        if ( alive < dead )
        {
            when(store.nodeExists(anyLong())).then(answerWithDistribution(alive, value(true), dead, value(false)));
        }
        else
        {
            when(store.nodeExists(anyLong())).then(answerWithDistribution(dead, value(false), alive, value(true)));
        }
    }

    private Answer<?> answerWithDistribution( Object ... alternatingPercentileAndProvider )
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
                    if(r <= percentiles[i])
                    {
                        return ((Provider<?>)probabilities.get( percentiles[i] )).instance();
                    }
                }
                return ((Provider<?>)probabilities.get(percentiles[percentiles.length - 1])).instance();
            }
        };
    }

    private Provider<PrimitiveIntIterator> ids( final int ... ids )
    {
        return new Provider<PrimitiveIntIterator>()
        {
            @Override
            public PrimitiveIntIterator instance()
            {
                return IteratorUtil.asPrimitiveIterator( ids );
            }
        };
    }

    private Provider<Integer> degree( final int degree )
    {
        return value( degree );
    }


    private <V> Provider<V> value( final V value )
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
