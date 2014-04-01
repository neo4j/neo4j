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

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.PrimitiveIntIterator;
import org.neo4j.kernel.impl.util.statistics.LabelledDistribution;
import org.neo4j.test.TargetDirectory;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.neo4j.helpers.collection.MapUtil.map;

public class RuntimeHeuristicsTest
{
    @Rule
    public TargetDirectory.TestDirectory dir = TargetDirectory.cleanTestDirForTest( getClass() );

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Test
    public void shouldGatherLabelDistribution() throws Throwable
    {
        // Given
        RuntimeHeuristicsService heuristics = new RuntimeHeuristicsService( generateStore(), null );

        // When
        heuristics.run();
        heuristics.run();

        // Then
        assertThat( heuristics.labelDistribution(), closeToDistribution(
                new LabelledDistribution<Integer>()
                        .record( asList(0), 20 )
                        .record( asList(1), 80 )
                        .recalculate() ) );
    }

    @Test
    public void shouldGatherRelationshipTypeAndDirectionDistribution() throws Exception
    {
        // Given
        RuntimeHeuristicsService heuristics = new RuntimeHeuristicsService( generateStore(), null );

        // When
        heuristics.run();
        heuristics.run();

        // Then
        assertThat( heuristics.relationshipTypeDistribution(),
                closeToDistribution( new LabelledDistribution<Integer>()
                        .record( asList( 0 ), 40 )
                        .record( asList( 1 ), 60 )
                        .recalculate() ) );
    }

    @Test
    public void shouldGatherRelationshipDegreeByLabelDistribution() throws Exception
    {
        // Given
        RuntimeHeuristicsService heuristics = new RuntimeHeuristicsService( generateStore(), null );

        // When
        heuristics.run();
        heuristics.run();

        // Then
        assertThat( heuristics.degree( 1, 0, Direction.INCOMING ), closeTo( 44.0, 10.0 ));
        assertThat( heuristics.degree( 1, 0, Direction.OUTGOING ), closeTo( 4.4,   1.0 ));
    }

    @Test
    public void shouldSerializeAndDeserialize() throws Exception
    {
        // Given
        StoreReadLayer store = generateStore();
        RuntimeHeuristicsService heuristics = new RuntimeHeuristicsService( store, null );
        heuristics.run();

        // When
        heuristics.save( fs, new File( dir.directory(), "somefile" ) );

        // Then
        assertThat( RuntimeHeuristicsService.load( fs, new File( dir.directory(), "somefile" ), store, null ), equalTo( heuristics ));
    }

    private StoreReadLayer generateStore() throws EntityNotFoundException
    {
        StoreReadLayer store = mock( StoreReadLayer.class );
        when(store.highestNodeIdInUse()).thenReturn( 1000l );
        when(store.nodeExists( anyLong() )).thenReturn( true );

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
        return new Provider<Integer>()
        {
            @Override
            public Integer instance()
            {
                return degree;
            }
        };
    }

    private Matcher<? super LabelledDistribution<Integer>> closeToDistribution( final LabelledDistribution<Integer> expected )
    {
        return new TypeSafeMatcher<LabelledDistribution<Integer>>()
        {
            @Override
            protected boolean matchesSafely( LabelledDistribution<Integer> item )
            {
                return item.equals( expected, 0.2f );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( expected.toString() );
            }
        };
    }

}
