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
package org.neo4j.kernel.impl.api.store;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.test.rule.RandomRule;

import static java.util.Collections.disjoint;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.single;

public class ParallelAllNodeProgressionTest
{
    @Rule
    public RandomRule random = new RandomRule();

    private final NodeStore nodeStore = mock( NodeStore.class );

    private int start;
    private int end;
    private int threads;

    @Before
    public void setup()
    {
        start = random.nextInt( 0, 20 );
        end = random.nextInt( start + 40, start + 100 );
        threads = random.nextInt( 2, 6 );
        when( nodeStore.getNumberOfReservedLowIds() ).thenReturn( start );
        when( nodeStore.getRecordsPerPage() ).thenReturn( end / random.nextInt( 2, 5 ) );
    }

    @Test
    public void shouldServeDisjointBatchesToDifferentThreads() throws Throwable
    {
        // given
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( (long) end );
        ParallelAllNodeProgression progression = new ParallelAllNodeProgression( nodeStore, null );
        ExecutorService service = Executors.newFixedThreadPool( threads );
        try
        {
            Future<Set<Long>>[] futures = runInParallel( threads, progression, service );
            Set<Long> mergedResults = mergeResultsAndAssertDisjoint( futures );
            Set<Long> expected = expected( start, end );
            assertEquals( message( expected, mergedResults ), expected, mergedResults );
        }
        finally
        {
            service.shutdown();
        }
    }

    @Test
    public void shouldConsiderHighIdChanges() throws Throwable
    {
        // given
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( (long) end, end + 1L, end + 2L );
        ParallelAllNodeProgression progression = new ParallelAllNodeProgression( nodeStore, null );
        ExecutorService service = Executors.newFixedThreadPool( threads );
        try
        {
            Future<Set<Long>>[] futures = runInParallel( threads, progression, service );
            Set<Long> mergedResults = mergeResultsAndAssertDisjoint( futures );
            Set<Long> expected = expected( start, end + 1 );
            assertEquals( message( expected, mergedResults ), expected, mergedResults );
        }
        finally
        {
            service.shutdown();
        }
    }

    @Test
    public void onlyOneShouldRetrieveTheAddedNodes() throws Throwable
    {
        TxState txState = new TxState();
        Set<Long> expected = new HashSet<>();
        for ( long i = 0; i < 10; i++ )
        {
            expected.add( end + i );
            txState.nodeDoCreate( end + i );
        }
        ParallelAllNodeProgression progression = new ParallelAllNodeProgression( nodeStore, txState );
        ExecutorService service = Executors.newFixedThreadPool( threads );
        try
        {
            @SuppressWarnings( "unchecked" )
            Future<Iterator<Long>>[] futures = new Future[threads];
            for ( int i = 0; i < threads; i++ )
            {
                futures[i] = service.submit( progression::addedNodes );
            }
            ArrayList<Iterator<Long>> results = new ArrayList<>();
            for ( int i = 0; i < threads; i++ )
            {
                results.add( futures[i].get() );
            }

            List<Iterator<Long>> nonNullResults = results.stream().filter( Objects::nonNull ).collect( toList() );
            assertEquals( 1, nonNullResults.size() );
            assertEquals( expected, Iterators.asSet( single( nonNullResults ) ) );
        }
        finally
        {
            service.shutdown();
        }
    }

    private Set<Long> mergeResultsAndAssertDisjoint( Future<Set<Long>>[] futures )
            throws InterruptedException, java.util.concurrent.ExecutionException
    {
        Set<Long> mergedResults = new HashSet<>();
        for ( int i = 0; i < threads; i++ )
        {
            Set<Long> result = futures[i].get();
            assertTrue( message( result, mergedResults ), disjoint( mergedResults, result ) );
            mergedResults.addAll( result );
        }
        return mergedResults;
    }

    private Future<Set<Long>>[] runInParallel( int threads, ParallelAllNodeProgression progression,
            ExecutorService service )
    {
        @SuppressWarnings( "unchecked" )
        Future<Set<Long>>[] futures = new Future[this.threads];
        for ( int i = 0; i < threads; i++ )
        {
            futures[i] = service.submit( () ->
            {
                Set<Long> result = new HashSet<>();
                NodeProgression.Batch batch = new NodeProgression.Batch();
                while ( progression.nextBatch( batch ) )
                {
                    while ( batch.hasNext() )
                    {
                        assertTrue( result.add( batch.next() ) );
                    }
                }
                return result;
            } );
        }
        return futures;
    }

    private Set<Long> expected( int start, int end )
    {
        Set<Long> expected = new HashSet<>();
        for ( long i = start; i <= end; i++ )
        {
            expected.add( i );
        }
        return expected;
    }

    private String message( Set<Long> expected, Set<Long> mergedResults )
    {
        return sort( expected ) + "\n" + sort( mergedResults );
    }

    private List<Object> sort( Collection<Long> expected )
    {
        return expected.stream().sorted().collect( toList() );
    }
}
