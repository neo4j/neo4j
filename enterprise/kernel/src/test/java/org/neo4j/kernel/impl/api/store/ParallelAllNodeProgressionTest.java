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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordPageLocationCalculator;
import org.neo4j.test.rule.RandomRule;

import static java.util.Collections.disjoint;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        int recordSize = random.nextInt( 2, 25 );
        int pageSize = random.nextInt( recordSize * 10, recordSize * 33);
        start = random.nextInt( 0, 20 );
        end = random.nextInt( start + 40, start + 1000 );
        threads = random.nextInt( 2, 6 );
        when( nodeStore.getNumberOfReservedLowIds() ).thenReturn( start );
        when( nodeStore.pageIdForRecord( anyLong() ) ).thenAnswer( invocation ->
        {
            long recordId = (long) invocation.getArguments()[0];
            return RecordPageLocationCalculator.pageIdForRecord( recordId, pageSize, recordSize );
        } );
        when( nodeStore.firstRecordOnPage( anyLong() )).thenAnswer( invocation ->
        {
            long pageId = (long) invocation.getArguments()[0];
            return RecordPageLocationCalculator.firstRecordOnPage( pageId, pageSize, recordSize, start );
        });
    }

    @Test
    public void shouldServeDisjointBatchesToDifferentThreads() throws Throwable
    {
        // given
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( (long) end );
        ParallelAllNodeProgression progression = new ParallelAllNodeProgression( nodeStore );
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
        ParallelAllNodeProgression progression = new ParallelAllNodeProgression( nodeStore );
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
        ParallelAllNodeProgression progression = new ParallelAllNodeProgression( null );
        ExecutorService service = Executors.newFixedThreadPool( threads );
        try
        {
            @SuppressWarnings( "unchecked" )
            Future<Boolean>[] futures = new Future[threads];
            for ( int i = 0; i < threads; i++ )
            {
                futures[i] = service.submit( progression::appendAdded );
            }
            List<Boolean> results = new ArrayList<>();
            for ( int i = 0; i < threads; i++ )
            {
                results.add( futures[i].get() );
            }

            List<Boolean> trueValues = results.stream().filter( x -> x ).collect( toList() );
            assertEquals( 1, trueValues.size() );
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
                Batch batch = new Batch();
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
