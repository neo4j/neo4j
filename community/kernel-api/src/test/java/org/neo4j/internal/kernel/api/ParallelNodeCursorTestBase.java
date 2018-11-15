/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.kernel.api;


import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public abstract class ParallelNodeCursorTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    private static List<Long> NODE_IDS;
    private static final int NUMBER_OF_NODES = 128;

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            NODE_IDS = new ArrayList<>( NUMBER_OF_NODES );
            for ( int i = 0; i < NUMBER_OF_NODES; i++ )
            {
                NODE_IDS.add( graphDb.createNodeId() );
            }

            tx.success();
        }
    }

    @Test
    public void shouldScanASubsetOfNodes()
    {
        try ( NodeCursor nodes = cursors.allocateNodeCursor() )
        {
            // when
            Scan<NodeCursor> scan = read.allNodesScan();
            assertTrue( scan.reserveBatch( nodes, 3 ) );

            assertTrue( nodes.next() );
            assertEquals( NODE_IDS.get( 0 ).longValue(), nodes.nodeReference() );
            assertTrue( nodes.next() );
            assertEquals( NODE_IDS.get( 1 ).longValue(), nodes.nodeReference() );
            assertTrue( nodes.next() );
            assertEquals( NODE_IDS.get( 2 ).longValue(), nodes.nodeReference() );
            assertFalse( nodes.next() );
        }
    }

    @Test
    public void shouldHandleSizeHintOverflow()
    {
        try ( NodeCursor nodes = cursors.allocateNodeCursor() )
        {
            // when
            Scan<NodeCursor> scan = read.allNodesScan();
            assertTrue( scan.reserveBatch( nodes, NUMBER_OF_NODES * 2 ) );

            List<Long> ids = new ArrayList<>();
            while ( nodes.next() )
            {
                ids.add( nodes.nodeReference() );
            }

            assertEquals( NODE_IDS, ids );
        }
    }

    @Test
    public void shouldHandleSizeHintZero()
    {
        try ( NodeCursor nodes = cursors.allocateNodeCursor() )
        {
            Scan<NodeCursor> scan = read.allNodesScan();
            assertTrue( scan.reserveBatch( nodes, 0 ) );
            assertFalse( nodes.next() );
            assertTrue( scan.reserveBatch( nodes, 1 ) );
            assertTrue( nodes.next() );
        }
    }

    @Test
    public void shouldScanAllNodesInBatches()
    {
        // given
        List<Long> ids = new ArrayList<>();
        try ( NodeCursor nodes = cursors.allocateNodeCursor() )
        {
            // when
            Scan<NodeCursor> scan = read.allNodesScan();
            while ( scan.reserveBatch( nodes, 3 ) )
            {
                while ( nodes.next() )
                {
                    ids.add( nodes.nodeReference() );
                }
            }
        }

        // then
        assertEquals( NODE_IDS, ids );
    }

    @Test
    public void shouldScanAllNodesFromMultipleThreads() throws InterruptedException, ExecutionException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<NodeCursor> scan = read.allNodesScan();
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        // when
        Future<List<Long>> future1 = service.submit( singleBatchWorker( scan, cursors, 32 ) );
        Future<List<Long>> future2 = service.submit( singleBatchWorker( scan, cursors, 32 ) );
        Future<List<Long>> future3 = service.submit( singleBatchWorker( scan, cursors, 32 ) );
        Future<List<Long>> future4 = service.submit( singleBatchWorker( scan, cursors, 32 ) );
        service.shutdown();
        service.awaitTermination( 1, TimeUnit.MINUTES );

        // then
        List<Long> ids1 = future1.get();
        List<Long> ids2 = future2.get();
        List<Long> ids3 = future3.get();
        List<Long> ids4 = future4.get();

        assertDistinct( ids1, ids2, ids3, ids4 );
        List<Long> concat = concat( ids1, ids2, ids3, ids4 );
        concat.sort( Long::compareTo );
        assertEquals( NODE_IDS, concat );
    }

    @Test
    public void shouldScanAllNodesFromMultipleThreadWithBigSizeHints() throws InterruptedException, ExecutionException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<NodeCursor> scan = read.allNodesScan();
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        // when
        Future<List<Long>> future1 = service.submit( singleBatchWorker( scan, cursors, 100 ) );
        Future<List<Long>> future2 = service.submit( singleBatchWorker( scan, cursors, 100 ) );
        Future<List<Long>> future3 = service.submit( singleBatchWorker( scan, cursors, 100 ) );
        Future<List<Long>> future4 = service.submit( singleBatchWorker( scan, cursors, 100 ) );
        service.shutdown();
        service.awaitTermination( 1, TimeUnit.MINUTES );

        // then
        List<Long> ids1 = future1.get();
        List<Long> ids2 = future2.get();
        List<Long> ids3 = future3.get();
        List<Long> ids4 = future4.get();

        assertDistinct( ids1, ids2, ids3, ids4 );
        List<Long> concat = concat( ids1, ids2, ids3, ids4 );
        concat.sort( Long::compareTo );
        assertEquals( NODE_IDS, concat );
    }

    @Test
    public void shouldScanAllNodesFromRandomlySizedWorkers() throws InterruptedException, ExecutionException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<NodeCursor> scan = read.allNodesScan();
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        // when
        ArrayList<Future<List<Long>>> futures = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            futures.add( service.submit( randomBatchWorker( scan, cursors ) ) );
        }

        service.shutdown();
        service.awaitTermination( 1, TimeUnit.MINUTES );

        // then
        List<List<Long>> lists = futures.stream().map( this::unsafeGet ).collect( Collectors.toList() );

        assertDistinct( lists );
        List<Long> concat = concat( lists );
        concat.sort( Long::compareTo );
        assertEquals( NODE_IDS, concat );
    }

    private <T> T unsafeGet( Future<T> future )
    {
        try
        {
            return future.get();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    @SafeVarargs
    private final <T> void assertDistinct( List<T>... lists )
    {
        assertDistinct( Arrays.asList( lists ) );
    }

    private <T> void assertDistinct( List<List<T>> lists )
    {
        Set<T> seen = new HashSet<T>();
        for ( List<T> list : lists )
        {
            for ( T item : list )
            {
                assertTrue( String.format( "%s was seen multiple times", item ), seen.add( item ) );
            }
        }
    }

    @SafeVarargs
    private final <T> List<T> concat( List<T>... lists )
    {
        return concat( Arrays.asList( lists ) );
    }

    private <T> List<T> concat( List<List<T>> lists )
    {
        return lists.stream().flatMap( Collection::stream ).collect( Collectors.toList());
    }

    private Callable<List<Long>> singleBatchWorker( Scan<NodeCursor> scan, CursorFactory cursorsFactory, int sizeHint )
    {
        return () -> {
            try ( NodeCursor nodes = cursorsFactory.allocateNodeCursor() )
            {
                List<Long> ids = new ArrayList<>( sizeHint );
                scan.reserveBatch( nodes, sizeHint );
                while ( nodes.next() )
                {
                    ids.add( nodes.nodeReference() );
                }

                return ids;
            }
        };
    }

    private Callable<List<Long>> randomBatchWorker( Scan<NodeCursor> scan, CursorFactory cursorsFactory )
    {
        return () -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();

            try ( NodeCursor nodes = cursorsFactory.allocateNodeCursor() )
            {
                int sizeHint = random.nextInt( 4 );
                List<Long> ids = new ArrayList<>();
                while ( scan.reserveBatch( nodes, sizeHint ) )
                {
                    while ( nodes.next() )
                    {
                        ids.add( nodes.nodeReference() );
                    }
                }

                return ids;
            }
        };
    }
}
