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


import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.TestUtils.assertDistinct;
import static org.neo4j.internal.kernel.api.TestUtils.concat;
import static org.neo4j.internal.kernel.api.TestUtils.randomBatchWorker;
import static org.neo4j.internal.kernel.api.TestUtils.singleBatchWorker;

public abstract class ParallelNodeCursorTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    private static LongList NODE_IDS;
    private static final int NUMBER_OF_NODES = 128;
    private static final ToLongFunction<NodeCursor> NODE_GET = NodeCursor::nodeReference;

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            MutableLongList list = new LongArrayList( NUMBER_OF_NODES );
            for ( int i = 0; i < NUMBER_OF_NODES; i++ )
            {
                list.add( graphDb.createNodeId() );
            }
            NODE_IDS = list;
            tx.success();
        }
    }

    @Test
    void shouldScanASubsetOfNodes()
    {
        try ( NodeCursor nodes = cursors.allocateNodeCursor() )
        {
            // when
            Scan<NodeCursor> scan = read.allNodesScan();
            assertTrue( scan.reserveBatch( nodes, 3 ) );

            assertTrue( nodes.next() );
            assertEquals( NODE_IDS.get( 0 ), nodes.nodeReference() );
            assertTrue( nodes.next() );
            assertEquals( NODE_IDS.get( 1 ), nodes.nodeReference() );
            assertTrue( nodes.next() );
            assertEquals( NODE_IDS.get( 2 ), nodes.nodeReference() );
            assertFalse( nodes.next() );
        }
    }

    @Test
    void shouldHandleSizeHintOverflow()
    {
        try ( NodeCursor nodes = cursors.allocateNodeCursor() )
        {
            // when
            Scan<NodeCursor> scan = read.allNodesScan();
            assertTrue( scan.reserveBatch( nodes, NUMBER_OF_NODES * 2 ) );

            LongArrayList ids = new LongArrayList();
            while ( nodes.next() )
            {
                ids.add( nodes.nodeReference() );
            }

            assertEquals( NODE_IDS, ids );
        }
    }

    @Test
    void shouldFailForSizeHintZero()
    {
        try ( NodeCursor nodes = cursors.allocateNodeCursor() )
        {
            // given
            Scan<NodeCursor> scan = read.allNodesScan();

            // when
            assertThrows( IllegalArgumentException.class, () -> scan.reserveBatch( nodes, 0 ) );
        }
    }

    @Test
    void shouldScanAllNodesInBatches()
    {
        // given
        LongArrayList ids = new LongArrayList();
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
    void shouldScanAllNodesFromMultipleThreads() throws InterruptedException, ExecutionException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<NodeCursor> scan = read.allNodesScan();
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        try
        {
            // when
            Future<LongList> future1 = service.submit(
                    singleBatchWorker( scan, cursors::allocateNodeCursor, NodeCursor::nodeReference, 32 ) );
            Future<LongList> future2 = service.submit(
                    singleBatchWorker( scan, cursors::allocateNodeCursor, NodeCursor::nodeReference, 32 ) );
            Future<LongList> future3 = service.submit(
                    singleBatchWorker( scan, cursors::allocateNodeCursor, NodeCursor::nodeReference, 32 ) );
            Future<LongList> future4 = service.submit(
                    singleBatchWorker( scan, cursors::allocateNodeCursor, NodeCursor::nodeReference, 32 ) );

            // then
            LongList ids1 = future1.get();
            LongList ids2 = future2.get();
            LongList ids3 = future3.get();
            LongList ids4 = future4.get();

            assertDistinct( ids1, ids2, ids3, ids4 );
            LongList concat = concat( ids1, ids2, ids3, ids4 ).toSortedList();
            assertEquals( NODE_IDS, concat );
        }
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    @Test
    void shouldScanAllNodesFromMultipleThreadWithBigSizeHints() throws InterruptedException, ExecutionException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<NodeCursor> scan = read.allNodesScan();
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        try
        {
            // when
            Supplier<NodeCursor> allocateNodeCursor = cursors::allocateNodeCursor;
            Future<LongList> future1 = service.submit( singleBatchWorker( scan, allocateNodeCursor, NODE_GET, 100 ) );
            Future<LongList> future2 = service.submit( singleBatchWorker( scan, allocateNodeCursor, NODE_GET, 100 ) );
            Future<LongList> future3 = service.submit( singleBatchWorker( scan, allocateNodeCursor, NODE_GET, 100 ) );
            Future<LongList> future4 = service.submit( singleBatchWorker( scan, allocateNodeCursor, NODE_GET, 100 ) );

            // then
            LongList ids1 = future1.get();
            LongList ids2 = future2.get();
            LongList ids3 = future3.get();
            LongList ids4 = future4.get();

            assertDistinct( ids1, ids2, ids3, ids4 );
            LongList concat = concat( ids1, ids2, ids3, ids4 ).toSortedList();
            assertEquals( NODE_IDS, concat );
        }
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    @Test
    void shouldScanAllNodesFromRandomlySizedWorkers() throws InterruptedException, ExecutionException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<NodeCursor> scan = read.allNodesScan();
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        try
        {
            // when
            ArrayList<Future<LongList>> futures = new ArrayList<>();
            for ( int i = 0; i < 10; i++ )
            {
                futures.add( service.submit( randomBatchWorker( scan, cursors::allocateNodeCursor, NODE_GET ) ) );
            }

            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );

            // then
            List<LongList> lists = futures.stream().map( TestUtils::unsafeGet ).collect( Collectors.toList() );

            assertDistinct( lists );
            LongList concat = concat( lists ).toSortedList();
            assertEquals( NODE_IDS, concat );
        }
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }
}
