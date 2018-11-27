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


import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.kernel.api.TestUtils.assertDistinct;
import static org.neo4j.internal.kernel.api.TestUtils.concat;
import static org.neo4j.internal.kernel.api.TestUtils.randomBatchWorker;
import static org.neo4j.internal.kernel.api.TestUtils.singleBatchWorker;

public abstract class ParallelNodeCursorTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    private static List<Long> NODE_IDS;
    private static final int NUMBER_OF_NODES = 128;
    @Rule
    public ExpectedException exception = ExpectedException.none();

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
    public void shouldFailForSizeHintZero()
    {
        try ( NodeCursor nodes = cursors.allocateNodeCursor() )
        {
            // given
            Scan<NodeCursor> scan = read.allNodesScan();

            // expect
            exception.expect( IllegalArgumentException.class );

            // when
            scan.reserveBatch( nodes, 0 );
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
        try
        {
            // when
            Future<List<Long>> future1 = service.submit( singleBatchWorker( scan, cursors, 32 ) );
            Future<List<Long>> future2 = service.submit( singleBatchWorker( scan, cursors, 32 ) );
            Future<List<Long>> future3 = service.submit( singleBatchWorker( scan, cursors, 32 ) );
            Future<List<Long>> future4 = service.submit( singleBatchWorker( scan, cursors, 32 ) );

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
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    @Test
    public void shouldScanAllNodesFromMultipleThreadWithBigSizeHints() throws InterruptedException, ExecutionException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<NodeCursor> scan = read.allNodesScan();
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        try
        {
            // when
            Future<List<Long>> future1 = service.submit( singleBatchWorker( scan, cursors, 100 ) );
            Future<List<Long>> future2 = service.submit( singleBatchWorker( scan, cursors, 100 ) );
            Future<List<Long>> future3 = service.submit( singleBatchWorker( scan, cursors, 100 ) );
            Future<List<Long>> future4 = service.submit( singleBatchWorker( scan, cursors, 100 ) );

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
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    @Test
    public void shouldScanAllNodesFromRandomlySizedWorkers() throws InterruptedException, ExecutionException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<NodeCursor> scan = read.allNodesScan();
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        try
        {
            // when
            ArrayList<Future<List<Long>>> futures = new ArrayList<>();
            for ( int i = 0; i < 10; i++ )
            {
                futures.add( service.submit( randomBatchWorker( scan, cursors ) ) );
            }

            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );

            // then
            List<List<Long>> lists = futures.stream().map( TestUtils::unsafeGet ).collect( Collectors.toList() );

            assertDistinct( lists );
            List<Long> concat = concat( lists );
            concat.sort( Long::compareTo );
            assertEquals( NODE_IDS, concat );
        }
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }
}
