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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.TestUtils.assertDistinct;
import static org.neo4j.internal.kernel.api.TestUtils.concat;
import static org.neo4j.internal.kernel.api.TestUtils.count;
import static org.neo4j.internal.kernel.api.TestUtils.randomBatchWorker;
import static org.neo4j.internal.kernel.api.TestUtils.singleBatchWorker;

public abstract class ParallelNodeLabelScanTransactionStateTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{

    private static final Function<NodeLabelIndexCursor,Long> NODE_GET = NodeLabelIndexCursor::nodeReference;

    @Test
    public void shouldHandleEmptyDatabase()
            throws TransactionFailureException, IllegalTokenNameException, TooManyLabelsException
    {
        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "L" );
            try ( NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor() )
            {
                Scan<NodeLabelIndexCursor> scan = tx.dataRead().nodeLabelScan( label );
                while ( scan.reserveBatch( cursor, 23 ) )
                {
                    assertFalse( cursor.next() );
                }
            }
        }
    }

    @Test
    public void scanShouldNotSeeDeletedNode() throws Exception
    {
        int size = 1000;
        Set<Long> created = new HashSet<>( size );
        Set<Long> deleted = new HashSet<>( size );
        int label = label( "L" );
        try ( Transaction tx = beginTransaction() )
        {
            Write write = tx.dataWrite();
            for ( int i = 0; i < size; i++ )
            {
                long createId = write.nodeCreate();
                long deleteId = write.nodeCreate();
                write.nodeAddLabel( createId, label );
                write.nodeAddLabel( deleteId, label );
                created.add( createId );
                deleted.add( deleteId );
            }
            tx.success();
        }

        try ( Transaction tx = beginTransaction() )
        {
            for ( long delete : deleted )
            {
                tx.dataWrite().nodeDelete( delete );
            }

            try ( NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor() )
            {
                Scan<NodeLabelIndexCursor> scan = tx.dataRead().nodeLabelScan( label );
                Set<Long> seen = new HashSet<>();
                while ( scan.reserveBatch( cursor, 128 ) )
                {
                    while ( cursor.next() )
                    {
                        long nodeId = cursor.nodeReference();
                        assertTrue( seen.add( nodeId ) );
                        assertTrue( created.remove( nodeId ) );
                    }
                }

                assertTrue( created.isEmpty() );
            }
        }
    }

    @Test
    public void scanShouldSeeAddedNodes() throws Exception
    {
        int size = 64;
        int label = label( "L" );
        Set<Long> existing = new HashSet<>( createNodesWithLabel( label, size ) );
        try ( Transaction tx = beginTransaction() )
        {
            Set<Long> added = new HashSet<>( createNodesWithLabel( tx.dataWrite(), label, size ) );

            try ( NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor() )
            {
                Scan<NodeLabelIndexCursor> scan = tx.dataRead().nodeLabelScan( label );
                Set<Long> seen = new HashSet<>();
                while ( scan.reserveBatch( cursor, 64 ) )
                {
                    while ( cursor.next() )
                    {
                        long nodeId = cursor.nodeReference();
                        assertTrue( seen.add( nodeId ), format( "%d was seen multiple times", nodeId ) );
                        assertTrue( existing.remove( nodeId ) || added.remove( nodeId ) );
                    }
                }

                //make sure we have seen all nodes
                assertTrue( existing.isEmpty() );
                assertTrue( added.isEmpty() );
            }
        }
    }

    @Test
    public void shouldReserveBatchFromTxState() throws KernelException
    {
        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "L" );
            createNodesWithLabel( tx.dataWrite(), label, 11 );

            try ( NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor() )
            {
                Scan<NodeLabelIndexCursor> scan = tx.dataRead().nodeLabelScan( label );
                assertTrue( scan.reserveBatch( cursor, 5 ) );
                assertEquals( 5, count( cursor ) );
                assertTrue( scan.reserveBatch( cursor, 4 ) );
                assertEquals( 4, count( cursor ) );
                assertTrue( scan.reserveBatch( cursor, 6 ) );
                assertEquals( 2, count( cursor ) );
                //now we should have fetched all nodes
                while ( scan.reserveBatch( cursor, 3 ) )
                {
                    assertFalse( cursor.next() );
                }
            }
        }
    }

    @Test
    public void shouldScanAllNodesFromMultipleThreads()
            throws InterruptedException, ExecutionException, KernelException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        int size = 1024;

        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "L" );
            List<Long> ids = createNodesWithLabel( tx.dataWrite(), label, size );

            Read read = tx.dataRead();
            Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( label );

            // when
            Supplier<NodeLabelIndexCursor> allocateCursor = cursors::allocateNodeLabelIndexCursor;
            Future<List<Long>> future1 =
                    service.submit( singleBatchWorker( scan, allocateCursor, NODE_GET, size / 4 ) );
            Future<List<Long>> future2 =
                    service.submit( singleBatchWorker( scan, allocateCursor, NODE_GET, size / 4 ) );
            Future<List<Long>> future3 =
                    service.submit( singleBatchWorker( scan, allocateCursor, NODE_GET, size / 4 ) );
            Future<List<Long>> future4 =
                    service.submit( singleBatchWorker( scan, allocateCursor, NODE_GET, size / 4 ) );

            // then
            List<Long> ids1 = future1.get();
            List<Long> ids2 = future2.get();
            List<Long> ids3 = future3.get();
            List<Long> ids4 = future4.get();

            assertDistinct( ids1, ids2, ids3, ids4 );
            List<Long> concat = concat( ids1, ids2, ids3, ids4 );
            ids.sort( Long::compareTo );
            concat.sort( Long::compareTo );
            assertEquals( ids, concat );
            tx.failure();

        }
        service.shutdown();
        service.awaitTermination( 1, TimeUnit.MINUTES );
    }

    @Test
    public void shouldScanAllNodesFromRandomlySizedWorkers()
            throws InterruptedException, KernelException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        int size = 2000;

        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "L" );
            List<Long> ids = createNodesWithLabel( tx.dataWrite(), label, size );

            Read read = tx.dataRead();
            Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( label );
            CursorFactory cursors = testSupport.kernelToTest().cursors();

            // when
            ArrayList<Future<List<Long>>> futures = new ArrayList<>();
            for ( int i = 0; i < 10; i++ )
            {
                futures.add(
                        service.submit( randomBatchWorker( scan, cursors::allocateNodeLabelIndexCursor, NODE_GET ) ) );
            }

            // then
            List<List<Long>> lists = futures.stream().map( TestUtils::unsafeGet ).collect( Collectors.toList() );

            assertDistinct( lists );
            List<Long> concat = concat( lists );
            concat.sort( Long::compareTo );
            ids.sort( Long::compareTo );

            assertEquals( ids, concat );
            tx.failure();
        }

        service.shutdown();
        service.awaitTermination( 1, TimeUnit.MINUTES );
    }

    @Test
    public void parallelTxStateScanStressTest()
            throws KernelException, InterruptedException
    {
        int label = label( "L" );
        Set<Long> existingNodes = new HashSet<>( createNodesWithLabel( label, 1000 ) );

        int workers = Runtime.getRuntime().availableProcessors();

        ExecutorService threadPool = Executors.newFixedThreadPool( workers );
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            Set<Long> allNodes = new HashSet<>( existingNodes );
            try ( Transaction tx = beginTransaction() )
            {
                int nodeInTx = random.nextInt( 1000 );
                allNodes.addAll( createNodesWithLabel( tx.dataWrite(), label, nodeInTx ) );
                Scan<NodeLabelIndexCursor> scan = tx.dataRead().nodeLabelScan( label );

                List<Future<List<Long>>> futures = new ArrayList<>( workers );
                for ( int j = 0; j < workers; j++ )
                {
                    futures.add(
                            threadPool.submit(
                                    randomBatchWorker( scan, cursors::allocateNodeLabelIndexCursor, NODE_GET ) ) );
                }

                List<List<Long>> lists = futures.stream().map( TestUtils::unsafeGet ).collect( Collectors.toList() );

                assertDistinct( lists );
                List<Long> concat = concat( lists );
                assertEquals( format( "nodes=%d, seen=%d, all=%d", nodeInTx, concat.size(), allNodes.size() ),
                        allNodes, new HashSet<>( concat ) );
                assertEquals( format( "nodes=%d", nodeInTx ), allNodes.size(), concat.size() );
                tx.failure();
            }
        }

        threadPool.shutdown();
        threadPool.awaitTermination( 1, TimeUnit.MINUTES );
    }

    private List<Long> createNodesWithLabel( int label, int size )
            throws KernelException
    {
        List<Long> ids;
        try ( Transaction tx = beginTransaction() )
        {
            Write write = tx.dataWrite();
            ids = createNodesWithLabel( write, label, size );
            tx.success();
        }
        return ids;
    }

    private List<Long> createNodesWithLabel( Write write, int label, int size )
            throws KernelException
    {
        List<Long> ids = new ArrayList<>( size );
        for ( int i = 0; i < size; i++ )
        {
            long node = write.nodeCreate();
            write.nodeAddLabel( node, label );
            ids.add( node );
        }
        return ids;
    }

    private int label( String name )
            throws TransactionFailureException, IllegalTokenNameException, TooManyLabelsException
    {
        int label;
        try ( Transaction tx = beginTransaction() )
        {
            label = tx.tokenWrite().labelGetOrCreateForName( name );
            tx.success();
        }
        return label;
    }
}
