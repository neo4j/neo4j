/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.WorkerContext;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.newapi.TestUtils.assertDistinct;
import static org.neo4j.kernel.impl.newapi.TestUtils.closeWorkContexts;
import static org.neo4j.kernel.impl.newapi.TestUtils.concat;
import static org.neo4j.kernel.impl.newapi.TestUtils.count;
import static org.neo4j.kernel.impl.newapi.TestUtils.createContexts;
import static org.neo4j.kernel.impl.newapi.TestUtils.createRandomWorkers;
import static org.neo4j.kernel.impl.newapi.TestUtils.createWorkers;
import static org.neo4j.util.concurrent.Futures.getAllResults;

public abstract class ParallelNodeLabelScanTransactionStateTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{
    private static final ToLongFunction<NodeLabelIndexCursor> NODE_GET = NodeLabelIndexCursor::nodeReference;

    @Test
    void shouldHandleEmptyDatabase() throws KernelException
    {
        try ( KernelTransaction tx = beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "L" );
            CursorContext cursorContext = tx.cursorContext();
            try ( NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor( cursorContext ) )
            {
                Scan<NodeLabelIndexCursor> scan = tx.dataRead().nodeLabelScan( label );
                while ( scan.reserveBatch( cursor, 23, cursorContext, tx.securityContext().mode() ) )
                {
                    assertFalse( cursor.next() );
                }
            }
        }
    }

    @Test
    void scanShouldNotSeeDeletedNode() throws Exception
    {
        int size = 1000;
        Set<Long> created = new HashSet<>( size );
        Set<Long> deleted = new HashSet<>( size );
        int label = label( "L" );
        try ( KernelTransaction tx = beginTransaction() )
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
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            for ( long delete : deleted )
            {
                tx.dataWrite().nodeDelete( delete );
            }

            CursorContext cursorContext = tx.cursorContext();
            try ( NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor( cursorContext ) )
            {
                Scan<NodeLabelIndexCursor> scan = tx.dataRead().nodeLabelScan( label );
                Set<Long> seen = new HashSet<>();
                while ( scan.reserveBatch( cursor, 128, cursorContext, tx.securityContext().mode() ) )
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
    void scanShouldSeeAddedNodes() throws Exception
    {
        int size = 64;
        int label = label( "L" );
        MutableLongSet existing = LongSets.mutable.withAll( createNodesWithLabel( label, size ) );
        try ( KernelTransaction tx = beginTransaction() )
        {
            MutableLongSet added = LongSets.mutable.withAll( createNodesWithLabel( tx.dataWrite(), label, size ) );

            CursorContext cursorContext = tx.cursorContext();
            try ( NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor( cursorContext ) )
            {
                Scan<NodeLabelIndexCursor> scan = tx.dataRead().nodeLabelScan( label );
                Set<Long> seen = new HashSet<>();
                while ( scan.reserveBatch( cursor, 64, cursorContext, tx.securityContext().mode() ) )
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
    void shouldReserveBatchFromTxState() throws KernelException
    {
        try ( KernelTransaction tx = beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "L" );
            createNodesWithLabel( tx.dataWrite(), label, 11 );

            CursorContext cursorContext = tx.cursorContext();
            try ( NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor( cursorContext ) )
            {
                Scan<NodeLabelIndexCursor> scan = tx.dataRead().nodeLabelScan( label );
                AccessMode accessMode = tx.securityContext().mode();
                assertTrue( scan.reserveBatch( cursor, 5, cursorContext, accessMode ) );
                assertEquals( 5, count( cursor ) );
                assertTrue( scan.reserveBatch( cursor, 4, cursorContext, accessMode ) );
                assertEquals( 4, count( cursor ) );
                assertTrue( scan.reserveBatch( cursor, 6, cursorContext, accessMode ) );
                assertEquals( 2, count( cursor ) );
                //now we should have fetched all nodes
                while ( scan.reserveBatch( cursor, 3, cursorContext, accessMode ) )
                {
                    assertFalse( cursor.next() );
                }
            }
        }
    }

    @Test
    void shouldScanAllNodesFromMultipleThreads()
            throws InterruptedException, ExecutionException, KernelException
    {
        // given
        int numberOfWorkers = 4;
        ExecutorService service = Executors.newFixedThreadPool( numberOfWorkers );
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        int size = 1024;

        try ( KernelTransaction tx = beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "L" );
            LongList ids = createNodesWithLabel( tx.dataWrite(), label, size );

            Read read = tx.dataRead();
            Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( label );

            List<WorkerContext<NodeLabelIndexCursor>> workers = createContexts( tx, cursors::allocateNodeLabelIndexCursor, numberOfWorkers );
            List<Future<LongList>> futures = service.invokeAll( createWorkers( size / numberOfWorkers, scan, numberOfWorkers, workers, NODE_GET ) );

            List<LongList> lists = getAllResults( futures );
            closeWorkContexts( workers );

            assertDistinct( lists );
            LongList concat = concat( lists );
            assertEquals( ids.toSortedList(), concat.toSortedList() );
            tx.rollback();
        }
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    @Test
    void shouldScanAllNodesFromRandomlySizedWorkers() throws InterruptedException, KernelException, ExecutionException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        int size = 2000;

        try ( KernelTransaction tx = beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "L" );
            LongList ids = createNodesWithLabel( tx.dataWrite(), label, size );

            Read read = tx.dataRead();
            Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( label );
            CursorFactory cursors = testSupport.kernelToTest().cursors();

            // when
            int numberOfWorkers = 10;
            List<WorkerContext<NodeLabelIndexCursor>> workers = createContexts( tx, cursors::allocateNodeLabelIndexCursor, numberOfWorkers );
            List<Future<LongList>> futures = service.invokeAll( createRandomWorkers( scan, numberOfWorkers, workers, NODE_GET ) );

            // then
            List<LongList> lists = getAllResults( futures );

            assertDistinct( lists );
            assertEquals( ids.toSortedList(), concat( lists ).toSortedList() );
            tx.rollback();
        }
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    @Test
    void parallelTxStateScanStressTest() throws KernelException, InterruptedException, ExecutionException
    {
        int label = label( "L" );
        MutableLongSet existingNodes = LongSets.mutable.withAll( createNodesWithLabel( label, 1000 ) );

        int numberOfWorkers = Runtime.getRuntime().availableProcessors();

        ExecutorService threadPool = Executors.newFixedThreadPool( numberOfWorkers );
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        try
        {
            for ( int i = 0; i < 1000; i++ )
            {
                MutableLongSet allNodes = LongSets.mutable.withAll( existingNodes );
                try ( KernelTransaction tx = beginTransaction() )
                {
                    int nodeInTx = random.nextInt( 1000 );
                    allNodes.addAll( createNodesWithLabel( tx.dataWrite(), label, nodeInTx ) );
                    Scan<NodeLabelIndexCursor> scan = tx.dataRead().nodeLabelScan( label );

                    List<WorkerContext<NodeLabelIndexCursor>> workers = createContexts( tx, cursors::allocateNodeLabelIndexCursor, numberOfWorkers );
                    List<Future<LongList>> futures = threadPool.invokeAll( createRandomWorkers( scan, numberOfWorkers, workers, NODE_GET ) );

                    List<LongList> lists = getAllResults( futures );

                    assertDistinct( lists );
                    LongList concat = concat( lists );
                    assertEquals( allNodes, LongSets.immutable.withAll( concat ),
                            format( "nodes=%d, seen=%d, all=%d", nodeInTx, concat.size(), allNodes.size() ) );
                    assertEquals( allNodes.size(), concat.size(), format( "nodes=%d", nodeInTx ) );
                }
            }
        }
        finally
        {
            threadPool.shutdown();
            threadPool.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    private static LongList createNodesWithLabel( int label, int size )
            throws KernelException
    {
        LongList ids;
        try ( KernelTransaction tx = beginTransaction() )
        {
            Write write = tx.dataWrite();
            ids = createNodesWithLabel( write, label, size );
            tx.commit();
        }
        return ids;
    }

    private static LongList createNodesWithLabel( Write write, int label, int size )
            throws KernelException
    {
        MutableLongList ids = LongLists.mutable.empty();
        for ( int i = 0; i < size; i++ )
        {
            long node = write.nodeCreate();
            write.nodeAddLabel( node, label );
            ids.add( node );
        }
        return ids;
    }

    private static int label( String name ) throws KernelException
    {
        int label;
        try ( KernelTransaction tx = beginTransaction() )
        {
            label = tx.tokenWrite().labelGetOrCreateForName( name );
            tx.commit();
        }
        return label;
    }
}
