/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.hamcrest.Matchers;
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

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Write;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.kernel.impl.newapi.TestUtils.assertDistinct;
import static org.neo4j.kernel.impl.newapi.TestUtils.concat;
import static org.neo4j.kernel.impl.newapi.TestUtils.randomBatchWorker;
import static org.neo4j.kernel.impl.newapi.TestUtils.singleBatchWorker;

public abstract class ParallelNodeLabelScanTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    private static final int NUMBER_OF_NODES = 1000;
    private static int FOO_LABEL;
    private static int BAR_LABEL;
    private static LongSet FOO_NODES;
    private static LongSet BAR_NODES;
    private static int[] ALL_LABELS = new int[]{FOO_LABEL, BAR_LABEL};
    private static final ToLongFunction<NodeLabelIndexCursor> NODE_GET = NodeLabelIndexCursor::nodeReference;

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        MutableLongSet fooNodes = LongSets.mutable.empty();
        MutableLongSet barNodes = LongSets.mutable.empty();
        try ( Transaction tx = beginTransaction() )
        {
            TokenWrite tokenWrite = tx.tokenWrite();
            FOO_LABEL = tokenWrite.labelGetOrCreateForName( "foo" );
            BAR_LABEL = tokenWrite.labelGetOrCreateForName( "bar" );
            Write write = tx.dataWrite();
            for ( int i = 0; i < NUMBER_OF_NODES; i++ )
            {
                long node = write.nodeCreate();

                if ( i % 2 == 0 )
                {
                    write.nodeAddLabel( node, FOO_LABEL );
                    fooNodes.add( node );
                }
                else
                {
                    write.nodeAddLabel( node, BAR_LABEL );
                    barNodes.add( node );
                }
            }

            FOO_NODES = fooNodes;
            BAR_NODES = barNodes;

            tx.success();
        }
        catch ( KernelException e )
        {
            throw new AssertionError( e );
        }
    }

    @Test
    void shouldScanASubsetOfNodes()
    {
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor() )
        {
            for ( int label : ALL_LABELS )
            {
                Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( label );
                assertTrue( scan.reserveBatch( nodes, 11 ) );

               MutableLongList found = LongLists.mutable.empty();
                while ( nodes.next() )
                {
                    found.add( nodes.nodeReference() );
                }

                assertThat( found.size(), Matchers.greaterThan( 0 ) );

                if ( label == FOO_LABEL )
                {

                    assertTrue( FOO_NODES.containsAll( found ) );
                    assertTrue( found.noneSatisfy( f -> BAR_NODES.contains( f ) ) );
                }
                else if ( label == BAR_LABEL )
                {
                    assertTrue( BAR_NODES.containsAll( found ) );
                    assertTrue( found.noneSatisfy( f -> FOO_NODES.contains( f ) ) );
                }
                else
                {
                    fail();
                }
            }
        }
    }

    @Test
    void shouldHandleSizeHintOverflow()
    {
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( FOO_LABEL );
            assertTrue( scan.reserveBatch( nodes, NUMBER_OF_NODES * 2 ) );

            MutableLongList ids = LongLists.mutable.empty();
            while ( nodes.next() )
            {
                ids.add( nodes.nodeReference() );
            }

            assertEquals( FOO_NODES.size(), ids.size() );
            assertTrue( FOO_NODES.containsAll( ids ) );
            assertTrue( ids.noneSatisfy( f -> BAR_NODES.contains( f ) ) );
        }
    }

    @Test
    void shouldFailForSizeHintZero()
    {
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor() )
        {
            // given
            Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( FOO_LABEL );

            // when
            assertThrows( IllegalArgumentException.class , () -> scan.reserveBatch( nodes, 0 ) );
        }
    }

    @Test
    void shouldScanAllNodesInBatches()
    {
        // given
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( FOO_LABEL );
            MutableLongList ids = LongLists.mutable.empty();
            while ( scan.reserveBatch( nodes, 3 ) )
            {
                while ( nodes.next() )
                {
                    ids.add( nodes.nodeReference() );
                }
            }

            // then
            assertEquals( FOO_NODES.size(), ids.size() );
            assertTrue( FOO_NODES.containsAll( ids ) );
            assertTrue( ids.noneSatisfy( f -> BAR_NODES.contains( f ) ) );
        }
    }

    @Test
    void shouldScanAllNodesFromMultipleThreads() throws InterruptedException, ExecutionException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( BAR_LABEL );
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        try
        {
            // when
            Supplier<NodeLabelIndexCursor> allocateCursor = cursors::allocateNodeLabelIndexCursor;
            Future<LongList> future1 =
                    service.submit( singleBatchWorker( scan, allocateCursor, NODE_GET, NUMBER_OF_NODES ) );
            Future<LongList> future2 =
                    service.submit( singleBatchWorker( scan, allocateCursor, NODE_GET, NUMBER_OF_NODES ) );
            Future<LongList> future3 =
                    service.submit( singleBatchWorker( scan, allocateCursor, NODE_GET, NUMBER_OF_NODES ) );
            Future<LongList> future4 =
                    service.submit( singleBatchWorker( scan, allocateCursor, NODE_GET, NUMBER_OF_NODES ) );

            // then
            LongList ids1 = future1.get();
            LongList ids2 = future2.get();
            LongList ids3 = future3.get();
            LongList ids4 = future4.get();

            assertDistinct( ids1, ids2, ids3, ids4 );
            assertEquals( BAR_NODES, LongSets.immutable.withAll( concat( ids1, ids2, ids3, ids4 ) ) );
        }
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    @Test
    void shouldScanAllNodesFromRandomlySizedWorkers() throws InterruptedException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( FOO_LABEL );
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        try
        {
            // when
            ArrayList<Future<LongList>> futures = new ArrayList<>();
            for ( int i = 0; i < 10; i++ )
            {
                futures.add(
                        service.submit( randomBatchWorker( scan, cursors::allocateNodeLabelIndexCursor, NODE_GET ) ) );
            }

            // then
            List<LongList> lists = futures.stream().map( TestUtils::unsafeGet ).collect( Collectors.toList() );

            assertDistinct( lists );
            assertEquals( FOO_NODES, LongSets.immutable.withAll(  concat( lists ) ) );
        }
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }
}
