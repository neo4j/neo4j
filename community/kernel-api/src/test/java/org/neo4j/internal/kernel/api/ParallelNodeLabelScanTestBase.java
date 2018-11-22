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


import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.internal.kernel.api.TestUtils.assertDistinct;
import static org.neo4j.internal.kernel.api.TestUtils.concat;
import static org.neo4j.internal.kernel.api.TestUtils.randomBatchWorker;
import static org.neo4j.internal.kernel.api.TestUtils.singleBatchWorker;

public abstract class ParallelNodeLabelScanTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    private static final int NUMBER_OF_NODES = 1000;
    private static int FOO_LABEL;
    private static int BAR_LABEL;
    private static Set<Long> FOO_NODES;
    private static Set<Long> BAR_NODES;
    private static int[] ALL_LABELS = new int[]{FOO_LABEL, BAR_LABEL};
    private static final Function<NodeLabelIndexCursor,Long> NODE_GET = NodeLabelIndexCursor::nodeReference;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        FOO_NODES = new HashSet<>( NUMBER_OF_NODES / 2 );
        BAR_NODES = new HashSet<>( NUMBER_OF_NODES / 2 );
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
                    FOO_NODES.add( node );
                }
                else
                {
                    write.nodeAddLabel( node, BAR_LABEL );
                    BAR_NODES.add( node );
                }
            }

            tx.success();
        }
        catch ( KernelException e )
        {
            throw new AssertionError( e );
        }
    }

    @Test
    public void shouldScanASubsetOfNodes()
    {
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor() )
        {
            for ( int label : ALL_LABELS )
            {
                Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( label );
                assertTrue( scan.reserveBatch( nodes, 11 ) );

                List<Long> found = new ArrayList<>();
                while ( nodes.next() )
                {
                    found.add( nodes.nodeReference() );
                }

                assertThat( found.size(), Matchers.greaterThan( 0 ) );

                if ( label == FOO_LABEL )
                {

                    assertTrue( FOO_NODES.containsAll( found ) );
                    assertTrue( found.stream().noneMatch( f -> BAR_NODES.contains( f ) ) );
                }
                else if ( label == BAR_LABEL )
                {
                    assertTrue( BAR_NODES.containsAll( found ) );
                    assertTrue( found.stream().noneMatch( f -> FOO_NODES.contains( f ) ) );
                }
                else
                {
                    fail();
                }
            }
        }
    }

    @Test
    public void shouldHandleSizeHintOverflow()
    {
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( FOO_LABEL );
            assertTrue( scan.reserveBatch( nodes, NUMBER_OF_NODES * 2 ) );

            List<Long> ids = new ArrayList<>();
            while ( nodes.next() )
            {
                ids.add( nodes.nodeReference() );
            }

            assertEquals( FOO_NODES.size(), ids.size() );
            assertTrue( FOO_NODES.containsAll( ids ) );
            assertTrue( ids.stream().noneMatch( f -> BAR_NODES.contains( f ) ) );
        }
    }

    @Test
    public void shouldFailForSizeHintZero()
    {
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor() )
        {
            // given
            Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( FOO_LABEL );

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
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( FOO_LABEL );
            List<Long> ids = new ArrayList<>();
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
            assertTrue( ids.stream().noneMatch( f -> BAR_NODES.contains( f ) ) );
        }
    }

    @Test
    public void shouldScanAllNodesFromMultipleThreads() throws InterruptedException, ExecutionException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( BAR_LABEL );
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        // when
        Supplier<NodeLabelIndexCursor> allocateCursor = cursors::allocateNodeLabelIndexCursor;
        Future<List<Long>> future1 =
                service.submit( singleBatchWorker( scan, allocateCursor, NODE_GET, NUMBER_OF_NODES ) );
        Future<List<Long>> future2 =
                service.submit( singleBatchWorker( scan, allocateCursor, NODE_GET, NUMBER_OF_NODES ) );
        Future<List<Long>> future3 =
                service.submit( singleBatchWorker( scan, allocateCursor, NODE_GET, NUMBER_OF_NODES ) );
        Future<List<Long>> future4 =
                service.submit( singleBatchWorker( scan, allocateCursor, NODE_GET, NUMBER_OF_NODES ) );

        // then
        List<Long> ids1 = future1.get();
        List<Long> ids2 = future2.get();
        List<Long> ids3 = future3.get();
        List<Long> ids4 = future4.get();

        assertDistinct( ids1, ids2, ids3, ids4 );
        assertEquals( BAR_NODES, new HashSet<>( concat( ids1, ids2, ids3, ids4 ) ) );

        service.shutdown();
        service.awaitTermination( 1, TimeUnit.MINUTES );
    }

    @Test
    public void shouldScanAllNodesFromRandomlySizedWorkers() throws InterruptedException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( FOO_LABEL );
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        // when
        ArrayList<Future<List<Long>>> futures = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            futures.add( service.submit( randomBatchWorker( scan, cursors::allocateNodeLabelIndexCursor, NODE_GET ) ) );
        }

        // then
        List<List<Long>> lists = futures.stream().map( TestUtils::unsafeGet ).collect( Collectors.toList() );

        assertDistinct( lists );
        assertEquals( FOO_NODES, new HashSet<>( concat( lists ) ) );

        service.shutdown();
        service.awaitTermination( 1, TimeUnit.MINUTES );
    }
}
