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
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.Scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.newapi.TestUtils.assertDistinct;
import static org.neo4j.kernel.impl.newapi.TestUtils.concat;
import static org.neo4j.kernel.impl.newapi.TestUtils.randomBatchWorker;
import static org.neo4j.kernel.impl.newapi.TestUtils.singleBatchWorker;

public abstract class ParallelRelationshipCursorTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    private static LongList RELATIONSHIPS;
    private static final int NUMBER_OF_RELATIONSHIPS = 128;
    private static final ToLongFunction<RelationshipScanCursor> REL_GET = RelationshipScanCursor::relationshipReference;

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            MutableLongList list = new LongArrayList( NUMBER_OF_RELATIONSHIPS );
            for ( int i = 0; i < NUMBER_OF_RELATIONSHIPS; i++ )
            {
                list.add( graphDb.createNode()
                        .createRelationshipTo( graphDb.createNode(), RelationshipType.withName( "R" ) ).getId() );
            }
            RELATIONSHIPS = list;
            tx.success();
        }
    }

    @Test
    void shouldScanASubsetOfRelationships()
    {
        try ( RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor() )
        {
            // when
            Scan<RelationshipScanCursor> scan = read.allRelationshipsScan();
            assertTrue( scan.reserveBatch( relationships, 3 ) );

            assertTrue( relationships.next() );
            assertEquals( RELATIONSHIPS.get( 0 ), relationships.relationshipReference() );
            assertTrue( relationships.next() );
            assertEquals( RELATIONSHIPS.get( 1 ), relationships.relationshipReference() );
            assertTrue( relationships.next() );
            assertEquals( RELATIONSHIPS.get( 2 ), relationships.relationshipReference() );
            assertFalse( relationships.next() );
        }
    }

    @Test
    void shouldHandleSizeHintOverflow()
    {
        try ( RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor() )
        {
            // when
            Scan<RelationshipScanCursor> scan = read.allRelationshipsScan();
            assertTrue( scan.reserveBatch( relationships, NUMBER_OF_RELATIONSHIPS * 2 ) );

            LongArrayList ids = new LongArrayList();
            while ( relationships.next() )
            {
                ids.add( relationships.relationshipReference() );
            }

            assertEquals( RELATIONSHIPS, ids );
        }
    }

    @Test
    void shouldFailForSizeHintZero()
    {
        try ( RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor() )
        {
            // given
            Scan<RelationshipScanCursor> scan = read.allRelationshipsScan();

            // when
            assertThrows( IllegalArgumentException.class, () -> scan.reserveBatch( relationships, 0 ) );
        }
    }

    @Test
    void shouldScanAllRelationshipsInBatches()
    {
        // given
        LongArrayList ids = new LongArrayList();
        try ( RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor() )
        {
            // when
            Scan<RelationshipScanCursor> scan = read.allRelationshipsScan();
            while ( scan.reserveBatch( relationships, 3 ) )
            {
                while ( relationships.next() )
                {
                    ids.add( relationships.relationshipReference() );
                }
            }
        }

        // then
        assertEquals( RELATIONSHIPS, ids );
    }

    @Test
    void shouldScanAllRelationshipsFromMultipleThreads() throws InterruptedException, ExecutionException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<RelationshipScanCursor> scan = read.allRelationshipsScan();
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        try
        {
            // when
            Future<LongList> future1 = service.submit(
                    singleBatchWorker( scan, cursors::allocateRelationshipScanCursor, REL_GET, 32 ) );
            Future<LongList> future2 = service.submit(
                    singleBatchWorker( scan, cursors::allocateRelationshipScanCursor, REL_GET, 32 ) );
            Future<LongList> future3 = service.submit(
                    singleBatchWorker( scan, cursors::allocateRelationshipScanCursor, REL_GET, 32 ) );
            Future<LongList> future4 = service.submit(
                    singleBatchWorker( scan, cursors::allocateRelationshipScanCursor, REL_GET, 32 ) );

            // then
            LongList ids1 = future1.get();
            LongList ids2 = future2.get();
            LongList ids3 = future3.get();
            LongList ids4 = future4.get();

            assertDistinct( ids1, ids2, ids3, ids4 );
            LongList concat = concat( ids1, ids2, ids3, ids4 ).toSortedList();
            assertEquals( RELATIONSHIPS, concat );
        }
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    @Test
    void shouldScanAllRelationshipsFromMultipleThreadWithBigSizeHints() throws InterruptedException, ExecutionException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<RelationshipScanCursor> scan = read.allRelationshipsScan();
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        try
        {
            // when
            Supplier<RelationshipScanCursor> allocateRelCursor = cursors::allocateRelationshipScanCursor;
            Future<LongList> future1 = service.submit( singleBatchWorker( scan, allocateRelCursor, REL_GET, 100 ) );
            Future<LongList> future2 = service.submit( singleBatchWorker( scan, allocateRelCursor, REL_GET, 100 ) );
            Future<LongList> future3 = service.submit( singleBatchWorker( scan, allocateRelCursor, REL_GET, 100 ) );
            Future<LongList> future4 = service.submit( singleBatchWorker( scan, allocateRelCursor, REL_GET, 100 ) );

            // then
            LongList ids1 = future1.get();
            LongList ids2 = future2.get();
            LongList ids3 = future3.get();
            LongList ids4 = future4.get();

            assertDistinct( ids1, ids2, ids3, ids4 );
            LongList concat = concat( ids1, ids2, ids3, ids4 ).toSortedList();
            assertEquals( RELATIONSHIPS, concat );
        }
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    @Test
    void shouldScanAllRelationshipsFromRandomlySizedWorkers() throws InterruptedException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        Scan<RelationshipScanCursor> scan = read.allRelationshipsScan();
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        try
        {
            // when
            ArrayList<Future<LongList>> futures = new ArrayList<>();
            for ( int i = 0; i < 11; i++ )
            {
                futures.add( service.submit( randomBatchWorker( scan, cursors::allocateRelationshipScanCursor, REL_GET ) ) );
            }

            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );

            // then
            List<LongList> lists = futures.stream().map( TestUtils::unsafeGet ).collect( Collectors.toList() );

            assertDistinct( lists );
            LongList concat = concat( lists ).toSortedList();
            assertEquals( RELATIONSHIPS, concat );
        }
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }
}
