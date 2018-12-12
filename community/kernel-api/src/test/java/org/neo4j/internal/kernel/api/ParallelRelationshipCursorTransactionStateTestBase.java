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
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.block.procedure.checked.primitive.CheckedLongProcedure;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.TestUtils.assertDistinct;
import static org.neo4j.internal.kernel.api.TestUtils.concat;
import static org.neo4j.internal.kernel.api.TestUtils.count;
import static org.neo4j.internal.kernel.api.TestUtils.randomBatchWorker;
import static org.neo4j.internal.kernel.api.TestUtils.singleBatchWorker;

public abstract class ParallelRelationshipCursorTransactionStateTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{

    private static final ToLongFunction<RelationshipScanCursor> REL_GET = RelationshipScanCursor::relationshipReference;

    @Test
    public void shouldHandleEmptyDatabase() throws TransactionFailureException
    {
        try ( Transaction tx = beginTransaction() )
        {
            try ( RelationshipScanCursor cursor = tx.cursors().allocateRelationshipScanCursor() )
            {
                Scan<RelationshipScanCursor> scan = tx.dataRead().allRelationshipsScan();
                while ( scan.reserveBatch( cursor, 23 ) )
                {
                    assertFalse( cursor.next() );
                }
            }
        }
    }

    @Test
    public void scanShouldNotSeeDeletedRelationships() throws Exception
    {
        int size = 100;
        MutableLongSet created = LongSets.mutable.empty();
        MutableLongSet deleted =  LongSets.mutable.empty();
        try ( Transaction tx = beginTransaction() )
        {
            Write write = tx.dataWrite();
            int type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            for ( int i = 0; i < size; i++ )
            {
                created.add( write.relationshipCreate( write.nodeCreate(), type, write.nodeCreate() ) );
                deleted.add( write.relationshipCreate( write.nodeCreate(), type, write.nodeCreate() ) );
            }
            tx.success();
        }

        try ( Transaction tx = beginTransaction() )
        {
            deleted.each( new CheckedLongProcedure()
            {
                @Override
                public void safeValue( long item ) throws Exception
                {
                    tx.dataWrite().relationshipDelete( item );
                }
            } );

            try ( RelationshipScanCursor cursor = tx.cursors().allocateRelationshipScanCursor() )
            {
                Scan<RelationshipScanCursor> scan = tx.dataRead().allRelationshipsScan();
                MutableLongSet seen =  LongSets.mutable.empty();
                while ( scan.reserveBatch( cursor, 17 ) )
                {
                    while ( cursor.next() )
                    {
                        long relationshipId = cursor.relationshipReference();
                        assertTrue( seen.add( relationshipId ) );
                        assertTrue( created.remove( relationshipId ) );
                    }
                }

                assertTrue( created.isEmpty() );
            }
        }
    }

    @Test
    public void scanShouldSeeAddedRelationships() throws Exception
    {
        int size = 100;
        MutableLongSet existing = createRelationships( size );
        MutableLongSet added = LongSets.mutable.empty();

        try ( Transaction tx = beginTransaction() )
        {
            Write write = tx.dataWrite();
            int type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );

            for ( int i = 0; i < size; i++ )
            {
                added.add( write.relationshipCreate( write.nodeCreate(), type, write.nodeCreate() ) );
            }

            try ( RelationshipScanCursor cursor = tx.cursors().allocateRelationshipScanCursor() )
            {
                Scan<RelationshipScanCursor> scan = tx.dataRead().allRelationshipsScan();
                MutableLongSet seen = LongSets.mutable.empty();
                while ( scan.reserveBatch( cursor, 17 ) )
                {
                    while ( cursor.next() )
                    {
                        long relationshipId = cursor.relationshipReference();
                        assertTrue( seen.add( relationshipId ) );
                        assertTrue( existing.remove( relationshipId ) || added.remove( relationshipId ) );
                    }
                }

                //make sure we have seen all relationships
                assertTrue( existing.isEmpty() );
                assertTrue( added.isEmpty() );
            }
        }
    }

    @Test
    public void shouldReserveBatchFromTxState()
            throws TransactionFailureException, InvalidTransactionTypeKernelException, IllegalTokenNameException,
            EntityNotFoundException
    {
        try ( Transaction tx = beginTransaction() )
        {
            Write write = tx.dataWrite();
            int type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            for ( int i = 0; i < 11; i++ )
            {
                write.relationshipCreate( write.nodeCreate(), type, write.nodeCreate() );
            }

            try ( RelationshipScanCursor cursor = tx.cursors().allocateRelationshipScanCursor() )
            {
                Scan<RelationshipScanCursor> scan = tx.dataRead().allRelationshipsScan();
                assertTrue( scan.reserveBatch( cursor, 5 ) );
                assertEquals( 5, count( cursor ) );
                assertTrue( scan.reserveBatch( cursor, 4 ) );
                assertEquals( 4, count( cursor ) );
                assertTrue( scan.reserveBatch( cursor, 6 ) );
                assertEquals( 2, count( cursor ) );
                //now we should have fetched all relationships
                while ( scan.reserveBatch( cursor, 3 ) )
                {
                    assertFalse( cursor.next() );
                }
            }
        }
    }

    @Test
    public void shouldScanAllRelationshipsFromMultipleThreads()
            throws InterruptedException, ExecutionException, TransactionFailureException,
            InvalidTransactionTypeKernelException, IllegalTokenNameException, EntityNotFoundException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        int size = 128;
        LongArrayList ids = new LongArrayList();
        try ( Transaction tx = beginTransaction() )
        {
            Write write = tx.dataWrite();
            int type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            for ( int i = 0; i < size; i++ )
            {
                ids.add( write.relationshipCreate( write.nodeCreate(), type, write.nodeCreate() ) );
            }

            Read read = tx.dataRead();
            Scan<RelationshipScanCursor> scan = read.allRelationshipsScan();

            // when
            Future<LongList> future1 = service.submit( singleBatchWorker( scan, cursors::allocateRelationshipScanCursor,
                    REL_GET, size / 4 ) );
            Future<LongList> future2 = service.submit( singleBatchWorker( scan, cursors::allocateRelationshipScanCursor,
                    REL_GET, size / 4 ) );
            Future<LongList> future3 = service.submit( singleBatchWorker( scan, cursors::allocateRelationshipScanCursor,
                    REL_GET, size / 4 ) );
            Future<LongList> future4 = service.submit( singleBatchWorker( scan, cursors::allocateRelationshipScanCursor,
                    REL_GET, size / 4 ) );

            // then
            LongList ids1 = future1.get();
            LongList ids2 = future2.get();
            LongList ids3 = future3.get();
            LongList ids4 = future4.get();

            assertDistinct( ids1, ids2, ids3, ids4 );
            LongList concat = concat( ids1, ids2, ids3, ids4 );
            assertEquals( ids.toSortedList(), concat.toSortedList() );
            tx.failure();
        }
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    @Test
    public void shouldScanAllRelationshipsFromMultipleThreadWithBigSizeHints()
            throws InterruptedException, ExecutionException, TransactionFailureException,
            InvalidTransactionTypeKernelException, IllegalTokenNameException, EntityNotFoundException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        int size = 128;
        LongArrayList ids = new LongArrayList();
        try ( Transaction tx = beginTransaction() )
        {
            Write write = tx.dataWrite();
            int type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            for ( int i = 0; i < size; i++ )
            {
                ids.add( write.relationshipCreate( write.nodeCreate(), type, write.nodeCreate() ) );
            }

            Read read = tx.dataRead();
            Scan<RelationshipScanCursor> scan = read.allRelationshipsScan();

            // when
            Supplier<RelationshipScanCursor> allocateCursor = cursors::allocateRelationshipScanCursor;
            Future<LongList> future1 = service.submit( singleBatchWorker( scan, allocateCursor, REL_GET, 100 ) );
            Future<LongList> future2 = service.submit( singleBatchWorker( scan, allocateCursor, REL_GET, 100 ) );
            Future<LongList> future3 = service.submit( singleBatchWorker( scan, allocateCursor, REL_GET, 100 ) );
            Future<LongList> future4 = service.submit( singleBatchWorker( scan, allocateCursor, REL_GET, 100 ) );

            // then
            LongList ids1 = future1.get();
            LongList ids2 = future2.get();
            LongList ids3 = future3.get();
            LongList ids4 = future4.get();

            assertDistinct( ids1, ids2, ids3, ids4 );
            LongList concat = concat( ids1, ids2, ids3, ids4 );
            assertEquals( ids.toSortedList(), concat.toSortedList() );
        }
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    @Test
    public void shouldScanAllRelationshipFromRandomlySizedWorkers()
            throws InterruptedException, TransactionFailureException,
            InvalidTransactionTypeKernelException, EntityNotFoundException, IllegalTokenNameException
    {
        // given
        ExecutorService service = Executors.newFixedThreadPool( 4 );
        int size = 128;
        LongArrayList ids = new LongArrayList();

        try ( Transaction tx = beginTransaction() )
        {
            Write write = tx.dataWrite();
            int type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            for ( int i = 0; i < size; i++ )
            {
                ids.add( write.relationshipCreate( write.nodeCreate(), type, write.nodeCreate() ) );
            }

            Read read = tx.dataRead();
            Scan<RelationshipScanCursor> scan = read.allRelationshipsScan();
            CursorFactory cursors = testSupport.kernelToTest().cursors();

            // when
            ArrayList<Future<LongList>> futures = new ArrayList<>();
            for ( int i = 0; i < 10; i++ )
            {
                futures.add(
                        service.submit( randomBatchWorker( scan, cursors::allocateRelationshipScanCursor, REL_GET ) ) );
            }

            // then
            List<LongList> lists = futures.stream().map( TestUtils::unsafeGet ).collect( Collectors.toList() );

            assertDistinct( lists );
            LongList concat = concat( lists );

            assertEquals( ids.toSortedList(), concat.toSortedList() );
            tx.failure();
        }
        finally
        {
            service.shutdown();
            service.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    @Test
    public void parallelTxStateScanStressTest()
            throws InvalidTransactionTypeKernelException, TransactionFailureException, InterruptedException,
            IllegalTokenNameException, EntityNotFoundException
    {
        LongSet existingRelationships = createRelationships( 77 );
        int workers = Runtime.getRuntime().availableProcessors();
        ExecutorService threadPool = Executors.newFixedThreadPool( workers );
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        try
        {
            for ( int i = 0; i < 1000; i++ )
            {
                MutableLongSet allRels = LongSets.mutable.withAll( existingRelationships );
                try ( Transaction tx = beginTransaction() )
                {
                    int relationshipsInTx = random.nextInt( 100 );
                    Write write = tx.dataWrite();
                    int type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
                    for ( int j = 0; j < relationshipsInTx; j++ )
                    {
                        allRels.add( write.relationshipCreate( write.nodeCreate(), type, write.nodeCreate() ) );
                    }

                    Scan<RelationshipScanCursor> scan = tx.dataRead().allRelationshipsScan();

                    List<Future<LongList>> futures = new ArrayList<>( workers );
                    for ( int j = 0; j < workers; j++ )
                    {
                        futures.add( threadPool.submit(
                                randomBatchWorker( scan, cursors::allocateRelationshipScanCursor, REL_GET ) ) );
                    }

                    List<LongList> lists =
                            futures.stream().map( TestUtils::unsafeGet ).collect( Collectors.toList() );

                    assertDistinct( lists );
                    LongList concat = concat( lists );
                    assertEquals( allRels, LongSets.immutable.withAll( concat ),
                            format( "relationships=%d, seen=%d, all=%d", relationshipsInTx, concat.size(),
                                    allRels.size() ) );
                    assertEquals( allRels.size(), concat.size(), format( "relationships=%d", relationshipsInTx ) );
                    tx.failure();
                }
            }
        }
        finally
        {
            threadPool.shutdown();
            threadPool.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    private MutableLongSet createRelationships( int size )
            throws TransactionFailureException, InvalidTransactionTypeKernelException, IllegalTokenNameException,
            EntityNotFoundException
    {
        MutableLongSet rels = LongSets.mutable.empty();
        try ( Transaction tx = beginTransaction() )
        {
            Write write = tx.dataWrite();
            int type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            for ( int i = 0; i < size; i++ )
            {
                rels.add( write.relationshipCreate( write.nodeCreate(), type, write.nodeCreate() ) );
            }
            tx.success();
        }
        return rels;
    }
}
