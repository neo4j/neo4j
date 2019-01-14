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
package org.neo4j.index.internal.gbptree;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.neo4j.cursor.RawCursor;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.test.rule.PageCacheRule.config;

/**
 * From a range of keys two disjunct sets are generated, "toAdd" and "toRemove".
 * In each "iteration" writer will grab enough work from toAdd and toRemove to fill up one "batch".
 * The batch will be applied to the GB+Tree during this iteration. The batch is also used to update
 * a set of keys that all readers MUST see.
 *
 * Readers are allowed to see more keys because they race with concurrent insertions, but they should
 * at least see every key that has been inserted in previous iterations or not yet removed in current
 * or previous iterations.
 *
 * The {@link TestCoordinator} is responsible for "planning" the execution of the test. It generates
 * toAdd and toRemove, prepare the GB+Tree with entries and serve readers and writer with information
 * about what they should do next.
 */
public abstract class GBPTreeConcurrencyITBase<KEY,VALUE>
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final RandomRule random = new RandomRule();

    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule ).around( random );

    private TestLayout<KEY,VALUE> layout;
    private GBPTree<KEY,VALUE> index;
    private final ExecutorService threadPool =
            Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );

    private GBPTree<KEY,VALUE> createIndex() throws IOException
    {
        int pageSize = 512;
        layout = getLayout( random );
        PageCache pageCache = pageCacheRule.getPageCache( fs.get(), config().withPageSize( pageSize ).withAccessChecks( true ) );
        return index = new GBPTreeBuilder<>( pageCache, directory.file( "index" ), layout ).build();
    }

    protected abstract TestLayout<KEY,VALUE> getLayout( RandomRule random );

    @After
    public void consistencyCheckAndClose() throws IOException
    {
        threadPool.shutdownNow();
        index.consistencyCheck();
        index.close();
    }

    @Test
    public void shouldReadForwardCorrectlyWithConcurrentInsert() throws Throwable
    {
        TestCoordinator testCoordinator = new TestCoordinator( random.random(), true, 1 );
        shouldReadCorrectlyWithConcurrentUpdates( testCoordinator );
    }

    @Test
    public void shouldReadBackwardCorrectlyWithConcurrentInsert() throws Throwable
    {
        TestCoordinator testCoordinator = new TestCoordinator( random.random(), false, 1 );
        shouldReadCorrectlyWithConcurrentUpdates( testCoordinator );
    }

    @Test
    public void shouldReadForwardCorrectlyWithConcurrentRemove() throws Throwable
    {
        TestCoordinator testCoordinator = new TestCoordinator( random.random(), true, 0 );
        shouldReadCorrectlyWithConcurrentUpdates( testCoordinator );
    }

    @Test
    public void shouldReadBackwardCorrectlyWithConcurrentRemove() throws Throwable
    {
        TestCoordinator testCoordinator = new TestCoordinator( random.random(), false, 0 );
        shouldReadCorrectlyWithConcurrentUpdates( testCoordinator );
    }

    @Test
    public void shouldReadForwardCorrectlyWithConcurrentUpdates() throws Throwable
    {
        TestCoordinator testCoordinator = new TestCoordinator( random.random(), true, 0.5 );
        shouldReadCorrectlyWithConcurrentUpdates( testCoordinator );
    }

    @Test
    public void shouldReadBackwardCorrectlyWithConcurrentUpdates() throws Throwable
    {
        TestCoordinator testCoordinator = new TestCoordinator( random.random(), false, 0.5 );
        shouldReadCorrectlyWithConcurrentUpdates( testCoordinator );
    }

    private void shouldReadCorrectlyWithConcurrentUpdates( TestCoordinator testCoordinator ) throws Throwable
    {
        // Readers config
        int readers = max( 1, Runtime.getRuntime().availableProcessors() - 1 );

        // Thread communication
        CountDownLatch readerReadySignal = new CountDownLatch( readers );
        CountDownLatch readerStartSignal = new CountDownLatch( 1 );
        AtomicBoolean endSignal = testCoordinator.endSignal();
        AtomicBoolean failHalt = new AtomicBoolean(); // Readers signal to writer that there is a failure
        AtomicReference<Throwable> readerError = new AtomicReference<>();

        // GIVEN
        index = createIndex();
        testCoordinator.prepare( index );

        // WHEN starting the readers
        RunnableReader readerTask = new RunnableReader( testCoordinator, readerReadySignal, readerStartSignal,
                endSignal, failHalt, readerError );
        for ( int i = 0; i < readers; i++ )
        {
            threadPool.submit( readerTask );
        }

        // and starting the checkpointer
        threadPool.submit( checkpointThread( endSignal, readerError, failHalt ) );

        // and starting the writer
        try
        {
            write( testCoordinator, readerReadySignal, readerStartSignal, endSignal, failHalt );
        }
        finally
        {
            // THEN no reader should have failed by the time we have finished all the scheduled updates.
            // A successful read means that all results were ordered and we saw all inserted values and
            // none of the removed values at the point of making the seek call.
            endSignal.set( true );
            threadPool.shutdown();
            threadPool.awaitTermination( 10, TimeUnit.SECONDS );
            if ( readerError.get() != null )
            {
                //noinspection ThrowFromFinallyBlock
                throw readerError.get();
            }
        }
    }

    private class TestCoordinator implements Supplier<ReaderInstruction>
    {
        private final Random random;

        // Range
        final long minRange = 0;
        final long maxRange = 1 << 13; // 8192

        // Instructions for writer
        private final int writeBatchSize;

        // Instructions for reader
        private final boolean forwardsSeek;
        private final double writePercentage;
        private final AtomicReference<ReaderInstruction> currentReaderInstruction;
        TreeSet<Long> readersShouldSee;

        // Progress
        private final AtomicBoolean endSignal;

        // Control for ADD and REMOVE
        Queue<Long> toRemove = new LinkedList<>();
        Queue<Long> toAdd = new LinkedList<>();
        List<UpdateOperation> updatesForNextIteration = new ArrayList<>();

        TestCoordinator( Random random, boolean forwardsSeek, double writePercentage )
        {
            this.endSignal = new AtomicBoolean();
            this.random = random;
            this.forwardsSeek = forwardsSeek;
            this.writePercentage = writePercentage;
            this.writeBatchSize = random.nextInt( 990 ) + 10; // 10-999
            currentReaderInstruction = new AtomicReference<>();
            Comparator<Long> comparator = forwardsSeek ? Comparator.naturalOrder() : Comparator.reverseOrder();
            readersShouldSee = new TreeSet<>( comparator );
        }

        List<Long> shuffleToNewList( List<Long> sourceList, Random random )
        {
            List<Long> shuffledList = new ArrayList<>( sourceList );
            Collections.shuffle( shuffledList, random );
            return shuffledList;
        }

        void prepare( GBPTree<KEY,VALUE> index ) throws IOException
        {
            prepareIndex( index, readersShouldSee, toRemove, toAdd, random );
            iterationFinished();
        }

        void prepareIndex( GBPTree<KEY,VALUE> index, TreeSet<Long> dataInIndex,
                Queue<Long> toRemove, Queue<Long> toAdd, Random random ) throws IOException
        {
            List<Long> fullRange = LongStream.range( minRange, maxRange ).boxed().collect( Collectors.toList() );
            List<Long> rangeOutOfOrder = shuffleToNewList( fullRange, random );
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                for ( Long key : rangeOutOfOrder )
                {
                    boolean addForRemoval = random.nextDouble() > writePercentage;
                    if ( addForRemoval )
                    {
                        writer.put( key( key ),value( key ) );
                        dataInIndex.add( key );
                        toRemove.add( key );
                    }
                    else
                    {
                        toAdd.add( key );
                    }
                }
            }
        }

        void iterationFinished()
        {
            // Create new set to not modify set that readers use concurrently
            readersShouldSee = new TreeSet<>( readersShouldSee );
            updateRecentlyInsertedData( readersShouldSee, updatesForNextIteration );
            updatesForNextIteration = generateUpdatesForNextIteration();
            updateWithSoonToBeRemovedData( readersShouldSee, updatesForNextIteration );
            currentReaderInstruction.set( newReaderInstruction( minRange, maxRange, readersShouldSee ) );
        }

        void updateRecentlyInsertedData( TreeSet<Long> readersShouldSee, List<UpdateOperation> updateBatch )
        {
            updateBatch.stream().filter( UpdateOperation::isInsert ).forEach( uo -> uo.applyToSet( readersShouldSee ) );
        }

        void updateWithSoonToBeRemovedData( TreeSet<Long> readersShouldSee, List<UpdateOperation> updateBatch )
        {
            updateBatch.stream().filter( uo -> !uo.isInsert() ).forEach( uo -> uo.applyToSet( readersShouldSee ) );
        }

        private ReaderInstruction newReaderInstruction( long minRange, long maxRange,
                TreeSet<Long> readersShouldSee )
        {
            return forwardsSeek ?
                   new ReaderInstruction( minRange, maxRange, readersShouldSee ) :
                   new ReaderInstruction( maxRange - 1, minRange, readersShouldSee );
        }

        private List<UpdateOperation> generateUpdatesForNextIteration()
        {
            List<UpdateOperation> updateOperations = new ArrayList<>();
            if ( toAdd.isEmpty() && toRemove.isEmpty() )
            {
                endSignal.set( true );
                return updateOperations;
            }

            int operationsInIteration = readersShouldSee.size() < 1000 ? 100 : readersShouldSee.size() / 10;
            int count = 0;
            while ( count < operationsInIteration && (!toAdd.isEmpty() || !toRemove.isEmpty()) )
            {
                UpdateOperation operation;
                if ( toAdd.isEmpty() )
                {
                    operation = new RemoveOperation( toRemove.poll() );
                }
                else if ( toRemove.isEmpty() )
                {
                    operation = new PutOperation( toAdd.poll() );
                }
                else
                {
                    boolean remove = random.nextDouble() > writePercentage;
                    if ( remove )
                    {
                        operation = new RemoveOperation( toRemove.poll() );
                    }
                    else
                    {
                        operation = new PutOperation( toAdd.poll() );
                    }
                }
                updateOperations.add( operation );
                count++;
            }
            return updateOperations;
        }

        Iterable<UpdateOperation> nextToWrite()
        {
            return updatesForNextIteration;
        }

        @Override
        public ReaderInstruction get()
        {
            return currentReaderInstruction.get();
        }

        AtomicBoolean endSignal()
        {
            return endSignal;
        }

        int writeBatchSize()
        {
            return writeBatchSize;
        }

        boolean isReallyExpected( long nextToSee )
        {
            return readersShouldSee.contains( nextToSee );
        }
    }

    private abstract class UpdateOperation
    {
        final long key;

        UpdateOperation( long key )
        {
            this.key = key;
        }

        abstract void apply( Writer<KEY,VALUE> writer ) throws IOException;

        abstract void applyToSet( Set<Long> set );

        abstract boolean isInsert();
    }

    private class PutOperation extends UpdateOperation
    {
        PutOperation( long key )
        {
            super( key );
        }

        @Override
        void apply( Writer<KEY,VALUE> writer ) throws IOException
        {
            writer.put( key( key ), value( key ) );
        }

        @Override
        void applyToSet( Set<Long> set )
        {
            set.add( key );
        }

        @Override
        boolean isInsert()
        {
            return true;
        }
    }

    private class RemoveOperation extends UpdateOperation
    {
        RemoveOperation( long key )
        {
            super( key );
        }

        @Override
        void apply( Writer<KEY,VALUE> writer ) throws IOException
        {
            writer.remove( key( key ) );
        }

        @Override
        void applyToSet( Set<Long> set )
        {
            set.remove( key );
        }

        @Override
        boolean isInsert()
        {
            return false;
        }
    }

    private void write( TestCoordinator testCoordinator, CountDownLatch readerReadySignal,
            CountDownLatch readerStartSignal,
            AtomicBoolean endSignal, AtomicBoolean failHalt ) throws InterruptedException, IOException
    {
        assertTrue( readerReadySignal.await( 10, SECONDS ) ); // Ready, set...
        readerStartSignal.countDown(); // GO!

        while ( !failHalt.get() && !endSignal.get() )
        {
            writeOneIteration( testCoordinator, failHalt );
            testCoordinator.iterationFinished();
        }
    }

    private void writeOneIteration( TestCoordinator testCoordinator,
            AtomicBoolean failHalt ) throws IOException, InterruptedException
    {
        int batchSize = testCoordinator.writeBatchSize();
        Iterable<UpdateOperation> toWrite = testCoordinator.nextToWrite();
        Iterator<UpdateOperation> toWriteIterator = toWrite.iterator();
        while ( toWriteIterator.hasNext() )
        {
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                int inBatch = 0;
                while ( toWriteIterator.hasNext() && inBatch < batchSize )
                {
                    UpdateOperation operation = toWriteIterator.next();
                    operation.apply( writer );
                    if ( failHalt.get() )
                    {
                        break;
                    }
                    inBatch++;
                }
            }
            // Sleep to allow checkpointer to step in
            MILLISECONDS.sleep( 1 );
        }
    }

    private class RunnableReader implements Runnable
    {
        private final CountDownLatch readerReadySignal;
        private final CountDownLatch readerStartSignal;
        private final AtomicBoolean endSignal;
        private final AtomicBoolean failHalt;
        private final AtomicReference<Throwable> readerError;
        private final TestCoordinator testCoordinator;

        RunnableReader( TestCoordinator testCoordinator, CountDownLatch readerReadySignal,
                CountDownLatch readerStartSignal, AtomicBoolean endSignal,
                AtomicBoolean failHalt, AtomicReference<Throwable> readerError )
        {
            this.readerReadySignal = readerReadySignal;
            this.readerStartSignal = readerStartSignal;
            this.endSignal = endSignal;
            this.failHalt = failHalt;
            this.readerError = readerError;
            this.testCoordinator = testCoordinator;
        }

        @Override
        public void run()
        {
            try
            {
                readerReadySignal.countDown(); // Ready, set...
                readerStartSignal.await(); // GO!

                while ( !endSignal.get() && !failHalt.get() )
                {
                    doRead();
                }
            }
            catch ( Throwable e )
            {
                readerError.set( e );
                failHalt.set( true );
            }
        }

        private void doRead() throws IOException
        {
            ReaderInstruction readerInstruction = testCoordinator.get();
            Iterator<Long> expectToSee = readerInstruction.expectToSee().iterator();
            long start = readerInstruction.start();
            long end = readerInstruction.end();
            boolean forward = start <= end;
            try ( RawCursor<Hit<KEY,VALUE>,IOException> cursor = index.seek( key( start ), key( end ) ) )
            {
                if ( expectToSee.hasNext() )
                {
                    long nextToSee = expectToSee.next();
                    while ( cursor.next() )
                    {
                        // Actual
                        long lastSeenKey = keySeed( cursor.get().key() );
                        long lastSeenValue = valueSeed( cursor.get().value() );

                        if ( lastSeenKey != lastSeenValue )
                        {
                            fail( String.format( "Read mismatching key value pair, key=%d, value=%d%n",
                                    lastSeenKey, lastSeenValue ) );
                        }

                        while ( (forward && lastSeenKey > nextToSee) ||
                                (!forward && lastSeenKey < nextToSee) )
                        {
                            if ( testCoordinator.isReallyExpected( nextToSee ) )
                            {
                                fail( String.format( "Expected to see %d but went straight to %d. ",
                                        nextToSee, lastSeenKey ) );
                            }
                            if ( expectToSee.hasNext() )
                            {
                                nextToSee = expectToSee.next();
                            }
                            else
                            {
                                break;
                            }
                        }
                        if ( nextToSee == lastSeenKey )
                        {
                            if ( expectToSee.hasNext() )
                            {
                                nextToSee = expectToSee.next();
                            }
                            else
                            {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    private Runnable checkpointThread( AtomicBoolean endSignal, AtomicReference<Throwable> readerError,
            AtomicBoolean failHalt )
    {
        return () ->
        {
            while ( !endSignal.get() )
            {
                try
                {
                    index.checkpoint( IOLimiter.unlimited() );
                    // Sleep a little in between checkpoints
                    MILLISECONDS.sleep( 20L );
                }
                catch ( Throwable e )
                {
                    readerError.set( e );
                    failHalt.set( true );
                }
            }
        };
    }

    private static class ReaderInstruction
    {
        private final long startRange;
        private final long endRange;
        private final TreeSet<Long> expectToSee;

        ReaderInstruction( long startRange, long endRange, TreeSet<Long> expectToSee )
        {
            this.startRange = startRange;
            this.endRange = endRange;
            this.expectToSee = expectToSee;
        }

        long start()
        {
            return startRange;
        }

        long end()
        {
            return endRange;
        }

        TreeSet<Long> expectToSee()
        {
            return expectToSee;
        }
    }

    private KEY key( long seed )
    {
        return layout.key( seed );
    }

    private VALUE value( long seed )
    {
        return layout.value( seed );
    }

    private long keySeed( KEY key )
    {
        return layout.keySeed( key );
    }

    private long valueSeed( VALUE value )
    {
        return layout.valueSeed( value );
    }
}
