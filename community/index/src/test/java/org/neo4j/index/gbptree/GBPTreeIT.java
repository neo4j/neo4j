/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.index.gbptree;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.Hit;
import org.neo4j.index.Index;
import org.neo4j.index.IndexWriter;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.test.rule.RandomRule;

import static java.lang.Integer.max;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.index.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

public class GBPTreeIT
{
    private final TemporaryFolder folder = new TemporaryFolder( new File( "target" ) );
    private final RandomRule random = new RandomRule();
    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( random ).around( folder );

    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private GBPTree<MutableLong,MutableLong> index;

    private GBPTree<MutableLong,MutableLong> createIndex( int pageSize )
            throws IOException
    {
        return createIndex( pageSize, NO_MONITOR );
    }

    private GBPTree<MutableLong,MutableLong> createIndex( int pageSize, GBPTree.Monitor monitor )
            throws IOException
    {
        PageCache pageCache = new MuninnPageCache( swapperFactory(), 10_000, pageSize, NULL );
        File indexFile = new File( folder.getRoot(), "index" );
        return index = new GBPTree<>( pageCache, indexFile, layout, 0/*use whatever page cache says*/, monitor );
    }

    private static PageSwapperFactory swapperFactory()
    {
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        swapperFactory.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        return swapperFactory;
    }

    @After
    public void consistencyCheck() throws IOException
    {
        index.consistencyCheck();
    }

    @Test
    public void shouldStayCorrectAfterRandomModifications() throws Exception
    {
        // GIVEN
        Index<MutableLong,MutableLong> index = createIndex( 1024 );
        Comparator<MutableLong> keyComparator = layout;
        Map<MutableLong,MutableLong> data = new TreeMap<>( keyComparator );
        int count = 1000;
        for ( int i = 0; i < count; i++ )
        {
            data.put( randomKey( random.random() ), randomKey( random.random() ) );
        }

        // WHEN
        try ( IndexWriter<MutableLong,MutableLong> writer = index.writer() )
        {
            for ( Map.Entry<MutableLong,MutableLong> entry : data.entrySet() )
            {
                writer.put( entry.getKey(), entry.getValue() );
            }
        }

        for ( int round = 0; round < 10; round++ )
        {
            // THEN
            for ( int i = 0; i < count; i++ )
            {
                MutableLong first = randomKey( random.random() );
                MutableLong second = randomKey( random.random() );
                MutableLong from, to;
                if ( first.longValue() < second.longValue() )
                {
                    from = first;
                    to = second;
                }
                else
                {
                    from = second;
                    to = first;
                }
                Map<MutableLong,MutableLong> expectedHits = expectedHits( data, from, to, keyComparator );
                try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> result = index.seek( from, to ) )
                {
                    while ( result.next() )
                    {
                        MutableLong key = result.get().key();
                        if ( expectedHits.remove( key ) == null )
                        {
                            fail( "Unexpected hit " + key + " when searching for " + from + " - " + to );
                        }

                        assertTrue( keyComparator.compare( key, from ) >= 0 );
                        assertTrue( keyComparator.compare( key, to ) < 0 );
                    }
                    if ( !expectedHits.isEmpty() )
                    {
                        fail( "There were results which were expected to be returned, but weren't:" + expectedHits +
                                " when searching range " + from + " - " + to );
                    }
                }
            }

            randomlyModifyIndex( index, data, random.random() );
        }
    }

    @Test
    public void shouldReadCorrectlyWhenConcurrentlyInsertingInOrder() throws Throwable
    {
        // GIVEN
        int minCheckpointInterval = 10;
        int maxCheckpointInterval = 20;
        index = createIndex( 256 );
        int readers = max( 1, Runtime.getRuntime().availableProcessors() - 1 );
        CountDownLatch readerReadySignal = new CountDownLatch( readers );
        CountDownLatch startSignal = new CountDownLatch( 1 );
        AtomicBoolean endSignal = new AtomicBoolean();
        AtomicInteger highestId = new AtomicInteger( -1 );
        AtomicReference<Throwable> readerError = new AtomicReference<>();
        AtomicInteger numberOfReads = new AtomicInteger();
        Runnable reader = () -> {
            int numberOfLocalReads = 0;
            try
            {
                readerReadySignal.countDown();
                startSignal.await( 10, SECONDS );

                while ( !endSignal.get() )
                {
                    long upToId = highestId.get();
                    if ( upToId < 10 )
                    {
                        continue;
                    }

                    // Read one go, we should see up to highId
                    long start = Long.max( 0, upToId - 1000 );
                    long lastSeen = start - 1;
                    long startTime;
                    long startTimeLeaf;
                    long endTime;
                    startTime = System.currentTimeMillis();
                    try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                                  // "to" is exclusive so do +1 on that
                                  index.seek( new MutableLong( start ), new MutableLong( upToId + 1 ) ) )
                    {
                        startTimeLeaf = System.currentTimeMillis();
                        while ( cursor.next() )
                        {
                            MutableLong hit = cursor.get().key();
                            long value = cursor.get().value().longValue();
                            if ( hit.longValue() != value )
                            {
                                fail( String.format( "Read mismatching key value pair, key=%d, value=%d%n",
                                        hit.longValue(), value ) );
                            }

                            if ( hit.longValue() != lastSeen + 1 )
                            {
                                fail( "Expected to see " + (lastSeen + 1) + " as next hit, but was " + hit +
                                        " where start was " + start );

                            }
                            assertEquals( lastSeen + 1, hit.longValue() );
                            lastSeen = hit.longValue();
                        }
                        endTime = System.currentTimeMillis();
                    }
                    // It's possible that the writer has gone further since we started,
                    // but we should at least have seen upToId
                    if ( lastSeen < upToId )
                    {
                        fail( "Seeked " + start + " - " + upToId + " (inclusive), but only saw " + lastSeen +
                                ". Read took " + (endTime - startTime) + "ms," +
                                " of which " + (endTime - startTimeLeaf) + "ms among leaves. " +
                                "MaxCheckpointInterval=" + maxCheckpointInterval );
                    }

                    // Keep a local counter and update the global one now and then, we don't want
                    // out little statistic here to affect concurrency
                    if ( ++numberOfLocalReads == 30 )
                    {
                        numberOfReads.addAndGet( numberOfLocalReads );
                        numberOfLocalReads = 0;
                    }
                }
            }
            catch ( Throwable e )
            {
                readerError.set( e );
            }
            finally
            {
                numberOfReads.addAndGet( numberOfLocalReads );
            }
        };

        // WHEN starting the readers
        Thread[] readerThreads = new Thread[readers];
        for ( int i = 0; i < readers; i++ )
        {
            readerThreads[i] = new Thread( reader );
            readerThreads[i].start();
        }

        // and starting the checkpointer
        Thread checkpointer = checkpointerThread( minCheckpointInterval, maxCheckpointInterval, endSignal );
        checkpointer.start();

        // and then starting the writer
        try
        {
            assertTrue( readerReadySignal.await( 10, SECONDS ) );
            startSignal.countDown();
            Random random1 = ThreadLocalRandom.current();
            int inserted = 0;
            while ( (inserted < 100_000 || numberOfReads.get() < 100) && readerError.get() == null )
            {
                try ( IndexWriter<MutableLong,MutableLong> writer = index.writer() )
                {
                    int groupCount = random1.nextInt( 1000 ) + 1;
                    for ( int i = 0; i < groupCount; i++, inserted++ )
                    {
                        MutableLong thing = new MutableLong( inserted );
                        writer.put( thing, thing );
                        highestId.set( inserted );
                    }
                }
                // Sleep a little in between update groups (transactions, sort of)
                MILLISECONDS.sleep( random1.nextInt( 10 ) + 3 );
            }
        }
        finally
        {
            // THEN no reader should have failed and by this time there have been a certain
            // number of successful reads. A successful read means that all results were ordered,
            // no holes and we saw all values that was inserted at the point of making the seek call.
            endSignal.set( true );
            for ( Thread readerThread : readerThreads )
            {
                readerThread.join( SECONDS.toMillis( 10 ) );
            }
            if ( readerError.get() != null )
            {
                throw readerError.get();
            }
            checkpointer.join();
        }
    }

    @Test
    public void shouldReadCorrectlyWhenConcurrentlyInsertingOutOfOrder() throws Throwable
    {
        // Checkpoint config
        int minCheckpointInterval = 10;
        int maxCheckpointInterval = 20;

        // Write group config
        int nbrOfGroups = 10;
        int wantedRangeWidth = 1_000;
        int rangeWidth = wantedRangeWidth - wantedRangeWidth % nbrOfGroups;
        int wantedNbrOfReads = 10_000;

        // Readers config
        int readers = max( 1, Runtime.getRuntime().availableProcessors() - 1 );

        // Thread communication
        AtomicInteger currentWriteIteration = new AtomicInteger( 0 );
        AtomicLong lastWrittenKey = new AtomicLong( -1 );
        CountDownLatch readerReadySignal = new CountDownLatch( readers );
        CountDownLatch startSignal = new CountDownLatch( 1 );
        AtomicBoolean endSignal = new AtomicBoolean();
        AtomicBoolean failHalt = new AtomicBoolean(); // Readers signal to writer that there is a failure
        AtomicReference<Throwable> readerError = new AtomicReference<>();
        AtomicInteger numberOfReads = new AtomicInteger();

        // given
        index = createIndex( 256 );

        Runnable reader = () -> {
            int numberOfLocalReads = 0;
            try
            {
                readerReadySignal.countDown();
                while ( currentWriteIteration.get() < 1 )
                {
                    startSignal.await( 5, SECONDS );
                }

                while ( !endSignal.get() && !failHalt.get() )
                {
                    // Read one go, we should see up to highId

                    // Kept for SeekCursor life
                    int iterationExpectedToSee = currentWriteIteration.get() - 1;
                    int rangeModulus = iterationExpectedToSee % nbrOfGroups;
                    long start = minRange( nbrOfGroups, rangeWidth, iterationExpectedToSee );
                    long end = maxRange( nbrOfGroups, rangeWidth, iterationExpectedToSee );

                    // Updated during SeekCursor life
                    long lastSeenKey = -1;
                    long nextToSeeBase = start;
                    long nextToSeeDelta = 0;
                    long nextToSee = nextToSeeBase + nextToSeeDelta;
                    long lastWrittenBeforeStart;
                    long lastWrittenWhenFinished;
                    long lastWrittenBeforeTraversingTree;

                    lastWrittenBeforeTraversingTree = lastWrittenKey.get();
                    try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                                  index.seek( new MutableLong( start ), new MutableLong( end ) ) )
                    {
                        lastWrittenWhenFinished = lastWrittenKey.get();
                        while ( cursor.next() )
                        {
                            lastWrittenBeforeStart = lastWrittenWhenFinished;
                            lastWrittenWhenFinished = lastWrittenKey.get();

                            lastSeenKey = cursor.get().key().longValue();
                            long lastSeenValue = cursor.get().value().longValue();
                            if ( lastSeenKey != lastSeenValue )
                            {
                                failHalt.set( true );
                                fail( String.format( "Read mismatching key value pair, key=%d, value=%d%n",
                                        lastSeenKey, lastSeenValue ) );
                            }

                            nextToSee = nextToSeeBase + nextToSeeDelta;
                            if ( nextToSee == lastSeenKey )
                            {
                                if ( nextToSeeDelta < rangeModulus )
                                {
                                    nextToSeeDelta++;
                                }
                                else
                                {
                                    nextToSeeDelta = 0;
                                    nextToSeeBase += nbrOfGroups;
                                }
                            }
                            else if ( lastSeenKey > nextToSee )
                            {
                                failHalt.set( true );
                                fail( String.format( "Expected to see %d+%d=%d but went straight to %d, " +
                                                "lastWrittenBeforeTraversingTree=%d, " +
                                                "lastWrittenBeforeNext=%d, " +
                                                "lastWrittenAfterNext=%d%n",
                                        nextToSeeBase, nextToSeeDelta, nextToSee, lastSeenKey,
                                        lastWrittenBeforeTraversingTree,
                                        lastWrittenBeforeStart,
                                        lastWrittenWhenFinished ) );
                            }
                        }
                        long difference = Math.abs( end - nextToSee );
                        boolean condition = difference <= nbrOfGroups;
                        if ( !condition )
                        {
                            failHalt.set( true );
                            fail( String.format( "Expected distance between end and nextToSee to be less " +
                                            "than %d but was %d. lastSeenKey=%d, nextToSee=%d, end=%d%n",
                                    nbrOfGroups, difference, lastSeenKey, nextToSee, end ) );
                        }
                    }

                    // Keep a local counter and update the global one now and then, we don't want
                    // out little statistic here to affect concurrency
                    if ( ++numberOfLocalReads == 30 )
                    {
                        numberOfReads.addAndGet( numberOfLocalReads );
                        numberOfLocalReads = 0;
                    }
                }
            }
            catch ( Throwable e )
            {
                readerError.set( e );
            }
            finally
            {
                numberOfReads.addAndGet( numberOfLocalReads );
            }
        };

        // WHEN starting the readers
        Thread[] readerThreads = new Thread[readers];
        for ( int i = 0; i < readers; i++ )
        {
            readerThreads[i] = new Thread( reader );
            readerThreads[i].start();
        }

        // and starting the checkpointer
        Thread checkpointer = checkpointerThread( minCheckpointInterval, maxCheckpointInterval, endSignal );
        checkpointer.start();

        // and then starting the writer
        try
        {
            assertTrue( readerReadySignal.await( 10, SECONDS ) );
            startSignal.countDown();
            int iteration = currentWriteIteration.get();
            while ( !failHalt.get() && numberOfReads.get() < wantedNbrOfReads && readerError.get() == null )
            {
                try ( IndexWriter<MutableLong,MutableLong> writer = index.writer() )
                {
                    for ( long i = minRange( nbrOfGroups, rangeWidth, iteration ) + iteration % nbrOfGroups;
                          i < maxRange( nbrOfGroups, rangeWidth, iteration ); i += nbrOfGroups )
                    {
                        MutableLong thing = new MutableLong( i );
                        writer.put( thing, thing );
                        lastWrittenKey.set( i );
                        if ( failHalt.get() )
                        {
                            break;
                        }
                    }
                }
                iteration = currentWriteIteration.incrementAndGet();
                // Sleep a little in between update groups (transactions, sort of)
                MILLISECONDS.sleep( random.nextInt( 10 ) + 3 );
            }
        }
        finally
        {
            // THEN no reader should have failed and by this time there have been a certain
            // number of successful reads. A successful read means that all results were ordered,
            // no holes and we saw all values that was inserted at the point of making the seek call.
            endSignal.set( true );
            for ( Thread readerThread : readerThreads )
            {
                readerThread.join( SECONDS.toMillis( 10 ) );
            }
            checkpointer.join();
            if ( readerError.get() != null )
            {
                throw readerError.get();
            }
        }
    }

    @Test
    public void shouldReadCorrectlyWhenConcurrentlyRemovingOutOfOrder() throws Throwable
    {
        // Checkpoint config
        int minCheckpointInterval = 10;
        int maxCheckpointInterval = 20;

        // Write group config
        int nbrOfGroups = 10;
        int wantedRangeWidth = 1_000;
        int rangeWidth = wantedRangeWidth - wantedRangeWidth % nbrOfGroups;
        long minValue = 0L;
        long maxValue = 100_000L;

        // Readers config
        int readers = max( 1, Runtime.getRuntime().availableProcessors() - 1 );

        // Thread communication
        AtomicInteger currentWriteIteration = new AtomicInteger( 0 );
        AtomicLong lastRemovedKey = new AtomicLong( -1 );
        CountDownLatch readerReadySignal = new CountDownLatch( readers );
        CountDownLatch startSignal = new CountDownLatch( 1 );
        AtomicBoolean endSignal = new AtomicBoolean();
        AtomicBoolean failHalt = new AtomicBoolean(); // Readers signal to writer that there is a failure
        AtomicReference<Throwable> readerError = new AtomicReference<>();
        AtomicInteger numberOfReads = new AtomicInteger();

        // given
        index = createIndex( 256 );
        insertEverythingInRange( index, minValue, maxValue );

        Runnable reader = () -> {
            int numberOfLocalReads = 0;
            try
            {
                readerReadySignal.countDown();
                while ( currentWriteIteration.get() < 1 )
                {
                    startSignal.await( 5, SECONDS );
                }

                while ( !endSignal.get() && !failHalt.get() )
                {
                    // Read one go, we should see up to highId

                    // Kept for SeekCursor life
                    int iterationExpectedToSee = currentWriteIteration.get();
                    long start = minRange( nbrOfGroups, rangeWidth, iterationExpectedToSee );
                    long end = maxRange( nbrOfGroups, rangeWidth, iterationExpectedToSee );

                    // Updated during SeekCursor life
                    long nextToSee = start + 1; // First odd key in range

                    try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                                  index.seek( new MutableLong( start ), new MutableLong( end ) ) )
                    {
                        while ( cursor.next() )
                        {
                            long thisKey = cursor.get().key().longValue();

                            // Verify value
                            long thisValue = cursor.get().value().longValue();
                            if ( thisKey != thisValue )
                            {
                                failHalt.set( true );
                                fail( String.format( "Read mismatching key value pair, key=%d, value=%d%n",
                                        thisKey, thisValue ) );
                            }

                            if ( nextToSee == thisKey )
                            {
                                nextToSee += 2; // Next odd key in range
                            }
                            else if ( thisKey > nextToSee )
                            {
                                failHalt.set( true );
                                fail( String.format( "Expected to see %d but went straight to %d%n",
                                        nextToSee, thisKey) );
                            }
                        }
                    }

                    // Keep a local counter and update the global one now and then, we don't want
                    // out little statistic here to affect concurrency
                    if ( ++numberOfLocalReads == 30 )
                    {
                        numberOfReads.addAndGet( numberOfLocalReads );
                        numberOfLocalReads = 0;
                    }
                }
            }
            catch ( Throwable e )
            {
                readerError.set( e );
            }
            finally
            {
                numberOfReads.addAndGet( numberOfLocalReads );
            }
        };

        // WHEN starting the readers
        Thread[] readerThreads = new Thread[readers];
        for ( int i = 0; i < readers; i++ )
        {
            readerThreads[i] = new Thread( reader );
            readerThreads[i].start();
        }

        // and starting the checkpointer
        Thread checkpointer = checkpointerThread( minCheckpointInterval, maxCheckpointInterval, endSignal );
        checkpointer.start();

        // and then starting the writer
        try
        {
            assertTrue( readerReadySignal.await( 10, SECONDS ) );
            startSignal.countDown();
            int iteration = currentWriteIteration.get();
            while ( !failHalt.get() && readerError.get() == null && lastRemovedKey.get() < maxValue - 2)
            {
                try ( IndexWriter<MutableLong,MutableLong> writer = index.writer() )
                {
                    int minRange = minRange( nbrOfGroups, rangeWidth, iteration );
                    int maxRange = maxRange( nbrOfGroups, rangeWidth, iteration );
                    for ( long i = minRange + (iteration % nbrOfGroups); i < maxRange; i += nbrOfGroups )
                    {
                        MutableLong thing = new MutableLong( i );
                        writer.remove( thing );
                        lastRemovedKey.set( i );
                        if ( failHalt.get() )
                        {
                            break;
                        }
                    }
                }
                iteration = currentWriteIteration.addAndGet( 2 );
                // Sleep a little in between update groups (transactions, sort of)
                MILLISECONDS.sleep( random.nextInt( 10 ) + 3 );
            }
        }
        finally
        {
            // THEN no reader should have failed and by this time there have been a certain
            // number of successful reads. A successful read means that all results were ordered,
            // no holes and we saw all values that was inserted at the point of making the seek call.
            endSignal.set( true );
            for ( Thread readerThread : readerThreads )
            {
                readerThread.join( SECONDS.toMillis( 10 ) );
            }
            checkpointer.join();
            if ( readerError.get() != null )
            {
                throw readerError.get();
            }
        }
    }

    private void insertEverythingInRange( GBPTree<MutableLong,MutableLong> index, long minValue, long maxValue )
            throws IOException
    {
        long nextToInsert = minValue;
        MutableLong key = new MutableLong();
        MutableLong value = new MutableLong();

        try ( IndexWriter<MutableLong,MutableLong> writer = index.writer() )
        {
            while ( nextToInsert < maxValue )
            {
                key.setValue( nextToInsert );
                value.setValue( nextToInsert );
                writer.put( key, value );
                nextToInsert++;
            }
        }
    }

    private Thread checkpointerThread( int minCheckpointInterval, int maxCheckpointInterval, AtomicBoolean endSignal )
    {
        return new Thread( () ->
        {
            while ( !endSignal.get() )
            {
                try
                {
                    index.checkpoint( IOLimiter.unlimited() );
                    // Sleep a little in between update groups (transactions, sort of)
                    MILLISECONDS.sleep( random.nextInt( minCheckpointInterval, maxCheckpointInterval ) );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }
        });
    }

    private int maxRange( int nbrOfGroups, int rangeWidth, int iteration )
    {
        return (iteration / nbrOfGroups + 1) * rangeWidth;
    }

    private int minRange( int nbrOfGroups, int rangeWidth, int iteration )
    {
        return iteration / nbrOfGroups * rangeWidth;
    }

    private static void randomlyModifyIndex( Index<MutableLong,MutableLong> index,
            Map<MutableLong,MutableLong> data, Random random ) throws IOException
    {
        int changeCount = random.nextInt( 10 ) + 10;
        try ( IndexWriter<MutableLong,MutableLong> writer = index.writer() )
        {
            for ( int i = 0; i < changeCount; i++ )
            {
                if ( random.nextBoolean() && data.size() > 0 )
                {   // remove
                    MutableLong key = randomKey( data, random );
                    MutableLong value = data.remove( key );
                    MutableLong removedValue = writer.remove( key );
                    assertEquals( "For " + key, value, removedValue );
                }
                else
                {   // put
                    MutableLong key = randomKey( random );
                    MutableLong value = randomKey( random );
                    writer.put( key, value );
                    data.put( key, value );
                }
            }
        }
    }

    private static Map<MutableLong,MutableLong> expectedHits( Map<MutableLong,MutableLong> data,
            MutableLong from, MutableLong to, Comparator<MutableLong> comparator )
    {
        Map<MutableLong,MutableLong> hits = new TreeMap<>( comparator );
        for ( Map.Entry<MutableLong,MutableLong> candidate : data.entrySet() )
        {
            if ( comparator.compare( candidate.getKey(), from ) >= 0 &&
                    comparator.compare( candidate.getKey(), to ) < 0 )
            {
                hits.put( candidate.getKey(), candidate.getValue() );
            }
        }
        return hits;
    }

    private static MutableLong randomKey( Map<MutableLong,MutableLong> data, Random random )
    {
        MutableLong[] keys = data.keySet().toArray( new MutableLong[data.size()] );
        return keys[random.nextInt( keys.length )];
    }

    private static MutableLong randomKey( Random random )
    {
        return new MutableLong( random.nextInt( 1_000 ) );
    }
}
