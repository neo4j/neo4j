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
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.Hit;
import org.neo4j.index.Index;
import org.neo4j.index.IndexWriter;
import org.neo4j.index.gbptree.GBPTree.Monitor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.test.Barrier;
import org.neo4j.test.rule.RandomRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static java.lang.Integer.max;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.index.IndexWriter.Options.DEFAULTS;
import static org.neo4j.index.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.gbptree.ThrowingRunnable.throwing;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

public class GBPTreeTest
{
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder( new File( "target" ) );
    @Rule
    public final RandomRule random = new RandomRule();
    private PageCache pageCache;
    private File indexFile;
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private GBPTree<MutableLong,MutableLong> index;

    public GBPTree<MutableLong,MutableLong> createIndex( int pageSize )
            throws IOException
    {
        return createIndex( pageSize, NO_MONITOR );
    }

    private GBPTree<MutableLong,MutableLong> createIndex( int pageSize, Monitor monitor )
            throws IOException
    {
        pageCache = new MuninnPageCache( swapperFactory(), 10_000, pageSize, NULL );
        indexFile = new File( folder.getRoot(), "index" );
        return index = new GBPTree<>( pageCache, indexFile, layout, 0/*use whatever page cache says*/, monitor );
    }

    private static PageSwapperFactory swapperFactory()
    {
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        swapperFactory.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        return swapperFactory;
    }

    @After
    public void closePageCache() throws IOException
    {
        if ( index != null )
        {
            assertTrue( index.consistencyCheck() );
            index.close();
        }
        pageCache.close();
    }

    /* Meta and state page tests */

    @Test
    public void shouldReadWrittenMetaData() throws Exception
    {
        // GIVEN
        try ( Index<MutableLong,MutableLong> index = createIndex( 1024 ) )
        {   // Open/close is enough
        }

        // WHEN
        index = new GBPTree<>( pageCache, indexFile, layout, 0, NO_MONITOR );

        // THEN being able to open validates that the same meta data was read
        // the test also closes the index afterwards
    }

    @Test
    public void shouldFailToOpenOnDifferentMetaData() throws Exception
    {
        // GIVEN
        try ( Index<MutableLong,MutableLong> index = createIndex( 1024 ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        try ( Index<MutableLong,MutableLong> index =
                new GBPTree<>( pageCache, indexFile, new SimpleLongLayout( "Something else" ), 0, NO_MONITOR ) )
        {
            fail( "Should not load" );
        }
        catch ( MetadataMismatchException e )
        {
            // THEN good
        }

        // THEN being able to open validates that the same meta data was read
        // the test also closes the index afterwards
    }

    @Test
    public void shouldFailToOpenOnDifferentLayout() throws Exception
    {
        // GIVEN
        try ( Index<MutableLong,MutableLong> index = createIndex( 1024 ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        try ( Index<MutableLong,MutableLong> index =
                new GBPTree<>( pageCache, indexFile, new SimpleLongLayout()
        {
            @Override
            public long identifier()
            {
                return 123456;
            }
        }, 0, NO_MONITOR ) )
        {

            fail( "Should not load" );
        }
        catch ( MetadataMismatchException e )
        {
            // THEN good
        }
    }

    @Test
    public void shouldFailToOpenOnDifferentMajorVersion() throws Exception
    {
        // GIVEN
        try ( Index<MutableLong,MutableLong> index = createIndex( 1024 ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        try ( Index<MutableLong,MutableLong> index =
            new GBPTree<>( pageCache, indexFile, new SimpleLongLayout()
            {
                @Override
                public int majorVersion()
                {
                    return super.majorVersion() + 1;
                }
            }, 0, NO_MONITOR ) )
        {
            fail( "Should not load" );
        }
        catch ( MetadataMismatchException e )
        {
            // THEN good
        }
    }

    @Test
    public void shouldFailToOpenOnDifferentMinorVersion() throws Exception
    {
        // GIVEN
        try ( Index<MutableLong,MutableLong> index = createIndex( 1024 ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        try ( Index<MutableLong,MutableLong> index =
            new GBPTree<>( pageCache, indexFile, new SimpleLongLayout()
            {
                @Override
                public int minorVersion()
                {
                    return super.minorVersion() + 1;
                }
            }, 0, NO_MONITOR ) )
        {
            fail( "Should not load" );
        }
        catch ( MetadataMismatchException e )
        {
            // THEN good
        }
    }

    @Test
    public void shouldFailOnOpenWithDifferentPageSize() throws Exception
    {
        // GIVEN
        int pageSize = 1024;
        try ( Index<MutableLong,MutableLong> index = createIndex( pageSize ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        pageCache.close();
        pageCache = new MuninnPageCache( swapperFactory(), 10_000, pageSize / 2, NULL );
        try ( Index<MutableLong,MutableLong> index = new GBPTree<>( pageCache, indexFile, layout, 0, NO_MONITOR ) )
        {
            fail( "Should not load" );
        }
        catch ( MetadataMismatchException e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "page size" ) );
        }
    }

    @Test
    public void shouldFailOnPageSizeLargerThanThatOfPageCache() throws Exception
    {
        // WHEN
        int pageSize = 512;
        pageCache = new MuninnPageCache( swapperFactory(), 10_000, pageSize, NULL );
        indexFile = new File( folder.getRoot(), "index" );
        try ( Index<MutableLong,MutableLong> index =
                new GBPTree<>( pageCache, indexFile, layout, pageSize * 2, NO_MONITOR ) )
        {
            fail( "Shouldn't have been created" );
        }
        catch ( MetadataMismatchException e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "page size" ) );
        }
    }

    @Test
    public void shouldReturnNoResultsOnEmptyIndex() throws Exception
    {
        // GIVEN
        index = createIndex( 256 );

        // WHEN
        RawCursor<Hit<MutableLong,MutableLong>,IOException> result =
                index.seek( new MutableLong( 0 ), new MutableLong( 10 ) );

        // THEN
        assertFalse( result.next() );
    }

    @Test
    public void shouldNotBeAbleToAcquireModifierTwice() throws Exception
    {
        // GIVEN
        index = createIndex( 256 );
        IndexWriter<MutableLong,MutableLong> writer = index.writer( DEFAULTS );

        // WHEN
        try
        {
            index.writer( DEFAULTS );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // THEN good
        }

        writer.close();
    }

    /* Check-pointing tests */

    @Test
    public void checkPointShouldLockOutWriter() throws Exception
    {
        // GIVEN
        CheckpointControlledMonitor monitor = new CheckpointControlledMonitor();
        index = createIndex( 1024, monitor );
        long key = 10;
        try ( IndexWriter<MutableLong,MutableLong> writer = index.writer( DEFAULTS ) )
        {
            writer.put( new MutableLong( key ), new MutableLong( key ) );
        }

        // WHEN
        monitor.enabled = true;
        Thread checkpointer = new Thread( throwing( () -> index.checkpoint( IOLimiter.unlimited() ) ) );
        checkpointer.start();
        monitor.barrier.awaitUninterruptibly();
        // now we're in the smack middle of a checkpoint
        Thread t2 = new Thread( throwing( () -> index.writer( DEFAULTS ) ) );
        t2.start();
        t2.join( 200 );
        assertTrue( Arrays.toString( checkpointer.getStackTrace() ), t2.isAlive() );
        monitor.barrier.release();

        // THEN
        t2.join();
    }

    @Test
    public void checkPointShouldWaitForWriter() throws Exception
    {
        // GIVEN
        index = createIndex( 1024 );

        // WHEN
        Barrier.Control barrier = new Barrier.Control();
        Thread writerThread = new Thread( throwing( () ->
        {
            try ( IndexWriter<MutableLong,MutableLong> writer = index.writer( DEFAULTS ) )
            {
                writer.put( new MutableLong( 1 ), new MutableLong( 1 ) );
                barrier.reached();
            }
        } ) );
        writerThread.start();
        barrier.awaitUninterruptibly();
        Thread checkpointer = new Thread( throwing( () -> index.checkpoint( IOLimiter.unlimited() ) ) );
        checkpointer.start();
        checkpointer.join( 200 );
        assertTrue( checkpointer.isAlive() );

        // THEN
        barrier.release();
        checkpointer.join();
    }

    /* Insertion and read tests */

    @Test
    public void shouldSeeSimpleInsertions() throws Exception
    {
        index = createIndex( 256 );
        int count = 1000;
        try ( IndexWriter<MutableLong,MutableLong> writer = index.writer( DEFAULTS ) )
        {
            for ( int i = 0; i < count; i++ )
            {
                writer.put( new MutableLong( i ), new MutableLong( i ) );
            }
        }

        try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                index.seek( new MutableLong( 0 ), new MutableLong( Long.MAX_VALUE ) ) )
        {
            for ( int i = 0; i < count; i++ )
            {
                assertTrue( cursor.next() );
                assertEquals( i, cursor.get().key().longValue() );
            }
            assertFalse( cursor.next() );
        }
    }

    /* Randomized tests */

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
        try ( IndexWriter<MutableLong,MutableLong> writer = index.writer( DEFAULTS ) )
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
    public void shouldSplitCorrectly() throws Exception
    {
        // GIVEN
        Index<MutableLong,MutableLong> index = createIndex( 256 );

        // WHEN
        int count = 1_000;
        PrimitiveLongSet seen = Primitive.longSet( count );
        try ( IndexWriter<MutableLong,MutableLong> writer = index.writer( DEFAULTS ) )
        {
            for ( int i = 0; i < count; i++ )
            {
                MutableLong key;
                do
                {
                    key = new MutableLong( random.nextInt( 100_000 ) );
                }
                while ( !seen.add( key.longValue() ) );
                MutableLong value = new MutableLong( i );
                writer.put( key, value );
                seen.add( key.longValue() );
            }
        }

        // THEN
        try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                      index.seek( new MutableLong( 0 ), new MutableLong( Long.MAX_VALUE ) ) )
        {
            long prev = -1;
            while ( cursor.next() )
            {
                MutableLong hit = cursor.get().key();
                if ( hit.longValue() < prev )
                {
                    fail( hit + " smaller than prev " + prev );
                }
                prev = hit.longValue();
                assertTrue( seen.remove( hit.longValue() ) );
            }

            if ( !seen.isEmpty() )
            {
                fail( "expected hits " + Arrays.toString( PrimitiveLongCollections.asArray( seen.iterator() ) ) );
            }
        }
    }

    @Test
    public void shouldReadCorrectlyWhenConcurrentlyInserting() throws Throwable
    {
        // GIVEN
        int maxCheckpointInterval = random.intBetween( 50, 400 );
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
                    try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                            // "to" is exclusive so do +1 on that
                            index.seek( new MutableLong( start ), new MutableLong( upToId + 1 ) ) )
                    {
                        while ( cursor.next() )
                        {
                            MutableLong hit = cursor.get().key();
                            if ( hit.longValue() != lastSeen + 1 )
                            {
                                fail( "Expected to see " + (lastSeen + 1) + " as next hit, but was " + hit +
                                        " where start was " + start );

                            }
                            assertEquals( lastSeen + 1, hit.longValue() );
                            lastSeen = hit.longValue();
                        }
                    }
                    // It's possible that the writer has gone further since we started,
                    // but we should at least have seen upToId
                    if ( lastSeen < upToId )
                    {
                        fail( "Seeked " + start + " - " + upToId + " (inclusive), but only saw " + lastSeen );
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
        Thread checkpointer = new Thread( () ->
        {
            while ( !endSignal.get() )
            {
                try
                {
                    index.checkpoint( IOLimiter.unlimited() );
                    // Sleep a little in between update groups (transactions, sort of)
                    MILLISECONDS.sleep( random.nextInt( maxCheckpointInterval ) );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }
        });
        checkpointer.start();

        // and then starting the writer
        try
        {
            assertTrue( readerReadySignal.await( 10, SECONDS ) );
            startSignal.countDown();
            Random random = ThreadLocalRandom.current();
            int inserted = 0;
            while ( (inserted < 100_000 || numberOfReads.get() < 100) && readerError.get() == null )
            {
                try ( IndexWriter<MutableLong,MutableLong> writer = index.writer( DEFAULTS ) )
                {
                    int groupCount = random.nextInt( 1000 ) + 1;
                    for ( int i = 0; i < groupCount; i++, inserted++ )
                    {
                        MutableLong thing = new MutableLong( inserted );
                        writer.put( thing, thing );
                        highestId.set( inserted );
                    }
                }
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
            if ( readerError.get() != null )
            {
                throw readerError.get();
            }
            checkpointer.join();
        }
    }

    private static void randomlyModifyIndex( Index<MutableLong,MutableLong> index,
            Map<MutableLong,MutableLong> data, Random random ) throws IOException
    {
        int changeCount = random.nextInt( 10 ) + 10;
        try ( IndexWriter<MutableLong,MutableLong> writer = index.writer( DEFAULTS ) )
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

    private static MutableLong randomKey( Map<MutableLong,MutableLong> data, Random random )
    {
        MutableLong[] keys = data.keySet().toArray( new MutableLong[data.size()] );
        return keys[random.nextInt( keys.length )];
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

    private static MutableLong randomKey( Random random )
    {
        return new MutableLong( random.nextInt( 1_000 ) );
    }

    private static class CheckpointControlledMonitor implements Monitor
    {
        private final Barrier.Control barrier = new Barrier.Control();
        private volatile boolean enabled;

        @Override
        public void checkpointCompleted()
        {
            if ( enabled )
            {
                barrier.reached();
            }
        }
    }

}
