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
package org.neo4j.index.bptree;

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
import org.neo4j.graphdb.Direction;
import org.neo4j.index.Hit;
import org.neo4j.index.Index;
import org.neo4j.index.bptree.path.PathIndexLayout;
import org.neo4j.index.bptree.path.SCIndexDescription;
import org.neo4j.index.bptree.path.TwoLongs;
import org.neo4j.index.Modifier;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static java.lang.Integer.max;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.index.Modifier.Options.DEFAULTS;
import static org.neo4j.index.ValueAmenders.overwrite;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

public class BPTreeIndexTest
{
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder( new File( "target" ) );
    @Rule
    public final RandomRule random = new RandomRule();
    private PageCache pageCache;
    private File indexFile;
    private final SCIndexDescription description = new SCIndexDescription( "a", "b", "c", OUTGOING, "d", null );
    private final Layout<TwoLongs,TwoLongs> layout = new PathIndexLayout( description );
    private BPTreeIndex<TwoLongs,TwoLongs> index;

    public BPTreeIndex<TwoLongs,TwoLongs> createIndex( int pageSize )
            throws IOException
    {
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        swapperFactory.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        pageCache = new MuninnPageCache( swapperFactory, 10_000, pageSize, NULL );
        indexFile = new File( folder.getRoot(), "index" );
        return index = new BPTreeIndex<>( pageCache, indexFile, layout, 0/*i.e. use whatever page cache says*/ );
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

    /* Meta data tests */

    @Test
    public void shouldReadWrittenMetaData() throws Exception
    {
        // GIVEN
        try ( Index<TwoLongs,TwoLongs> index = createIndex( 1024 ) )
        {   // Open/close is enough
        }

        // WHEN
        index = new BPTreeIndex<>( pageCache, indexFile, layout, 0 );

        // THEN being able to open validates that the same meta data was read
        // the test also closes the index afterwards
    }

    @Test
    public void shouldFailToOpenOnDifferentMetaData() throws Exception
    {
        // GIVEN
        try ( Index<TwoLongs,TwoLongs> index = createIndex( 1024 ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        SCIndexDescription wrongDescription = new SCIndexDescription( "_", "_", "_", Direction.INCOMING, null, "prop" );
        try ( Index<TwoLongs,TwoLongs> index =
                new BPTreeIndex<>( pageCache, indexFile, new PathIndexLayout( wrongDescription ), 0 ) )
        {
            fail( "Should not load" );
        }
        catch ( IllegalArgumentException e )
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
        try ( Index<TwoLongs,TwoLongs> index = createIndex( 1024 ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        try ( Index<TwoLongs,TwoLongs> index =
                new BPTreeIndex<>( pageCache, indexFile, new PathIndexLayout( description )
        {
            @Override
            public long identifier()
            {
                return 123456;
            }
        }, 0 ) )
        {

            fail( "Should not load" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
        }
    }

    @Test
    public void shouldFailToOpenOnDifferentMajorVersion() throws Exception
    {
        // GIVEN
        try ( Index<TwoLongs,TwoLongs> index = createIndex( 1024 ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        try ( Index<TwoLongs,TwoLongs> index =
            new BPTreeIndex<>( pageCache, indexFile, new PathIndexLayout( description )
            {
                @Override
                public int majorVersion()
                {
                    return super.majorVersion() + 1;
                }
            }, 0 ) )
        {
            fail( "Should not load" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
        }
    }

    @Test
    public void shouldFailToOpenOnDifferentMinorVersion() throws Exception
    {
        // GIVEN
        try ( Index<TwoLongs,TwoLongs> index = createIndex( 1024 ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        try ( Index<TwoLongs,TwoLongs> index =
            new BPTreeIndex<>( pageCache, indexFile, new PathIndexLayout( description )
            {
                @Override
                public int minorVersion()
                {
                    return super.minorVersion() + 1;
                }
            }, 0 ) )
        {
            fail( "Should not load" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
        }
    }

    /* Insertion and read tests */

    @Test
    public void shouldSeeSimpleInsertions() throws Exception
    {
        index = createIndex( 128 );
        int count = 1000;
        try ( Modifier<TwoLongs,TwoLongs> inserter = index.modifier( DEFAULTS ) )
        {
            for ( int i = 0; i < count; i++ )
            {
                inserter.insert( new TwoLongs( i, i ), new TwoLongs( i, i ) );
            }
        }

        try ( RawCursor<Hit<TwoLongs,TwoLongs>,IOException> cursor =
                index.seek( new TwoLongs( 0, 0 ), new TwoLongs( Long.MAX_VALUE, Long.MAX_VALUE ) ) )
        {
            for ( int i = 0; i < count; i++ )
            {
                assertTrue( cursor.next() );
                assertEquals( new TwoLongs( i, i ), cursor.get().key() );
            }
            assertFalse( cursor.next() );
        }
    }

    /* Randomized tests */

    @Test
    public void shouldStayCorrectAfterRandomModifications() throws Exception
    {
        // GIVEN
        Index<TwoLongs,TwoLongs> index = createIndex( 1024 );
        Comparator<TwoLongs> keyComparator = layout;
        Map<TwoLongs,TwoLongs> data = new TreeMap<>( keyComparator );
        int count = 1000;
        for ( int i = 0; i < count; i++ )
        {
            data.put( randomTreeThing( random.random() ), randomTreeThing( random.random() ) );
        }

        // WHEN
        try ( Modifier<TwoLongs,TwoLongs> inserter = index.modifier( DEFAULTS ) )
        {
            for ( Map.Entry<TwoLongs,TwoLongs> entry : data.entrySet() )
            {
                inserter.insert( entry.getKey(), entry.getValue() );
            }
        }

        for ( int round = 0; round < 10; round++ )
        {
            // THEN
            for ( int i = 0; i < count; i++ )
            {
                TwoLongs first = randomTreeThing( random.random() );
                TwoLongs second = randomTreeThing( random.random() );
                TwoLongs from, to;
                if ( first.first < second.first )
                {
                    from = first;
                    to = second;
                }
                else
                {
                    from = second;
                    to = first;
                }
                Map<TwoLongs,TwoLongs> expectedHits = expectedHits( data, from, to, keyComparator );
                try ( RawCursor<Hit<TwoLongs,TwoLongs>,IOException> result = index.seek( from, to ) )
                {
                    while ( result.next() )
                    {
                        TwoLongs key = result.get().key();
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
        BPTreeIndex<TwoLongs,TwoLongs> index = createIndex( 128 );

        // WHEN
        int count = 1_000;
        PrimitiveLongSet seen = Primitive.longSet( count );
        try ( Modifier<TwoLongs,TwoLongs> inserter = index.modifier( DEFAULTS ) )
        {
            for ( int i = 0; i < count; i++ )
            {
                TwoLongs key;
                do
                {
                    key = new TwoLongs( random.nextInt( 100_000 ), 0 );
                }
                while ( !seen.add( key.first ) );
                TwoLongs value = new TwoLongs();
                inserter.insert( key, value, overwrite() );
                seen.add( key.first );
            }
        }

        // THEN
        try ( RawCursor<Hit<TwoLongs,TwoLongs>,IOException> cursor =
                      index.seek( new TwoLongs( 0, 0 ), new TwoLongs( Long.MAX_VALUE, 0 ) ) )
        {
            TwoLongs prev = new TwoLongs( -1, -1 );
            while ( cursor.next() )
            {
                TwoLongs hit = cursor.get().key();
                if ( hit.first < prev.first )
                {
                    fail( hit + " smaller than prev " + prev );
                }
                prev = new TwoLongs( hit.first, hit.other );
                assertTrue( seen.remove( hit.first ) );
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
        index = createIndex( 256 );
        int readers = max( 1, Runtime.getRuntime().availableProcessors() - 1 );
        CountDownLatch readerReadySignal = new CountDownLatch( readers );
        CountDownLatch startSignal = new CountDownLatch( 1 );
        AtomicBoolean endSignal = new AtomicBoolean();
        AtomicInteger highestId = new AtomicInteger( -1 );
        AtomicReference<Throwable> readerError = new AtomicReference<>();
        AtomicInteger numberOfReads = new AtomicInteger();
        Runnable reader = new Runnable()
        {
            @Override
            public void run()
            {
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
                        try ( RawCursor<Hit<TwoLongs,TwoLongs>,IOException> cursor =
                                // "to" is exclusive so do +1 on that
                                index.seek( new TwoLongs( start, 0 ), new TwoLongs( upToId + 1, 0 ) ) )
                        {
                            while ( cursor.next() )
                            {
                                TwoLongs hit = cursor.get().key();
                                if ( hit.first != lastSeen + 1 )
                                {
                                    fail( "Expected to see " + (lastSeen + 1) + " as next hit, but was " + hit.first +
                                            " where start was " + start );

                                }
                                assertEquals( lastSeen + 1, hit.first );
                                lastSeen = hit.first;
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
            }
        };

        // WHEN starting the readers
        Thread[] readerThreads = new Thread[readers];
        for ( int i = 0; i < readers; i++ )
        {
            readerThreads[i] = new Thread( reader );
            readerThreads[i].start();
        }

        // and then starting the modifier
        try
        {
            readerReadySignal.await( 10, SECONDS );
            startSignal.countDown();
            Random random = ThreadLocalRandom.current();
            try ( Modifier<TwoLongs,TwoLongs> inserter = index.modifier( DEFAULTS ) )
            {
                int inserted = 0;
                while ( (inserted < 100_000 || numberOfReads.get() < 100) && readerError.get() == null )
                {
                    int groupCount = random.nextInt( 1000 ) + 1;
                    for ( int i = 0; i < groupCount; i++, inserted++ )
                    {
                        TwoLongs thing = new TwoLongs( inserted, 0 );
                        inserter.insert( thing, thing );
                        highestId.set( inserted );
                    }
                    // Sleep a little in between update groups (transactions, sort of)
                    MILLISECONDS.sleep( random.nextInt( 10 ) + 3 );
                }
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
        }
    }

    private void randomlyModifyIndex( Index<TwoLongs,TwoLongs> index2, Map<TwoLongs,TwoLongs> data, Random random )
            throws IOException
    {
        int changeCount = random.nextInt( 10 ) + 10;
        try ( Modifier<TwoLongs,TwoLongs> modifier = index2.modifier( DEFAULTS ) )
        {
            for ( int i = 0; i < changeCount; i++ )
            {
                // TODO temporarily disabled
//                if ( random.nextBoolean() && data.size() > 0 )
//                {   // remove
//                    TwoLongs key = randomKey( data, random );
//                    TwoLongs value = data.remove( key );
//                    TwoLongs removedValue = modifier.remove( key );
//                    assertEquals( "For " + key, value, removedValue );
//                }
//                else
                {   // insert
                    TwoLongs key = randomTreeThing( random );
                    TwoLongs value = randomTreeThing( random );
                    modifier.insert( key, value, overwrite() );
                    data.put( key, value );
                }
            }
        }
    }

    private TwoLongs randomKey( Map<TwoLongs,TwoLongs> data, Random random )
    {
        TwoLongs[] keys = data.keySet().toArray( new TwoLongs[data.size()] );
        return keys[random.nextInt( keys.length )];
    }

    private Map<TwoLongs,TwoLongs> expectedHits( Map<TwoLongs,TwoLongs> data, TwoLongs from, TwoLongs to,
            Comparator<TwoLongs> comparator )
    {
        Map<TwoLongs,TwoLongs> hits = new TreeMap<>( comparator );
        for ( Map.Entry<TwoLongs,TwoLongs> candidate : data.entrySet() )
        {
            if ( comparator.compare( candidate.getKey(), from ) >= 0 &&
                    comparator.compare( candidate.getKey(), to ) < 0 )
            {
                hits.put( candidate.getKey(), candidate.getValue() );
            }
        }
        return hits;
    }

    private TwoLongs randomTreeThing( Random random )
    {
        return new TwoLongs( random.nextInt( 1_000 ), random.nextInt( 1_000 ) );
    }
}
