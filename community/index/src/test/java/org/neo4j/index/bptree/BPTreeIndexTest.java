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
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.cursor.RawCursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.index.Hit;
import org.neo4j.index.bptree.BPTreeIndex;
import org.neo4j.index.bptree.Layout;
import org.neo4j.index.bptree.path.PathIndexLayout;
import org.neo4j.index.bptree.path.SCIndexDescription;
import org.neo4j.index.bptree.path.TwoLongs;
import org.neo4j.index.Modifier;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static java.lang.System.currentTimeMillis;
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
    private PageCache pageCache;
    private File indexFile;
    private final SCIndexDescription description = new SCIndexDescription( "a", "b", "c", OUTGOING, "d", null );
    @SuppressWarnings( "rawtypes" )
    private BPTreeIndex index;

    @SuppressWarnings( "unchecked" )
    public <KEY,VALUE> BPTreeIndex<KEY,VALUE> createIndex( int pageSize, Layout<KEY,VALUE> layout ) throws IOException
    {
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        swapperFactory.setFileSystemAbstraction( new DefaultFileSystemAbstraction() );
        pageCache = new MuninnPageCache( swapperFactory, 10_000, pageSize, NULL );
        indexFile = new File( folder.getRoot(), "index" );
        return index = new BPTreeIndex<>( pageCache, indexFile, new PathIndexLayout( description ), 0 );
    }

    @After
    public void closePageCache() throws IOException
    {
        if ( index != null )
        {
            index.close();
        }
        pageCache.close();
    }

    @Test
    public void shouldReadWrittenMetaData() throws Exception
    {
        // GIVEN
        Layout<TwoLongs,TwoLongs> layout = new PathIndexLayout( description );
        try ( BPTreeIndex<TwoLongs,TwoLongs> index = createIndex( 1024, layout ) )
        {   // Open/close is enough
        }

        // WHEN
        index = new BPTreeIndex<>( pageCache, indexFile, layout, 0 );

        // THEN being able to open validates that the same meta data was read
        // the test also closes the index afterwards
    }

    @Test
    public void shouldStayCorrectAfterRandomModifications() throws Exception
    {
        // GIVEN
        PathIndexLayout layout = new PathIndexLayout( description );
        BPTreeIndex<TwoLongs,TwoLongs> index = createIndex( 1024, layout );
        Comparator<TwoLongs> keyComparator = layout;
        Map<TwoLongs,TwoLongs> data = new TreeMap<>( keyComparator );
        long seed = currentTimeMillis();
        Random random = new Random( seed );
        int count = 1000;
        for ( int i = 0; i < count; i++ )
        {
            data.put( randomTreeThing( random ), randomTreeThing( random ) );
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
                TwoLongs first = randomTreeThing( random );
                TwoLongs second = randomTreeThing( random );
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

            randomlyModifyIndex( index, data, random );
        }
    }

    @Test
    public void shouldSplitCorrectly() throws Exception
    {
        // GIVEN
        BPTreeIndex<TwoLongs,TwoLongs> index = createIndex( 128, new PathIndexLayout( description ) );

        // WHEN
        long seed = currentTimeMillis();
        Random random = new Random( seed );
        int count = 1_000;
        PrimitiveLongSet seen = Primitive.longSet( count );
        try ( Modifier<TwoLongs,TwoLongs> inserter = index.modifier( DEFAULTS ) )
        {
            for ( int i = 0; i < 1_000; i++ )
            {
                TwoLongs key = new TwoLongs( random.nextInt( 100_000 ), 0 );
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
                    index.printTree();
                    fail( hit + " smaller than prev " + prev );
                }
                prev = new TwoLongs( hit.first, hit.other );
                assertTrue( seen.remove( hit.first ) );
            }
            assertTrue( seen.isEmpty() );
        }
    }

    @Test
    public void shouldFailToOpenOnDifferentMetaData() throws Exception
    {
        // GIVEN
        Layout<TwoLongs,TwoLongs> layout = new PathIndexLayout( description );
        try ( BPTreeIndex<TwoLongs,TwoLongs> index = createIndex( 1024, layout ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        SCIndexDescription wrongDescription = new SCIndexDescription( "_", "_", "_", Direction.INCOMING, null, "prop" );
        try
        {
            new BPTreeIndex<>( pageCache, indexFile, new PathIndexLayout( wrongDescription ), 0 );
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
        Layout<TwoLongs,TwoLongs> layout = new PathIndexLayout( description );
        try ( BPTreeIndex<TwoLongs,TwoLongs> index = createIndex( 1024, layout ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        try
        {
            new BPTreeIndex<>( pageCache, indexFile, new PathIndexLayout( description )
            {
                @Override
                public long identifier()
                {
                    return 123456;
                }
            }, 0 );
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
        Layout<TwoLongs,TwoLongs> layout = new PathIndexLayout( description );
        try ( BPTreeIndex<TwoLongs,TwoLongs> index = createIndex( 1024, layout ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        try
        {
            new BPTreeIndex<>( pageCache, indexFile, new PathIndexLayout( description )
            {
                @Override
                public int majorVersion()
                {
                    return super.majorVersion() + 1;
                }
            }, 0 );
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
        Layout<TwoLongs,TwoLongs> layout = new PathIndexLayout( description );
        try ( BPTreeIndex<TwoLongs,TwoLongs> index = createIndex( 1024, layout ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        try
        {
            new BPTreeIndex<>( pageCache, indexFile, new PathIndexLayout( description )
            {
                @Override
                public int minorVersion()
                {
                    return super.minorVersion() + 1;
                }
            }, 0 );
            fail( "Should not load" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
        }
    }

    @Test
    public void shouldSeeSimpleInsertions() throws Exception
    {
        Layout<TwoLongs,TwoLongs> layout = new PathIndexLayout( description );
        index = createIndex( 1024, layout );
        TwoLongs first = new TwoLongs( 1, 1 );
        TwoLongs second = new TwoLongs( 2, 2 );
        try ( Modifier<TwoLongs,TwoLongs> inserter = index.modifier( DEFAULTS ) )
        {
            inserter.insert( first, first );
            inserter.insert( second, second );
        }

        try ( RawCursor<Hit<TwoLongs,TwoLongs>,IOException> cursor =
                index.seek( first, new TwoLongs( Long.MAX_VALUE, Long.MAX_VALUE ) ) )
        {
            assertTrue( cursor.next() );
            assertEquals( first, cursor.get().key() );
            assertTrue( cursor.next() );
            assertEquals( second, cursor.get().key() );
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void shouldReadCorrectlyWhenConcurrentlyInserting() throws Throwable
    {
        // GIVEN
        Layout<TwoLongs,TwoLongs> layout = new PathIndexLayout( description );
        index = createIndex( 1024, layout );
        CountDownLatch readerReadySignal = new CountDownLatch( 1 );
        CountDownLatch startSignal = new CountDownLatch( 1 );
        AtomicBoolean endSignal = new AtomicBoolean();
        AtomicInteger highestId = new AtomicInteger( -1 );
        AtomicReference<Throwable> readerError = new AtomicReference<>();
        AtomicInteger numberOfReads = new AtomicInteger();
        Thread reader = new Thread()
        {
            @Override
            public void run()
            {
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
                        long lastSeen = -1;
                        try ( RawCursor<Hit<TwoLongs,TwoLongs>,IOException> cursor =
                                // "to" is exclusive so do +1 on that
                                index.seek( new TwoLongs( 0, 0 ), new TwoLongs( upToId + 1, 0 ) ) )
                        {
                            while ( cursor.next() )
                            {
                                TwoLongs hit = cursor.get().key();
                                assertEquals( lastSeen + 1, hit.first );
                                lastSeen = hit.first;
                            }
                        }
                        // It's possible that the writer has gone further since we started,
                        // but we should at least have seen upToId
                        if ( lastSeen < upToId/2 )
                        {
                            fail( "Wanted to have read up to " + upToId/2 +
                                    " (ideally " + upToId + " though)" +
                                    " but could only see up to " + lastSeen );
                        }
                        numberOfReads.incrementAndGet();
                    }
                }
                catch ( Throwable e )
                {
                    readerError.set( e );
                }
            }
        };

        // WHEN
        try
        {
            reader.start();
            readerReadySignal.await( 10, SECONDS );
            startSignal.countDown();
            Random random = ThreadLocalRandom.current();
            try ( Modifier<TwoLongs,TwoLongs> inserter = index.modifier( DEFAULTS ) )
            {
                int inserted = 0;
                while ( (inserted < 100_000 || numberOfReads.get() < 10) && readerError.get() == null )
                {
                    int groupCount = random.nextInt( 1_000 ) + 1;
                    for ( int i = 0; i < groupCount; i++, inserted++ )
                    {
                        TwoLongs thing = new TwoLongs( inserted, 0 );
                        inserter.insert( thing, thing );
                        highestId.set( inserted );
                    }
                    MILLISECONDS.sleep( random.nextInt( 10 ) + 3 );
                }
            }
        }
        finally
        {
            // THEN
            endSignal.set( true );
            reader.join( SECONDS.toMillis( 10 ) );
            if ( readerError.get() != null )
            {
                throw readerError.get();
            }
        }
    }

    private void randomlyModifyIndex( BPTreeIndex<TwoLongs,TwoLongs> index, Map<TwoLongs,TwoLongs> data, Random random )
            throws IOException
    {
        int changeCount = random.nextInt( 10 ) + 10;
        try ( Modifier<TwoLongs,TwoLongs> modifier = index.modifier( DEFAULTS ) )
        {
            for ( int i = 0; i < changeCount; i++ )
            {
                if ( random.nextBoolean() && data.size() > 0 )
                {   // remove
                    TwoLongs key = randomKey( data, random );
                    TwoLongs value = data.remove( key );
                    TwoLongs removedValue = modifier.remove( key );
                    assertEquals( "For " + key, value, removedValue );
                }
                else
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
