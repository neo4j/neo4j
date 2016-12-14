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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.Hit;
import org.neo4j.index.Index;
import org.neo4j.index.IndexWriter;
import org.neo4j.index.ValueMerger;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.index.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.gbptree.ThrowingRunnable.throwing;
import static org.neo4j.io.pagecache.IOLimiter.unlimited;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

public class GBPTreeRecoveryTest
{
    private static final int PAGE_SIZE = 256;

    private final RandomRule random = new RandomRule();
    private final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( random ).around( fs ).around( directory );

    private final MutableLong key = new MutableLong();
    private final MutableLong value = new MutableLong();

    @Test
    public void shouldRecoverFromCrashBeforeFirstCheckpoint() throws Exception
    {
        // GIVEN
        // a tree with only small amount of data that has not yet seen checkpoint from outside
        File file = directory.file( "index" );
        {
            PageCache pageCache = createPageCache();
            GBPTree<MutableLong,MutableLong> index = createIndex( pageCache, file );
            IndexWriter<MutableLong,MutableLong> writer = index.writer( IndexWriter.Options.DEFAULTS );

            key.setValue( 1L );
            value.setValue( 10L );
            writer.put( key, value );
            pageCache.flushAndForce();
            fs.snapshot( throwing( () ->
            {
                writer.close();
                index.close();
                pageCache.close();
            } ) );
        }

        // WHEN
        try ( PageCache pageCache = createPageCache();
              GBPTree<MutableLong,MutableLong> index = createIndex( pageCache, file ) )
        {
            // this is the mimic:ed recovery
            index.prepareForRecovery();

            try ( IndexWriter<MutableLong,MutableLong> writer = index.writer( IndexWriter.Options.DEFAULTS ) )
            {
                writer.put( key, value );
            }

            // THEN
            // we should end up with a consistent index
            index.consistencyCheck();

            // ... containing all the stuff load says
            try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                          index.seek( new MutableLong( Long.MIN_VALUE ), new MutableLong( Long.MAX_VALUE ) ) )
            {
                assertTrue( cursor.next() );
                Hit<MutableLong,MutableLong> hit = cursor.get();
                assertEquals( key.getValue(), hit.key().getValue() );
                assertEquals( value.getValue(), hit.value().getValue() );
            }
        }
    }

    @Test
    public void shouldRecoverFromAnything() throws Exception
    {
        // GIVEN
        // a tree which has had random updates and checkpoints in it, load generated with specific seed
        File file = directory.file( "index" );
        List<Action> load = generateLoad();
        {
            PageCache pageCache = createPageCache();
            GBPTree<MutableLong,MutableLong> index = createIndex( pageCache, file );
            execute( load, index );
            pageCache.flushAndForce();
            fs.snapshot( throwing( () ->
            {
                index.close();
                pageCache.close();
            } ) );
        }

        // WHEN doing recovery
        // using the same seed, generate the same load and replay all transactions from the last checkpoint
        try ( PageCache pageCache = createPageCache();
                GBPTree<MutableLong,MutableLong> index = createIndex( pageCache, file ) )
        {
            // this is the mimic:ed recovery
            index.prepareForRecovery();
            execute( fromLastCheckPoint( load ), index );

            // THEN
            // we should end up with a consistent index containing all the stuff load says
            index.consistencyCheck();
            long[/*key,value,key,value...*/] aggregate = expectedSortedAggregatedDataFromGeneratedLoad( load );
            try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                    index.seek( new MutableLong( Long.MIN_VALUE ), new MutableLong( Long.MAX_VALUE ) ) )
            {
                for ( int i = 0; i < aggregate.length; )
                {
                    assertTrue( cursor.next() );
                    Hit<MutableLong,MutableLong> hit = cursor.get();
                    assertEquals( aggregate[i++], hit.key().longValue() );
                    assertEquals( aggregate[i++], hit.value().longValue() );
                }
                assertFalse( cursor.next() );
            }
        }
    }

    private static void execute( List<Action> load, Index<MutableLong,MutableLong> index )
            throws IOException
    {
        for ( Action action : load )
        {
            action.execute( index );
        }
    }

    private static long[] expectedSortedAggregatedDataFromGeneratedLoad( List<Action> load ) throws IOException
    {
        CapturingIndex index = new CapturingIndex();
        execute( load, index );
        @SuppressWarnings( "unchecked" )
        Map.Entry<Long,Long>[] entries = index.map.entrySet().toArray( new Map.Entry[index.map.size()] );
        long[] result = new long[entries.length * 2];
        for ( int i = 0, c = 0; i < entries.length; i++ )
        {
            result[c++] = entries[i].getKey().longValue();
            result[c++] = entries[i].getValue().longValue();
        }
        return result;
    }

    private static List<Action> fromLastCheckPoint( List<Action> actions ) throws IOException
    {
        int lastCheckpoint = indexOfLastCheckpoint( actions );
        return actions.size() > lastCheckpoint + 1
                ? actions.subList( lastCheckpoint + 1, actions.size() )
                : Collections.emptyList();
    }

    private static int indexOfLastCheckpoint( List<Action> actions ) throws IOException
    {
        CapturingIndex index = new CapturingIndex();
        int i = 0;
        int lastCheckpoint = -1;
        for ( Action action : actions )
        {
            action.execute( index );
            if ( index.wasCheckpoint() )
            {
                lastCheckpoint = i;
            }
            i++;
        }
        return lastCheckpoint;
    }

    private List<Action> generateLoad()
    {
        List<Action> actions = new ArrayList<>();
        int count = random.intBetween( 300, 1_000 );
        for ( int i = 0; i < count; i++ )
        {
            actions.add( randomAction() );
        }
        return actions;
    }

    private Action randomAction()
    {
        float randomized = random.nextFloat();
        if ( randomized <= 0.7 )
        {
            // put
            long[] data = modificationData( 30, 200 );
            return index ->
            {
                try ( IndexWriter<MutableLong,MutableLong> writer = index.writer( IndexWriter.Options.DEFAULTS ) )
                {
                    for ( int i = 0; i < data.length; )
                    {
                        key.setValue( data[i++] );
                        value.setValue( data[i++] );
                        writer.put( key, value );
                    }
                }
            };
        }
        else if ( randomized <= 0.95 )
        {
            // remove
            long[] data = modificationData( 5, 20 );
            return index ->
            {
                try ( IndexWriter<MutableLong,MutableLong> writer = index.writer( IndexWriter.Options.DEFAULTS ) )
                {
                    for ( int i = 0; i < data.length; )
                    {
                        key.setValue( data[i++] );
                        i++; // value
                        writer.remove( key );
                    }
                }
            };
        }
        else
        {
            // checkpoint
            return index -> index.checkpoint( unlimited() );
        }
    }

    private long[] modificationData( int min, int max )
    {
        int count = random.intBetween( min, max );
        long[] data = new long[count * 2];
        for ( int i = 0, c = 0; i < count; i++ )
        {
            data[c++] = random.intBetween( 0, 1_000_000 ); // key
            data[c++] = random.intBetween( 0, 1_000_000 ); // value
        }
        return data;
    }

    private static GBPTree<MutableLong,MutableLong> createIndex( PageCache pageCache, File file ) throws IOException
    {
        return new GBPTree<>( pageCache, file, new SimpleLongLayout(), 0, NO_MONITOR );
    }

    private PageCache createPageCache()
    {
        PageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        swapper.setFileSystemAbstraction( fs.get() );
        return new MuninnPageCache( swapper, 4_000, PAGE_SIZE, NULL );
    }

    interface Action
    {
        void execute( Index<MutableLong,MutableLong> index ) throws IOException;
    }

    private static class CapturingIndex implements Index<MutableLong,MutableLong>, IndexWriter<MutableLong,MutableLong>
    {
        private final TreeMap<Long,Long> map = new TreeMap<>();
        private boolean checkpointCalled;

        @Override
        public void close() throws IOException
        {   // no
        }

        @Override
        public RawCursor<Hit<MutableLong,MutableLong>,IOException> seek( MutableLong fromInclusive,
                MutableLong toExclusive ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexWriter<MutableLong,MutableLong> writer( Options options ) throws IOException
        {
            return this;
        }

        boolean wasCheckpoint()
        {
            try
            {
                return checkpointCalled;
            }
            finally
            {
                checkpointCalled = false;
            }
        }

        @Override
        public void checkpoint( IOLimiter ioLimiter ) throws IOException
        {
            checkpointCalled = true;
        }

        @Override
        public void put( MutableLong key, MutableLong value ) throws IOException
        {
            map.put( key.getValue(), value.getValue() );
        }

        @Override
        public void merge( MutableLong key, MutableLong value, ValueMerger<MutableLong> valueMerger ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public MutableLong remove( MutableLong key ) throws IOException
        {
            Long value = map.remove( key.getValue() );
            return value == null ? null : new MutableLong( value );
        }
    }
}
