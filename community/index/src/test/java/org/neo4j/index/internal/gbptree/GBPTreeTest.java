/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.GBPTree.Monitor;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.Barrier;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.rules.RuleChain.outerRule;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.ThrowingRunnable.throwing;
import static org.neo4j.test.rule.PageCacheRule.config;

public class GBPTreeTest
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule( config().withAccessChecks( true ) );
    private final RandomRule random = new RandomRule();

    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule ).around( random );

    private PageCache pageCache;
    private File indexFile;
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private GBPTree<MutableLong,MutableLong> index;

    private GBPTree<MutableLong,MutableLong> createIndex( int pageSize )
            throws IOException
    {
        return createIndex( pageSize, NO_MONITOR );
    }

    private GBPTree<MutableLong,MutableLong> createIndex( int pageSize, Monitor monitor )
            throws IOException
    {
        pageCache = createPageCache( pageSize );
        return index = new GBPTree<>( pageCache, indexFile, layout, 0/*use whatever page cache says*/, monitor );
    }

    private PageCache createPageCache( int pageSize )
    {
        return pageCacheRule.getPageCache( fs.get(), config().withPageSize( pageSize ) );
    }

    @Before
    public void setUpIndexFile()
    {
        indexFile = directory.file( "index" );
    }

    @After
    public void closeIndexAndPageCache() throws IOException
    {
        if ( index != null )
        {
            assertTrue( index.consistencyCheck() );
            closeIndex();
        }
    }

    /* Meta and state page tests */

    @Test
    public void shouldReadWrittenMetaData() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong,MutableLong> index = createIndex( 1024 ) )
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
        try ( GBPTree<MutableLong,MutableLong> index = createIndex( 1024 ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index =
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
        try ( GBPTree<MutableLong,MutableLong> index = createIndex( 1024 ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index =
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
        try ( GBPTree<MutableLong,MutableLong> index = createIndex( 1024 ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index =
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
        try ( GBPTree<MutableLong,MutableLong> index = createIndex( 1024 ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index =
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
        try ( GBPTree<MutableLong,MutableLong> index = createIndex( pageSize ) )
        {   // Open/close is enough
        }
        index = null;

        // WHEN
        pageCache.close();
        pageCache = createPageCache( pageSize / 2 );
        try ( GBPTree<MutableLong,MutableLong> index = new GBPTree<>( pageCache, indexFile, layout, 0, NO_MONITOR ) )
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
    public void shouldFailOnStartingWithPageSizeLargerThanThatOfPageCache() throws Exception
    {
        // WHEN
        int pageSize = 512;
        pageCache = createPageCache( pageSize );
        try ( GBPTree<MutableLong,MutableLong> index =
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
    public void shouldMapIndexFileWithProvidedPageSizeIfLessThanOrEqualToCachePageSize() throws Exception
    {
        // WHEN
        int pageSize = 1024;
        pageCache = createPageCache( pageSize );
        try ( GBPTree<MutableLong,MutableLong> index =
                new GBPTree<>( pageCache, indexFile, layout, pageSize / 2, NO_MONITOR ) )
        {
            // Good
        }
    }

    @Test
    public void shouldFailWhenTryingToRemapWithPageSizeLargerThanCachePageSize() throws Exception
    {
        // WHEN
        int pageSize = 1024;
        pageCache = createPageCache( pageSize );
        try ( GBPTree<MutableLong,MutableLong> index =
                new GBPTree<>( pageCache, indexFile, layout, pageSize, NO_MONITOR ) )
        {
            // Good
        }

        pageCache = createPageCache( pageSize / 2 );
        try ( GBPTree<MutableLong, MutableLong> index =
                new GBPTree<>( pageCache, indexFile, layout, pageSize, NO_MONITOR ) )
        {
            fail( "Expected to fail" );
        }
        catch ( MetadataMismatchException e )
        {
            // THEN Good
            assertThat( e.getMessage(), containsString( "page size" ) );
        }
    }

    @Test
    public void shouldRemapFileIfMappedWithPageSizeLargerThanCreationSize() throws Exception
    {
        // WHEN
        int pageSize = 1024;
        pageCache = createPageCache( pageSize );
        List<Long> expectedData = new ArrayList<>();
        for ( long i = 0; i < 100; i++ )
        {
            expectedData.add( i );
        }
        try ( GBPTree<MutableLong,MutableLong> index =
                new GBPTree<>( pageCache, indexFile, layout, pageSize / 2, NO_MONITOR ) )
        {
            // Insert some data
            try ( Writer<MutableLong, MutableLong> writer = index.writer() )
            {
                MutableLong key = new MutableLong();
                MutableLong value = new MutableLong();

                for ( Long insert : expectedData )
                {
                    key.setValue( insert );
                    value.setValue( insert );
                    writer.put( key, value );
                }
            }
            index.checkpoint( IOLimiter.unlimited() );
        }

        // THEN
        try ( GBPTree<MutableLong,MutableLong> index = new GBPTree<>( pageCache, indexFile, layout, 0, NO_MONITOR ) )
        {
            MutableLong fromInclusive = new MutableLong( 0L );
            MutableLong toExclusive = new MutableLong( 200L );
            try ( RawCursor<Hit<MutableLong,MutableLong>, IOException> seek = index.seek( fromInclusive, toExclusive ) )
            {
                int i = 0;
                while ( seek.next() )
                {
                    Hit<MutableLong,MutableLong> hit = seek.get();
                    assertEquals( hit.key().getValue(), expectedData.get( i ) );
                    assertEquals( hit.value().getValue(), expectedData.get( i ) );
                    i++;
                }
            }
        }
    }

    @Test
    public void shouldFailWhenTryingToOpenWithDifferentFormatVersion() throws Exception
    {
        // GIVEN
        int pageSize = 1024;
        try ( GBPTree<MutableLong,MutableLong> index = createIndex( pageSize ) )
        {   // Open/close is enough
        }
        index = null;
        setFormatVersion( pageSize, GBPTree.FORMAT_VERSION - 1 );

        try
        {
            // WHEN
            index = new GBPTree<>( pageCache, indexFile, layout, 0, NO_MONITOR );
            fail( "Should have failed" );
        }
        catch ( MetadataMismatchException e )
        {
            // THEN good
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
        Writer<MutableLong,MutableLong> writer = index.writer();

        // WHEN
        try
        {
            index.writer();
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // THEN good
        }

        writer.close();
    }

    @Test
    public void shouldAllowClosingWriterMultipleTimes() throws Exception
    {
        // GIVEN
        index = createIndex( 256 );
        Writer<MutableLong,MutableLong> writer = index.writer();
        writer.put( new MutableLong( 0 ), new MutableLong( 1 ) );
        writer.close();

        // WHEN
        writer.close();

        // THEN that should be OK
    }

    /* Check-pointing tests */

    @Test
    public void checkPointShouldLockOutWriter() throws Exception
    {
        // GIVEN
        CheckpointControlledMonitor monitor = new CheckpointControlledMonitor();
        index = createIndex( 1024, monitor );
        long key = 10;
        try ( Writer<MutableLong,MutableLong> writer = index.writer() )
        {
            writer.put( new MutableLong( key ), new MutableLong( key ) );
        }

        // WHEN
        monitor.enabled = true;
        Thread checkpointer = new Thread( throwing( () -> index.checkpoint( IOLimiter.unlimited() ) ) );
        checkpointer.start();
        monitor.barrier.awaitUninterruptibly();
        // now we're in the smack middle of a checkpoint
        Thread t2 = new Thread( throwing( () -> index.writer().close() ) );
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
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
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
        try ( Writer<MutableLong,MutableLong> writer = index.writer() )
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
    public void shouldSplitCorrectly() throws Exception
    {
        // GIVEN
        GBPTree<MutableLong,MutableLong> index = createIndex( 256 );

        // WHEN
        int count = 1_000;
        PrimitiveLongSet seen = Primitive.longSet( count );
        try ( Writer<MutableLong,MutableLong> writer = index.writer() )
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
    public void shouldCheckpointAfterInitialCreation() throws Exception
    {
        // GIVEN
        CheckpointCounter checkpointCounter = new CheckpointCounter();

        // WHEN
        GBPTree<MutableLong,MutableLong> index = createIndex( 256, checkpointCounter );

        // THEN
        assertEquals( 1, checkpointCounter.count );
    }

    @Test
    public void shouldCheckpointOnCloseAfterChangesHappened() throws Exception
    {
        // GIVEN
        CheckpointCounter checkpointCounter = new CheckpointCounter();

        // WHEN
        GBPTree<MutableLong,MutableLong> index = createIndex( 256, checkpointCounter );
        int countBefore = checkpointCounter.count;
        try ( Writer<MutableLong,MutableLong> writer = index.writer() )
        {
            writer.put( new MutableLong( 0 ), new MutableLong( 1 ) );
        }

        // THEN
        closeIndex();
        assertEquals( countBefore + 1, checkpointCounter.count );
    }

    @Test
    public void shouldNotCheckpointOnCloseIfNoChangesHappened() throws Exception
    {
        // GIVEN
        CheckpointCounter checkpointCounter = new CheckpointCounter();

        // WHEN
        GBPTree<MutableLong,MutableLong> index = createIndex( 256, checkpointCounter );
        int countBefore = checkpointCounter.count;
        try ( Writer<MutableLong,MutableLong> writer = index.writer() )
        {
            writer.put( new MutableLong( 0 ), new MutableLong( 1 ) );
        }
        index.checkpoint( IOLimiter.unlimited() );
        assertEquals( countBefore + 1, checkpointCounter.count );

        // THEN
        closeIndex();
        assertEquals( countBefore + 1, checkpointCounter.count );
    }

    private void closeIndex() throws IOException
    {
        if ( index != null )
        {
            index.close();
            index = null;
        }
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

    private static class CheckpointCounter implements Monitor
    {
        private int count;

        @Override
        public void checkpointCompleted()
        {
            count++;
        }
    }

    private void setFormatVersion( int pageSize, int formatVersion ) throws IOException
    {
        try ( PagedFile pagedFile = pageCache.map( indexFile, pageSize );
              PageCursor cursor = pagedFile.io( IdSpace.META_PAGE_ID, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            assertTrue( cursor.next() );
            cursor.putInt( formatVersion );
        }
    }
}
