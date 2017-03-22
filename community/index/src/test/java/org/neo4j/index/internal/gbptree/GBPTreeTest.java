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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.GBPTree.Monitor;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.SilentHealth;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.test.Barrier;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER;
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

    private File indexFile;

    class GBPTreeBuilder
    {
        private PageCache pageCache;
        private Layout<MutableLong,MutableLong> layout;
        private int tentativePageSize;
        private GBPTree.Monitor monitor;
        private Header.Reader headerReader;
        private Log log;

        GBPTreeBuilder( PageCache pageCache )
        {
            this.pageCache = pageCache;
            this.layout = new SimpleLongLayout();
            this.tentativePageSize = pageCache.pageSize();
            this.monitor = NO_MONITOR;
            this.headerReader = NO_HEADER;
            this.log = NullLog.getInstance();
        }

        GBPTree<MutableLong,MutableLong> build() throws IOException
        {
            return new GBPTree<>( pageCache, indexFile, layout, tentativePageSize, monitor, headerReader,
                    new SilentHealth(), log );
        }

        GBPTreeBuilder with( int tentativePageSize )
        {
            this.tentativePageSize = tentativePageSize;
            return this;
        }

        GBPTreeBuilder with( GBPTree.Monitor monitor )
        {
            this.monitor = monitor;
            return this;
        }

        GBPTreeBuilder with( Header.Reader headerReader )
        {
            this.headerReader = headerReader;
            return this;
        }

        GBPTreeBuilder with( Log log )
        {
            this.log = log;
            return this;
        }

        public GBPTreeBuilder with( Layout<MutableLong,MutableLong> layout )
        {
            this.layout = layout;
            return this;
        }
    }

    private GBPTreeBuilder builder( PageCache pageCache )
    {
        return new GBPTreeBuilder( pageCache );
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

    /* Meta and state page tests */

    @Test
    public void shouldReadWrittenMetaData() throws Exception
    {
        // GIVEN
        PageCache pageCache = createPageCache( 1024 );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {   // Open/close is enough
        }

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {   // Open/close is enough
        }

        // THEN being able to open validates that the same meta data was read
    }

    @Test
    public void shouldFailToOpenOnDifferentMetaData() throws Exception
    {
        // GIVEN
        PageCache pageCache = createPageCache( 1024 );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {   // Open/close is enough
        }

        // WHEN
        SimpleLongLayout otherLayout = new SimpleLongLayout( "Something else" );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).with( otherLayout ).build() )
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
        PageCache pageCache = createPageCache( 1024 );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {   // Open/close is enough
        }

        // WHEN
        SimpleLongLayout otherLayout = new SimpleLongLayout()
        {
            @Override
            public long identifier()
            {
                return 123456;
            }
        };
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).with( otherLayout ).build() )
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
        PageCache pageCache = createPageCache( 1024 );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {   // Open/close is enough
        }

        // WHEN
        SimpleLongLayout otherLayout = new SimpleLongLayout()
        {
            @Override
            public int majorVersion()
            {
                return super.majorVersion() + 1;
            }
        };
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).with( otherLayout ).build() )
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
        PageCache pageCache = createPageCache( 1024 );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {   // Open/close is enough
        }

        // WHEN
        SimpleLongLayout otherLayout = new SimpleLongLayout()
        {
            @Override
            public int minorVersion()
            {
                return super.minorVersion() + 1;
            }
        };
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).with( otherLayout ).build() )
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
        PageCache pageCache = createPageCache( pageSize );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {   // Open/close is enough
        }

        // WHEN
        pageCache.close();
        pageCache = createPageCache( pageSize / 2 );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
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
        PageCache pageCache = createPageCache( pageSize );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).with( pageSize * 2 ).build() )
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
        PageCache pageCache = createPageCache( pageSize );
        int tentativePageSize = pageSize / 2;
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).with( tentativePageSize ).build() )
        {
            // Good
        }
    }

    @Test
    public void shouldFailWhenTryingToRemapWithPageSizeLargerThanCachePageSize() throws Exception
    {
        // WHEN
        int pageSize = 1024;
        PageCache pageCache = createPageCache( pageSize );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {
            // Good
        }

        pageCache = createPageCache( pageSize / 2 );
        try ( GBPTree<MutableLong, MutableLong> index = builder( pageCache ).build() )
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
        PageCache pageCache = createPageCache( pageSize );
        List<Long> expectedData = new ArrayList<>();
        for ( long i = 0; i < 100; i++ )
        {
            expectedData.add( i );
        }
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).with( pageSize / 2 ).build() )
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
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
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
        PageCache pageCache = createPageCache( pageSize );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {   // Open/close is enough
        }
        setFormatVersion( pageCache, pageSize, GBPTree.FORMAT_VERSION - 1 );

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {
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
        PageCache pageCache = createPageCache( 256 );
        try( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {
            // WHEN
            RawCursor<Hit<MutableLong,MutableLong>,IOException> result =
                    index.seek( new MutableLong( 0 ), new MutableLong( 10 ) );

            // THEN
            assertFalse( result.next() );
        }
    }

    @Test
    public void shouldNotBeAbleToAcquireModifierTwice() throws Exception
    {
        // GIVEN
        PageCache pageCache = createPageCache( 256 );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
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
            }
        }
    }

    @Test
    public void shouldAllowClosingWriterMultipleTimes() throws Exception
    {
        // GIVEN
        PageCache pageCache = createPageCache( 256 );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {
            Writer<MutableLong,MutableLong> writer = index.writer();
            writer.put( new MutableLong( 0 ), new MutableLong( 1 ) );
            writer.close();

            // WHEN
            writer.close();

            // THEN that should be OK
        }
    }

    @Test
    public void shouldAllowClosingTreeMultipleTimes() throws Exception
    {
        // GIVEN
        index = createIndex( 256 );

        // WHEN
        index.close();

        // THEN
        index.close(); // should be OK
        index = null; // so that our @After clause won't try to consistency check it
    }

    @Test
    public void shouldPutHeaderDataInCheckPoint() throws Exception
    {
        // GIVEN
        byte[] expectedHeader = new byte[12];
        ThreadLocalRandom.current().nextBytes( expectedHeader );
        PageCache pageCache = createPageCache( 256 );
        GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build();
        index.close( cursor -> cursor.putBytes( expectedHeader ) );

        // WHEN
        byte[] readHeader = new byte[expectedHeader.length];
        AtomicInteger length = new AtomicInteger();
        Header.Reader reader = ( cursor, len ) ->
        {
            length.set( len );
            cursor.getBytes( readHeader );
        };
        index = builder( pageCache )
                .with( reader )
                .build();
        index.close();

        // THEN
        assertEquals( expectedHeader.length, length.get() );
        assertArrayEquals( expectedHeader, readHeader );
    }

    @Test
    public void shouldCarryOverHeaderDataInNextCheckPoint() throws Exception
    {
        // GIVEN
        byte[] expectedHeader = new byte[12];
        ThreadLocalRandom.current().nextBytes( expectedHeader );
        PageCache pageCache = createPageCache( 256 );
        GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build();
        index.checkpoint( IOLimiter.unlimited(), cursor -> cursor.putBytes( expectedHeader ) );
        index.close();

        // WHEN
        byte[] readHeader = new byte[expectedHeader.length];
        index = builder( pageCache )
                .with( (cursor,length) -> cursor.getBytes( readHeader ) )
                .build();
        index.close();

        // THEN
        assertArrayEquals( expectedHeader, readHeader );
    }

    @Test
    public void shouldReplaceHeaderDataInNextCheckPoint() throws Exception
    {
        // GIVEN
        byte[] expectedHeader = new byte[12];
        ThreadLocalRandom.current().nextBytes( expectedHeader );
        PageCache pageCache = createPageCache( 256 );
        GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build();
        index.checkpoint( IOLimiter.unlimited(), cursor -> cursor.putBytes( expectedHeader ) );
        ThreadLocalRandom.current().nextBytes( expectedHeader );
        index.close( cursor -> cursor.putBytes( expectedHeader ) );

        // WHEN
        byte[] readHeader = new byte[expectedHeader.length];
        index = builder( pageCache )
                .with( (cursor,length) -> cursor.getBytes( readHeader ) )
                .build();
        index.close();

        // THEN
        assertArrayEquals( expectedHeader, readHeader );
    }

    /* Check-pointing tests */

    @Test
    public void checkPointShouldLockOutWriter() throws Exception
    {
        // GIVEN
        CheckpointControlledMonitor monitor = new CheckpointControlledMonitor();
        PageCache pageCache = createPageCache( 1024 );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).with( monitor ).build() )
        {
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
    }

    @Test
    public void checkPointShouldWaitForWriter() throws Exception
    {
        // GIVEN
        PageCache pageCache = createPageCache( 1024 );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {
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
    }

    @Test
    public void closeShouldLockOutWriter() throws Exception
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
        Thread closer = new Thread( throwing( () -> index.close() ) );
        closer.start();
        monitor.barrier.awaitUninterruptibly();
        // now we're in the smack middle of a close/checkpoint
        AtomicReference<Exception> writerError = new AtomicReference<>();
        Thread t2 = new Thread( () ->
        {
            try
            {
                index.writer().close();
            }
            catch ( Exception e )
            {
                writerError.set( e );
            }
        } );

        t2.start();
        t2.join( 200 );
        assertTrue( Arrays.toString( closer.getStackTrace() ), t2.isAlive() );
        monitor.barrier.release();

        // THEN
        t2.join();
        assertTrue( "Writer should not be able to acquired after close",
                writerError.get() instanceof IllegalStateException );
        index = null;
    }

    @Test
    public void closeShouldWaitForWriter() throws Exception
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
        Thread closer = new Thread( throwing( () -> index.close() ) );
        closer.start();
        closer.join( 200 );
        assertTrue( closer.isAlive() );

        // THEN
        barrier.release();
        closer.join();
        index = null;
    }

    /* Insertion and read tests */

    @Test
    public void shouldSeeSimpleInsertions() throws Exception
    {
        PageCache pageCache = createPageCache( 256 );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {
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
    }

    /* Randomized tests */

    @Test
    public void shouldSplitCorrectly() throws Exception
    {
        // GIVEN
        PageCache pageCache = createPageCache( 256 );
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).build() )
        {
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
    }

    @Test
    public void shouldCheckpointAfterInitialCreation() throws Exception
    {
        // GIVEN
        CheckpointCounter checkpointCounter = new CheckpointCounter();
        PageCache pageCache = createPageCache( 256 );

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).with( checkpointCounter ).build() )
        {}

        // THEN
        assertEquals( 1, checkpointCounter.count() );
    }

    @Test
    public void shouldCheckpointOnCloseAfterChangesHappened() throws Exception
    {
        // GIVEN
        CheckpointCounter checkpointCounter = new CheckpointCounter();
        PageCache pageCache = createPageCache( 256 );

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).with( checkpointCounter ).build() )
        {
            checkpointCounter.reset();
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                writer.put( new MutableLong( 0 ), new MutableLong( 1 ) );
            }
        }

        // THEN
        assertEquals( 1, checkpointCounter.count() );
    }

    @Test
    public void shouldNotCheckpointOnCloseIfNoChangesHappened() throws Exception
    {
        // GIVEN
        CheckpointCounter checkpointCounter = new CheckpointCounter();
        PageCache pageCache = createPageCache( 256 );

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index = builder( pageCache ).with( checkpointCounter ).build() )
        {
            checkpointCounter.reset();
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                writer.put( new MutableLong( 0 ), new MutableLong( 1 ) );
            }
            index.checkpoint( IOLimiter.unlimited() );
            assertEquals( 1, checkpointCounter.count() );
        }
        // THEN
        assertEquals( 1, checkpointCounter.count() );
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

    private void setFormatVersion( PageCache pageCache, int pageSize, int formatVersion ) throws IOException
    {
        try ( PagedFile pagedFile = pageCache.map( indexFile, pageSize );
              PageCursor cursor = pagedFile.io( IdSpace.META_PAGE_ID, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            assertTrue( cursor.next() );
            cursor.putInt( formatVersion );
        }
    }
}
