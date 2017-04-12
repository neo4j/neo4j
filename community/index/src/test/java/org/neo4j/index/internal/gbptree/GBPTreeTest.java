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
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.GBPTree.Monitor;
import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.DelegatingPagedFile;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.test.Barrier;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
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
import static org.neo4j.io.pagecache.IOLimiter.unlimited;
import static org.neo4j.test.rule.PageCacheRule.config;

@SuppressWarnings( "EmptyTryBlock" )
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
    private static final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private ExecutorService executor;

    @Before
    public void setUp()
    {
        executor = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
        indexFile = directory.file( "index" );
    }

    @After
    public void teardown()
    {
        executor.shutdown();
    }

    /* Meta and state page tests */

    @Test
    public void shouldReadWrittenMetaData() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong,MutableLong> ignored = index().build() )
        {   // open/close is enough
        }

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> ignored = index().build() )
        {   // open/close is enough
        }

        // THEN being able to open validates that the same meta data was read
    }

    @Test
    public void shouldFailToOpenOnDifferentMetaData() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong,MutableLong> ignored = index().withPageCachePageSize( 1024 ).build() )
        {   // Open/close is enough
        }

        // WHEN
        SimpleLongLayout otherLayout = new SimpleLongLayout( "Something else" );
        try ( GBPTree<MutableLong,MutableLong> ignored = index().with( otherLayout ).build() )
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
        try ( GBPTree<MutableLong,MutableLong> ignored = index().build() )
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
        try ( GBPTree<MutableLong,MutableLong> ignored = index().with( otherLayout ).build() )
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
        try ( GBPTree<MutableLong,MutableLong> ignored = index().withPageCachePageSize( 1024 ).build() )
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
        try ( GBPTree<MutableLong,MutableLong> ignored = index().with( otherLayout ).build() )
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
        try ( GBPTree<MutableLong,MutableLong> ignored = index().build() )
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
        try ( GBPTree<MutableLong,MutableLong> ignored = index().with( otherLayout ).build() )
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
        try ( GBPTree<MutableLong,MutableLong> ignored = index().withPageCachePageSize( pageSize ).build() )
        {   // Open/close is enough
        }

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> ignored = index().withPageCachePageSize( pageSize / 2 ).build() )
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
        int pageCachePageSize = 512;
        try ( GBPTree<MutableLong,MutableLong> ignored = index()
                .withPageCachePageSize( pageCachePageSize )
                .withIndexPageSize( 2 * pageCachePageSize )
                .build() )
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
        int pageCachePageSize = 1024;
        try ( GBPTree<MutableLong,MutableLong> ignored = index()
                .withPageCachePageSize( pageCachePageSize )
                .withIndexPageSize( pageCachePageSize / 2 )
                .build() )
        {
            // Good
        }
    }

    @Test
    public void shouldFailWhenTryingToRemapWithPageSizeLargerThanCachePageSize() throws Exception
    {
        // WHEN
        int pageCachePageSize = 1024;
        try ( GBPTree<MutableLong,MutableLong> ignored = index().withPageCachePageSize( pageCachePageSize ).build() )
        {
            // Good
        }

        try ( GBPTree<MutableLong, MutableLong> ignored = index()
                .withPageCachePageSize( pageCachePageSize / 2 )
                .withIndexPageSize( pageCachePageSize )
                .build() )
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
        List<Long> expectedData = new ArrayList<>();
        for ( long i = 0; i < 100; i++ )
        {
            expectedData.add( i );
        }
        try ( GBPTree<MutableLong,MutableLong> index = index()
                .withPageCachePageSize( pageSize )
                .withIndexPageSize( pageSize / 2 )
              .build() )
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
            index.checkpoint( unlimited() );
        }

        // THEN
        try ( GBPTree<MutableLong,MutableLong> index = index().withPageCachePageSize( pageSize ).build() )
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
        int pageSize = 256;
        try ( GBPTree<MutableLong,MutableLong> ignored = index().withPageCachePageSize( pageSize ).build() )
        {   // Open/close is enough
        }
        setFormatVersion( pageSize, GBPTree.FORMAT_VERSION - 1 );

        try
        {
            // WHEN
            index().withPageCachePageSize( pageSize ).build();
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
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
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
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
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
    }

    @Test
    public void shouldAllowClosingWriterMultipleTimes() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
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
        GBPTree<MutableLong,MutableLong> index = index().build();

        // WHEN
        index.close();

        // THEN
        index.close(); // should be OK
    }

    @Test
    public void shouldPutHeaderDataInCheckPoint() throws Exception
    {
        // GIVEN
        byte[] expectedHeader = new byte[12];
        ThreadLocalRandom.current().nextBytes( expectedHeader );
        GBPTree<MutableLong,MutableLong> index = index().build();
        index.checkpoint( IOLimiter.unlimited(), cursor -> cursor.putBytes( expectedHeader ) );
        index.close();

        // WHEN
        byte[] readHeader = new byte[expectedHeader.length];
        AtomicInteger length = new AtomicInteger();
        Header.Reader headerReader = ( cursor, len ) ->
        {
            length.set( len );
            cursor.getBytes( readHeader );
        };
        try ( GBPTree<MutableLong,MutableLong> ignored = index().with( headerReader ).build() )
        {   // open/close is enough
        }

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
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            index.checkpoint( unlimited(), cursor -> cursor.putBytes( expectedHeader ) );
        }

        // WHEN
        byte[] readHeader = new byte[expectedHeader.length];
        Header.Reader headerReader = ( cursor, length ) -> cursor.getBytes( readHeader );
        try ( GBPTree<MutableLong,MutableLong> ignored = index().with( headerReader ).build() )
        {   // open/close is enough
        }

        // THEN
        assertArrayEquals( expectedHeader, readHeader );
    }

    @Test
    public void shouldReplaceHeaderDataInNextCheckPoint() throws Exception
    {
        // GIVEN
        byte[] expectedHeader = new byte[12];
        ThreadLocalRandom.current().nextBytes( expectedHeader );
        GBPTree<MutableLong,MutableLong> index = index().build();
        index.checkpoint( unlimited(), cursor -> cursor.putBytes( expectedHeader ) );
        ThreadLocalRandom.current().nextBytes( expectedHeader );
        index.checkpoint( IOLimiter.unlimited(), cursor -> cursor.putBytes( expectedHeader ) );
        index.close();

        // WHEN
        byte[] readHeader = new byte[expectedHeader.length];
        Header.Reader headerReader = (cursor,length) -> cursor.getBytes( readHeader );
        try ( GBPTree<MutableLong,MutableLong> ignored = index().with( headerReader ).build() )
        {   // open/close is enough
        }

        // THEN
        assertArrayEquals( expectedHeader, readHeader );
    }

    /* Check-pointing tests */

    @Test
    public void checkPointShouldLockOutWriter() throws Exception
    {
        // GIVEN
        CheckpointControlledMonitor monitor = new CheckpointControlledMonitor();
        try ( GBPTree<MutableLong,MutableLong> index = index().with( monitor ).build() )
        {
            long key = 10;
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                writer.put( new MutableLong( key ), new MutableLong( key ) );
            }

            // WHEN
            monitor.enabled = true;
            Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( unlimited() ) ) );
            monitor.barrier.awaitUninterruptibly();
            // now we're in the smack middle of a checkpoint
            Future<?> writerClose = executor.submit( throwing( () -> index.writer().close() ) );

            // THEN
            wait( writerClose );
            monitor.barrier.release();

            writerClose.get();
            checkpoint.get();
        }
    }

    @Test
    public void checkPointShouldWaitForWriter() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            // WHEN
            Barrier.Control barrier = new Barrier.Control();
             Future<?> write = executor.submit( throwing( () ->
            {
                try ( Writer<MutableLong,MutableLong> writer = index.writer() )
                {
                    writer.put( new MutableLong( 1 ), new MutableLong( 1 ) );
                    barrier.reached();
                }
            } ) );
            barrier.awaitUninterruptibly();
            Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( unlimited() ) ) );
            wait( checkpoint );

            // THEN
            barrier.release();
            checkpoint.get();
            write.get();
        }
    }

    @Test
    public void closeShouldLockOutWriter() throws Exception
    {
        // GIVEN
        AtomicBoolean enabled = new AtomicBoolean();
        Barrier.Control barrier = new Barrier.Control();
        PageCache pageCacheWithBarrier = pageCacheWithBarrierInClose( enabled, barrier );
        GBPTree<MutableLong,MutableLong> index = index().with( pageCacheWithBarrier ).build();
        long key = 10;
        try ( Writer<MutableLong,MutableLong> writer = index.writer() )
        {
            writer.put( new MutableLong( key ), new MutableLong( key ) );
        }

        // WHEN
        enabled.set( true );
        Future<?> close = executor.submit( throwing( index::close ) );
        barrier.awaitUninterruptibly();
        // now we're in the smack middle of a close/checkpoint
        AtomicReference<Exception> writerError = new AtomicReference<>();
        Future<?> write = executor.submit( () ->
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

        wait( write );
        barrier.release();

        // THEN
        write.get();
        close.get();
        assertTrue( "Writer should not be able to acquired after close",
                writerError.get() instanceof IllegalStateException );
    }

    private PageCache pageCacheWithBarrierInClose( final AtomicBoolean enabled, final Barrier.Control barrier )
    {
        return new DelegatingPageCache( createPageCache( 1024 ) )
        {
            @Override
            public PagedFile map( File file, int pageSize, OpenOption... openOptions ) throws IOException
            {
                return new DelegatingPagedFile( super.map( file, pageSize, openOptions ) )
                {
                    @Override
                    public void close() throws IOException
                    {
                        if ( enabled.get() )
                        {
                            barrier.reached();
                        }
                        super.close();
                    }
                };
            }
        };
    }

    @Test
    public void closeShouldWaitForWriter() throws Exception
    {
        // GIVEN
        GBPTree<MutableLong,MutableLong> index = index().build();

        // WHEN
        Barrier.Control barrier = new Barrier.Control();
        Future<?> write = executor.submit( throwing( () ->
        {
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                writer.put( new MutableLong( 1 ), new MutableLong( 1 ) );
                barrier.reached();
            }
        } ) );
        barrier.awaitUninterruptibly();
        Future<?> close = executor.submit( throwing( index::close ) );
        wait( close );

        // THEN
        barrier.release();
        close.get();
        write.get();
    }

    /* Insertion and read tests */

    @Test
    public void shouldSeeSimpleInsertions() throws Exception
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
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
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
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

    /* Checkpoint tests */

    @Test
    public void shouldCheckpointAfterInitialCreation() throws Exception
    {
        // GIVEN
        CheckpointCounter checkpointCounter = new CheckpointCounter();

        // WHEN
        GBPTree<MutableLong,MutableLong> index = index().with( checkpointCounter ).build();

        // THEN
        assertEquals( 1, checkpointCounter.count() );
        index.close();
    }

    @Test
    public void shouldNotCheckpointOnCloseIfNoChangesHappened() throws Exception
    {
        // GIVEN
        CheckpointCounter checkpointCounter = new CheckpointCounter();

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index = index().with( checkpointCounter ).build() )
        {
            checkpointCounter.reset();
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                writer.put( new MutableLong( 0 ), new MutableLong( 1 ) );
            }
            index.checkpoint( unlimited() );
            assertEquals( 1, checkpointCounter.count() );
        }

        // THEN
        assertEquals( 1, checkpointCounter.count() );
    }

    /* Read only mode tests */

    @Test
    public void openIndexInReadOnlyMustNotWriteAnything() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong, MutableLong> ignored = index().build() )
        {
            // simply create
        }
        PageCacheTracer tracer = new DefaultPageCacheTracer();
        PageCacheRule.PageCacheConfig config = PageCacheRule.config().withTracer( tracer );

        // WHEN
        try ( PageCache pageCache = pageCacheRule.getPageCache( fs, config );
              GBPTree<MutableLong,MutableLong> ignored = index().readOnly().with( pageCache ).build() )
        {
            assertThat( tracer.bytesWritten(), is( 0L ) );
        }
    }

    @Test
    public void checkpointInReadOnlyModeMustThrow() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong, MutableLong> ignored = index().build() )
        {
            // simply create
        }

        // WHEN
        try ( GBPTree<MutableLong, MutableLong> index = index().readOnly().build() )
        {
            index.checkpoint( IOLimiter.unlimited() );
            fail( "Expected checkpoint in read only mode to throw" );
        }
        catch ( UnsupportedOperationException e )
        {
            // THEN
            // good
            assertThat( e.getMessage(),
                    allOf( containsString( "read only" ), containsString( "checkpoint" ) ) );
        }
    }

    @Test
    public void openWriterInReadOnlyMustThrow() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong, MutableLong> ignored = index().build() )
        {
            // simply create
        }

        // WHEN
        try ( GBPTree<MutableLong, MutableLong> index = index().readOnly().build() )
        {
            index.writer();
            fail( "Expected open writer in read only mode to throw" );
        }
        catch ( UnsupportedOperationException e )
        {
            // THEN
            // good
            assertThat( e.getMessage(),
                    allOf( containsString( "read only" ), containsString( "writer" ) ) );
        }
    }

    @Test
    public void createNewIndexInReadOnlyModeMustThrow() throws Exception
    {
        // GIVEN
        // no existing index

        // WHEN
        try ( GBPTree<MutableLong, MutableLong> ignored = index().readOnly().build() )
        {
            fail( "Expected creation of new index in read only mode to throw" );
        }
        catch ( UnsupportedOperationException e )
        {
            // THEN
            // good
            assertThat( e.getMessage(),
                    allOf( containsString( "read only" ), containsString( "create" ) ) );
        }
    }

    @Test
    public void prepareForRecoveryInReadOnlyMustThrow() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong, MutableLong> ignored = index().build() )
        {
            // simply create
        }

        // WHEN
        try ( GBPTree<MutableLong, MutableLong> index = index().readOnly().build() )
        {
            index.prepareForRecovery();
            fail( "Expected prepare for recovery in read only mode to throw" );
        }
        catch ( UnsupportedOperationException e )
        {
            // THEN
            // good
            assertThat( e.getMessage(),
                    allOf( containsString( "read only" ), containsString( "prepare for recovery" ) ) );
        }
    }

    @Test
    public void finishRecoveryInReadOnlyMustThrow() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong, MutableLong> ignored = index().build() )
        {
            // simply create
        }

        // WHEN
        try ( GBPTree<MutableLong, MutableLong> index = index().readOnly().build() )
        {
            index.finishRecovery();
            fail( "Expected finish recovery in read only mode to throw" );
        }
        catch ( UnsupportedOperationException e )
        {
            // THEN
            // good
            assertThat( e.getMessage(),
                    allOf( containsString( "read only" ), containsString( "finish recovery" ) ) );
        }
    }

    private void wait( Future<?> future )throws InterruptedException, ExecutionException
    {
        try
        {
            future.get( 200, TimeUnit.MILLISECONDS );
            fail( "Expected timeout" );
        }
        catch ( TimeoutException e )
        {
            // good
        }
    }

    private PageCache createPageCache( int pageSize )
    {
        return pageCacheRule.getPageCache( fs.get(), config().withPageSize( pageSize ) );
    }

    private GBPTreeBuilder index()
    {
        return new GBPTreeBuilder();
    }

    private class GBPTreeBuilder
    {
        private int pageCachePageSize = 256;
        private int tentativePageSize = 0;
        private Monitor monitor = NO_MONITOR;
        private Header.Reader headerReader = NO_HEADER;
        private Layout<MutableLong,MutableLong> layout = GBPTreeTest.layout;
        private PageCache specificPageCache;
        private boolean readOnly = false;

        private GBPTreeBuilder withPageCachePageSize( int pageSize )
        {
            this.pageCachePageSize = pageSize;
            return this;
        }

        private GBPTreeBuilder withIndexPageSize( int tentativePageSize )
        {
            this.tentativePageSize = tentativePageSize;
            return this;
        }

        private GBPTreeBuilder with( GBPTree.Monitor monitor )
        {
            this.monitor = monitor;
            return this;
        }

        private GBPTreeBuilder with( Header.Reader headerReader )
        {
            this.headerReader = headerReader;
            return this;
        }

        private GBPTreeBuilder with( Layout<MutableLong,MutableLong> layout )
        {
            this.layout = layout;
            return this;
        }

        private GBPTreeBuilder with( PageCache pageCache )
        {
            this.specificPageCache = pageCache;
            return this;
        }

        private GBPTreeBuilder readOnly()
        {
            this.readOnly = true;
            return this;
        }

        private GBPTree<MutableLong,MutableLong> build() throws IOException
        {
            PageCache pageCacheToUse;
            if ( specificPageCache == null )
            {
                if ( pageCache != null )
                {
                    pageCache.close();
                }
                pageCache = createPageCache( pageCachePageSize );
                pageCacheToUse = pageCache;
            }
            else
            {
                pageCacheToUse = specificPageCache;
            }

            return new GBPTree<>( pageCacheToUse, indexFile, layout, tentativePageSize, monitor, headerReader,
                    readOnly );
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

        public void reset()
        {
            count = 0;
        }

        public int count()
        {
            return count;
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
