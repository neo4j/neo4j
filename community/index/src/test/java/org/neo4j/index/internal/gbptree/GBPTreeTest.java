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

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.neo4j.cursor.RawCursor;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.index.internal.gbptree.GBPTree.Monitor;
import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.DelegatingPagedFile;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.test.Barrier;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.util.FeatureToggles;

import static java.lang.Long.MAX_VALUE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;
import static org.neo4j.index.internal.gbptree.ThrowingRunnable.throwing;
import static org.neo4j.io.pagecache.IOLimiter.unlimited;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.test.rule.PageCacheRule.config;

@SuppressWarnings( "EmptyTryBlock" )
public class GBPTreeTest
{
    private static final int DEFAULT_PAGE_SIZE = 256;

    private static final Layout<MutableLong,MutableLong> layout = longLayout().build();

    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule( config().withAccessChecks( true ) );
    private final RandomRule random = new RandomRule();

    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule ).around( random );

    private File indexFile;
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
        try ( GBPTree<MutableLong,MutableLong> ignored = index( 1024 ).build() )
        {   // Open/close is enough
        }

        // WHEN
        SimpleLongLayout otherLayout = longLayout().withCustomerNameAsMetaData( "Something else" ).build();
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
        SimpleLongLayout otherLayout = longLayout().withIdentifier( 123456 ).build();
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
        try ( GBPTree<MutableLong,MutableLong> ignored = index( 1024 ).build() )
        {   // Open/close is enough
        }

        // WHEN
        SimpleLongLayout otherLayout = longLayout().withMajorVersion( 123 ).build();
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
        SimpleLongLayout otherLayout = longLayout().withMinorVersion( 123 ).build();
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
        try ( GBPTree<MutableLong,MutableLong> ignored = index( pageSize ).build() )
        {   // Open/close is enough
        }

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> ignored = index( pageSize / 2 ).build() )
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
        try ( GBPTree<MutableLong,MutableLong> ignored = index( pageCachePageSize )
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
        try ( GBPTree<MutableLong,MutableLong> ignored = index( pageCachePageSize )
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
        try ( GBPTree<MutableLong,MutableLong> ignored = index( pageCachePageSize ).build() )
        {
            // Good
        }

        try ( GBPTree<MutableLong, MutableLong> ignored = index( pageCachePageSize / 2 )
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
        try ( GBPTree<MutableLong,MutableLong> index = index( pageSize )
                .withIndexPageSize( pageSize / 2 )
                .build() )
        {
            // Insert some data
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
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
        try ( GBPTree<MutableLong,MutableLong> index = index( pageSize ).build() )
        {
            MutableLong fromInclusive = new MutableLong( 0L );
            MutableLong toExclusive = new MutableLong( 200L );
            try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> seek = index.seek( fromInclusive, toExclusive ) )
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
    public void shouldFailWhenTryingToOpenWithDifferentFormatIdentifier() throws Exception
    {
        // GIVEN
        int pageSize = DEFAULT_PAGE_SIZE;
        PageCache pageCache = createPageCache( pageSize );
        GBPTreeBuilder<MutableLong,MutableLong> builder = index( pageCache );
        try ( GBPTree<MutableLong,MutableLong> ignored = builder.build() )
        {   // Open/close is enough
        }

        try
        {
            // WHEN
            builder.with( longLayout().withFixedSize( false ).build() ).build();
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

    /* Lifecycle tests */

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

            // Should be able to close old writer
            writer.close();
            // And open and closing a new one
            index.writer().close();
        }
    }

    @Test
    public void shouldNotAllowClosingWriterMultipleTimes() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            Writer<MutableLong,MutableLong> writer = index.writer();
            writer.put( new MutableLong( 0 ), new MutableLong( 1 ) );
            writer.close();

            try
            {
                // WHEN
                writer.close();
                fail( "Should have failed" );
            }
            catch ( IllegalStateException e )
            {
                // THEN
                assertThat( e.getMessage(), containsString( "already closed" ) );
            }
        }
    }

    @Test
    public void failureDuringInitializeWriterShouldNotFailNextInitialize() throws Exception
    {
        // GIVEN
        IOException no = new IOException( "No" );
        AtomicBoolean throwOnNextIO = new AtomicBoolean();
        PageCache controlledPageCache = pageCacheThatThrowExceptionWhenToldTo( no, throwOnNextIO );
        try ( GBPTree<MutableLong, MutableLong> index = index( controlledPageCache ).build() )
        {
            // WHEN
            assertTrue( throwOnNextIO.compareAndSet( false, true ) );
            try ( Writer<MutableLong,MutableLong> ignored = index.writer() )
            {
                fail( "Expected to throw" );
            }
            catch ( IOException e )
            {
                assertSame( no, e );
            }

            // THEN
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                writer.put( new MutableLong( 1 ), new MutableLong( 1 ) );
            }
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

    /* Header test */

    @Test
    public void shouldPutHeaderDataInCheckPoint() throws Exception
    {
        BiConsumer<GBPTree<MutableLong,MutableLong>,byte[]> beforeClose = ( index, expected ) ->
        {
            ThrowingRunnable throwingRunnable = () ->
                    index.checkpoint( unlimited(), cursor -> cursor.putBytes( expected ) );
            ThrowingRunnable.throwing( throwingRunnable ).run();
        };
        verifyHeaderDataAfterClose( beforeClose );
    }

    @Test
    public void shouldCarryOverHeaderDataInCheckPoint() throws Exception
    {
        BiConsumer<GBPTree<MutableLong,MutableLong>,byte[]> beforeClose = ( index, expected ) ->
        {
            ThrowingRunnable throwingRunnable = () ->
            {
                index.checkpoint( unlimited(), cursor -> cursor.putBytes( expected ) );
                insert( index, 0, 1 );

                // WHEN
                // Should carry over header data
                index.checkpoint( unlimited() );
            };
            ThrowingRunnable.throwing( throwingRunnable ).run();
        };
        verifyHeaderDataAfterClose( beforeClose );
    }

    @Test
    public void shouldCarryOverHeaderDataOnDirtyClose() throws Exception
    {
        BiConsumer<GBPTree<MutableLong,MutableLong>,byte[]> beforeClose = ( index, expected ) ->
        {
            ThrowingRunnable throwingRunnable = () ->
            {
                index.checkpoint( unlimited(), cursor -> cursor.putBytes( expected ) );
                insert( index, 0, 1 );

                // No checkpoint
            };
            ThrowingRunnable.throwing( throwingRunnable ).run();
        };
        verifyHeaderDataAfterClose( beforeClose );
    }

    @Test
    public void shouldReplaceHeaderDataInNextCheckPoint() throws Exception
    {
        BiConsumer<GBPTree<MutableLong,MutableLong>,byte[]> beforeClose = ( index, expected ) ->
        {
            ThrowingRunnable throwingRunnable = () ->
            {
                index.checkpoint( unlimited(), cursor -> cursor.putBytes( expected ) );
                ThreadLocalRandom.current().nextBytes( expected );
                index.checkpoint( unlimited(), cursor -> cursor.putBytes( expected ) );
            };
            ThrowingRunnable.throwing( throwingRunnable ).run();
        };

        verifyHeaderDataAfterClose( beforeClose );
    }

    @Test
    public void mustWriteHeaderOnInitialization() throws Exception
    {
        // GIVEN
        byte[] headerBytes = new byte[12];
        ThreadLocalRandom.current().nextBytes( headerBytes );
        Consumer<PageCursor> headerWriter = pc -> pc.putBytes( headerBytes );

        // WHEN
        PageCache pageCache = createPageCache( DEFAULT_PAGE_SIZE );
        try ( GBPTree<MutableLong,MutableLong> ignore = index( pageCache ).with( headerWriter ).build() )
        {
        }

        // THEN
        verifyHeader( pageCache, headerBytes );
    }

    @Test
    public void mustNotOverwriteHeaderOnExistingTree() throws Exception
    {
        // GIVEN
        byte[] expectedBytes = new byte[12];
        ThreadLocalRandom.current().nextBytes( expectedBytes );
        Consumer<PageCursor> headerWriter = pc -> pc.putBytes( expectedBytes );
        PageCache pageCache = createPageCache( DEFAULT_PAGE_SIZE );
        try ( GBPTree<MutableLong,MutableLong> ignore = index( pageCache ).with( headerWriter ).build() )
        {
        }

        // WHEN
        byte[] fraudulentBytes = new byte[12];
        do
        {
            ThreadLocalRandom.current().nextBytes( fraudulentBytes );
        }
        while ( Arrays.equals( expectedBytes, fraudulentBytes ) );

        try ( GBPTree<MutableLong,MutableLong> ignore = index( pageCache ).with( headerWriter ).build() )
        {
        }

        // THEN
        verifyHeader( pageCache, expectedBytes );
    }

    private void verifyHeaderDataAfterClose( BiConsumer<GBPTree<MutableLong,MutableLong>,byte[]> beforeClose )
            throws IOException
    {
        byte[] expectedHeader = new byte[12];
        ThreadLocalRandom.current().nextBytes( expectedHeader );
        PageCache pageCache = createPageCache( DEFAULT_PAGE_SIZE );

        // GIVEN
        try ( GBPTree<MutableLong,MutableLong> index = index( pageCache ).build() )
        {
            beforeClose.accept( index, expectedHeader );
        }

        verifyHeader( pageCache, expectedHeader );
    }

    @Test( timeout = 10_000 )
    public void writeHeaderInDirtyTreeMustNotDeadlock() throws Exception
    {
        PageCache pageCache = createPageCache( 256 );
        makeDirty( pageCache );

        Consumer<PageCursor> headerWriter = pc -> pc.putBytes( "failed".getBytes() );
        try ( GBPTree<MutableLong,MutableLong> index = index( pageCache ).with( RecoveryCleanupWorkCollector.ignore() ).build() )
        {
            index.checkpoint( IOLimiter.unlimited(), headerWriter );
        }

        verifyHeader( pageCache, "failed".getBytes() );
    }

    private void verifyHeader( PageCache pageCache, byte[] expectedHeader ) throws IOException
    {
        // WHEN
        byte[] readHeader = new byte[expectedHeader.length];
        AtomicInteger length = new AtomicInteger();
        Header.Reader headerReader = headerData ->
        {
            length.set( headerData.limit() );
            headerData.get( readHeader );
        };

        // Read as part of construction
        try ( GBPTree<MutableLong,MutableLong> ignored = index( pageCache ).with( headerReader ).build() )
        {   // open/close is enough to read header
        }

        // THEN
        assertEquals( expectedHeader.length, length.get() );
        assertArrayEquals( expectedHeader, readHeader );

        // WHEN
        // Read separate
        GBPTree.readHeader( pageCache, indexFile, headerReader );

        assertEquals( expectedHeader.length, length.get() );
        assertArrayEquals( expectedHeader, readHeader );
    }

    @Test
    public void readHeaderMustThrowIfFileDoesNotExist() throws Exception
    {
        // given
        File doesNotExist = new File( "Does not exist" );
        try
        {
            GBPTree.readHeader( createPageCache( DEFAULT_PAGE_SIZE ), doesNotExist, NO_HEADER_READER );
            fail( "Should have failed" );
        }
        catch ( NoSuchFileException e )
        {
            // good
        }
    }

    @Test
    public void openWithReadHeaderMustThrowMetadataMismatchExceptionIfFileIsEmpty() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfFileIsEmpty( pageCache -> GBPTree.readHeader( pageCache, indexFile, NO_HEADER_READER ) );
    }

    @Test
    public void openWithConstructorMustThrowMetadataMismatchExceptionIfFileIsEmpty() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfFileIsEmpty( pageCache -> index( pageCache ).build() );
    }

    private void openMustThrowMetadataMismatchExceptionIfFileIsEmpty( ThrowingConsumer<PageCache,IOException> opener ) throws Exception
    {
        // given an existing empty file
        PageCache pageCache = createPageCache( DEFAULT_PAGE_SIZE );
        pageCache.map( indexFile, pageCache.pageSize(), StandardOpenOption.CREATE ).close();

        // when
        try
        {
            opener.accept( pageCache );
            fail( "Should've thrown IOException" );
        }
        catch ( MetadataMismatchException e )
        {
            // then good
        }
    }

    @Test
    public void readHeaderMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing(
                pageCache -> GBPTree.readHeader( pageCache, indexFile, NO_HEADER_READER ) );
    }

    @Test
    public void constructorMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing( pageCache -> index( pageCache ).build() );
    }

    private void openMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing( ThrowingConsumer<PageCache,IOException> opener ) throws Exception
    {
        // given an existing index with only the first page in it
        PageCache pageCache = createPageCache( DEFAULT_PAGE_SIZE );
        try ( GBPTree<MutableLong,MutableLong> tree = index( pageCache ).build() )
        {   // Just for creating it
        }
        fs.truncate( indexFile, DEFAULT_PAGE_SIZE /*truncate right after the first page*/ );

        // when
        try
        {
            opener.accept( pageCache );
            fail( "Should've thrown IOException" );
        }
        catch ( MetadataMismatchException e )
        {
            // then good
        }
    }

    @Test
    public void readHeaderMustThrowIOExceptionIfStatePagesAreAllZeros() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros(
                pageCache -> GBPTree.readHeader( pageCache, indexFile, NO_HEADER_READER ) );
    }

    @Test
    public void constructorMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros( pageCache -> index( pageCache ).build() );
    }

    private void openMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros( ThrowingConsumer<PageCache,IOException> opener ) throws Exception
    {
        // given an existing index with all-zero state pages
        PageCache pageCache = createPageCache( DEFAULT_PAGE_SIZE );
        try ( GBPTree<MutableLong,MutableLong> tree = index( pageCache ).build() )
        {   // Just for creating it
        }
        fs.truncate( indexFile, DEFAULT_PAGE_SIZE /*truncate right after the first page*/ );
        try ( OutputStream out = fs.openAsOutputStream( indexFile, true ) )
        {
            byte[] allZeroPage = new byte[DEFAULT_PAGE_SIZE];
            out.write( allZeroPage ); // page A
            out.write( allZeroPage ); // page B
        }

        // when
        try
        {
            opener.accept( pageCache );
            fail( "Should've thrown IOException" );
        }
        catch ( MetadataMismatchException e )
        {
            // then good
        }
    }

    @Test
    public void readHeaderMustWorkWithOpenIndex() throws Exception
    {
        // GIVEN
        byte[] headerBytes = new byte[12];
        ThreadLocalRandom.current().nextBytes( headerBytes );
        Consumer<PageCursor> headerWriter = pc -> pc.putBytes( headerBytes );
        PageCache pageCache = createPageCache( DEFAULT_PAGE_SIZE );

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> ignore = index( pageCache ).with( headerWriter ).build() )
        {
            byte[] readHeader = new byte[headerBytes.length];
            AtomicInteger length = new AtomicInteger();
            Header.Reader headerReader = headerData ->
            {
                length.set( headerData.limit() );
                headerData.get( readHeader );
            };
            GBPTree.readHeader( pageCache, indexFile, headerReader );

            // THEN
            assertEquals( headerBytes.length, length.get() );
            assertArrayEquals( headerBytes, readHeader);
        }
    }

    /* Mutex tests */

    @Test( timeout = 5_000L )
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
            shouldWait( writerClose );
            monitor.barrier.release();

            writerClose.get();
            checkpoint.get();
        }
    }

    @Test( timeout = 5_000L )
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
            shouldWait( checkpoint );

            // THEN
            barrier.release();
            checkpoint.get();
            write.get();
        }
    }

    @Test( timeout = 50_000L )
    public void closeShouldLockOutWriter() throws Exception
    {
        // GIVEN
        AtomicBoolean enabled = new AtomicBoolean();
        Barrier.Control barrier = new Barrier.Control();
        PageCache pageCacheWithBarrier = pageCacheWithBarrierInClose( enabled, barrier );
        GBPTree<MutableLong,MutableLong> index = index( pageCacheWithBarrier ).build();
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

        shouldWait( write );
        barrier.release();

        // THEN
        write.get();
        close.get();
        assertTrue( "Writer should not be able to acquired after close",
                writerError.get() instanceof FileIsNotMappedException );
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

    @Test( timeout = 5_000L )
    public void writerShouldLockOutClose() throws Exception
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
        shouldWait( close );

        // THEN
        barrier.release();
        close.get();
        write.get();
    }

    @Test
    public void dirtyIndexIsNotCleanOnNextStartWithoutRecovery() throws IOException
    {
        makeDirty();

        try ( GBPTree<MutableLong, MutableLong> index = index().with( RecoveryCleanupWorkCollector.ignore() ).build() )
        {
            assertTrue( index.wasDirtyOnStartup() );
        }
    }

    @Test
    public void correctlyShutdownIndexIsClean() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                writer.put( new MutableLong( 1L ), new MutableLong( 2L ) );
            }
            index.checkpoint( IOLimiter.unlimited() );
        }
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            assertFalse( index.wasDirtyOnStartup() );
        }
    }

    @Test( timeout = 5_000L )
    public void cleanJobShouldLockOutCheckpoint() throws Exception
    {
        // GIVEN
        makeDirty();

        RecoveryCleanupWorkCollector cleanupWork = new ControlledRecoveryCleanupWorkCollector();
        CleanJobControlledMonitor monitor = new CleanJobControlledMonitor();
        try ( GBPTree<MutableLong,MutableLong> index = index().with( monitor ).with( cleanupWork ).build() )
        {
            // WHEN cleanup not finished
            Future<?> cleanup = executor.submit( throwing( cleanupWork::start ) );
            monitor.barrier.awaitUninterruptibly();
            index.writer().close();

            // THEN
            Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( IOLimiter.unlimited() ) ) );
            shouldWait( checkpoint );

            monitor.barrier.release();
            cleanup.get();
            checkpoint.get();
        }
    }

    @Test( timeout = 5_000L )
    public void cleanJobShouldLockOutCheckpointOnNoUpdate() throws Exception
    {
        // GIVEN
        makeDirty();

        RecoveryCleanupWorkCollector cleanupWork = new ControlledRecoveryCleanupWorkCollector();
        CleanJobControlledMonitor monitor = new CleanJobControlledMonitor();
        try ( GBPTree<MutableLong,MutableLong> index = index().with( monitor ).with( cleanupWork ).build() )
        {
            // WHEN cleanup not finished
            Future<?> cleanup = executor.submit( throwing( cleanupWork::start ) );
            monitor.barrier.awaitUninterruptibly();

            // THEN
            Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( IOLimiter.unlimited() ) ) );
            shouldWait( checkpoint );

            monitor.barrier.release();
            cleanup.get();
            checkpoint.get();
        }
    }

    @Test( timeout = 5_000L )
    public void cleanJobShouldNotLockOutClose() throws Exception
    {
        // GIVEN
        makeDirty();

        RecoveryCleanupWorkCollector cleanupWork = new ControlledRecoveryCleanupWorkCollector();
        CleanJobControlledMonitor monitor = new CleanJobControlledMonitor();
        GBPTree<MutableLong,MutableLong> index = index().with( monitor ).with( cleanupWork ).build();

        // WHEN
        // Cleanup not finished
        Future<?> cleanup = executor.submit( throwing( cleanupWork::start ) );
        monitor.barrier.awaitUninterruptibly();

        // THEN
        Future<?> close = executor.submit( throwing( index::close ) );
        close.get();

        monitor.barrier.release();
        cleanup.get();
    }

    @Test( timeout = 5_000L )
    public void cleanJobShouldNotLockOutWriter() throws Exception
    {
        // GIVEN
        makeDirty();

        RecoveryCleanupWorkCollector cleanupWork = new ControlledRecoveryCleanupWorkCollector();
        CleanJobControlledMonitor monitor = new CleanJobControlledMonitor();
        try ( GBPTree<MutableLong,MutableLong> index = index().with( monitor ).with( cleanupWork ).build() )
        {
            // WHEN
            // Cleanup not finished
            Future<?> cleanup = executor.submit( throwing( cleanupWork::start ) );
            monitor.barrier.awaitUninterruptibly();

            // THEN
            Future<?> writer = executor.submit( throwing( () -> index.writer().close() ) );
            writer.get();

            monitor.barrier.release();
            cleanup.get();
        }
    }

    @Test
    public void writerShouldNotLockOutCleanJob() throws Exception
    {
        // GIVEN
        makeDirty();

        RecoveryCleanupWorkCollector cleanupWork = new ControlledRecoveryCleanupWorkCollector();
        try ( GBPTree<MutableLong,MutableLong> index = index().with( cleanupWork ).build() )
        {
            // WHEN
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                // THEN
                Future<?> cleanup = executor.submit( throwing( cleanupWork::start ) );
                // Move writer to let cleaner pass
                writer.put( new MutableLong( 1 ), new MutableLong( 1 ) );
                cleanup.get();
            }
        }
    }

    /* Cleaner test */

    @Test
    public void cleanerShouldDieSilentlyOnClose() throws Throwable
    {
        // GIVEN
        makeDirty();

        AtomicBoolean blockOnNextIO = new AtomicBoolean();
        Barrier.Control control = new Barrier.Control();
        PageCache pageCache = pageCacheThatBlockWhenToldTo( control, blockOnNextIO );
        ControlledRecoveryCleanupWorkCollector collector = new ControlledRecoveryCleanupWorkCollector();
        collector.init();

        Future<List<CleanupJob>> cleanJob;
        try ( GBPTree<MutableLong, MutableLong> index = index( pageCache ).with( collector ).build() )
        {
            blockOnNextIO.set( true );
            cleanJob = executor.submit( startAndReturnStartedJobs( collector ) );

            // WHEN
            // ... cleaner is still alive
            control.await();

            // ... close
        }

        // THEN
        control.release();
        assertFailedDueToUnmappedFile( cleanJob );
    }

    @Test
    public void treeMustBeDirtyAfterCleanerDiedOnClose() throws Throwable
    {
        // GIVEN
        makeDirty();

        AtomicBoolean blockOnNextIO = new AtomicBoolean();
        Barrier.Control control = new Barrier.Control();
        PageCache pageCache = pageCacheThatBlockWhenToldTo( control, blockOnNextIO );
        ControlledRecoveryCleanupWorkCollector collector = new ControlledRecoveryCleanupWorkCollector();
        collector.init();

        Future<List<CleanupJob>> cleanJob;
        try ( GBPTree<MutableLong,MutableLong> index = index( pageCache ).with( collector ).build() )
        {
            blockOnNextIO.set( true );
            cleanJob = executor.submit( startAndReturnStartedJobs( collector ) );

            // WHEN
            // ... cleaner is still alive
            control.await();

            // ... close
        }

        // THEN
        control.release();
        assertFailedDueToUnmappedFile( cleanJob );

        MonitorDirty monitor = new MonitorDirty();
        try ( GBPTree<MutableLong,MutableLong> index = index().with( monitor ).build() )
        {
            assertFalse( monitor.cleanOnStart() );
        }
    }

    private Callable<List<CleanupJob>> startAndReturnStartedJobs(
            ControlledRecoveryCleanupWorkCollector collector )
    {
        return () ->
        {
            try
            {
                collector.start();
            }
            catch ( Throwable throwable )
            {
                throw new RuntimeException( throwable );
            }
            return collector.allStartedJobs();
        };
    }

    private void assertFailedDueToUnmappedFile( Future<List<CleanupJob>> cleanJob )
            throws InterruptedException, ExecutionException
    {
        for ( CleanupJob job : cleanJob.get() )
        {
            assertTrue( job.hasFailed() );
            assertThat( job.getCause().getMessage(),
                    allOf( containsString( "File" ), containsString( "unmapped" ) ) );
        }
    }

    @Test
    public void checkpointMustRecognizeFailedCleaning() throws Exception
    {
        // given
        makeDirty();
        RuntimeException cleanupException = new RuntimeException( "Fail cleaning job" );
        CleanJobControlledMonitor cleanupMonitor = new CleanJobControlledMonitor()
        {
            @Override
            public void cleanupFinished( long numberOfPagesVisited, long numberOfCleanedCrashPointers,
                    long durationMillis )
            {
                super.cleanupFinished( numberOfPagesVisited, numberOfCleanedCrashPointers, durationMillis );
                throw cleanupException;
            }
        };
        ControlledRecoveryCleanupWorkCollector collector = new ControlledRecoveryCleanupWorkCollector();

        // when
        try ( GBPTree<MutableLong,MutableLong> index = index()
                .with( cleanupMonitor )
                .with( collector )
                .build() )
        {
            index.writer().close(); // Changes since last checkpoint

            Future<?> cleanup = executor.submit( throwing( collector::start ) );
            shouldWait( cleanup );

            Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( IOLimiter.unlimited() ) ) );
            shouldWait( checkpoint );

            cleanupMonitor.barrier.release();
            cleanup.get();

            // then
            try
            {
                checkpoint.get();
                fail( "Expected checkpoint to fail because of failed cleaning job" );
            }
            catch ( ExecutionException e )
            {
                assertThat( e.getMessage(), allOf( containsString( "cleaning" ), containsString( "failed" ) ) );
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
    public void shouldNotCheckpointOnClose() throws Exception
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

    @Test
    public void shouldCheckpointEvenIfNoChanges() throws Exception
    {
        // GIVEN
        CheckpointCounter checkpointCounter = new CheckpointCounter();

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index = index().with( checkpointCounter ).build() )
        {
            checkpointCounter.reset();
            index.checkpoint( unlimited() );

            // THEN
            assertEquals( 1, checkpointCounter.count() );
        }
    }

    @Test
    public void mustNotSeeUpdatesThatWasNotCheckpointed() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong, MutableLong> index = index().build() )
        {
            insert( index, 0, 1 );

            // WHEN
            // No checkpoint before close
        }

        // THEN
        try ( GBPTree<MutableLong, MutableLong> index = index().build() )
        {
            MutableLong from = new MutableLong( Long.MIN_VALUE );
            MutableLong to = new MutableLong( MAX_VALUE );
            try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> seek = index.seek( from, to ) )
            {
                assertFalse( seek.next() );
            }
        }
    }

    @Test
    public void mustSeeUpdatesThatWasCheckpointed() throws Exception
    {
        // GIVEN
        int key = 1;
        int value = 2;
        try ( GBPTree<MutableLong, MutableLong> index = index().build() )
        {
            insert( index, key, value );

            // WHEN
            index.checkpoint( unlimited() );
        }

        // THEN
        try ( GBPTree<MutableLong, MutableLong> index = index().build() )
        {
            MutableLong from = new MutableLong( Long.MIN_VALUE );
            MutableLong to = new MutableLong( MAX_VALUE );
            try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> seek = index.seek( from, to ) )
            {
                assertTrue( seek.next() );
                assertEquals( key, seek.get().key().longValue() );
                assertEquals( value, seek.get().value().longValue() );
            }
        }
    }

    @Test
    public void mustBumpUnstableGenerationOnOpen() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong, MutableLong> index = index().build() )
        {
            insert( index, 0, 1 );

            // no checkpoint
        }

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        try ( GBPTree<MutableLong, MutableLong> ignore = index().with( monitor ).build() )
        {
        }

        // THEN
        assertTrue( "Expected monitor to get recovery complete message", monitor.cleanupFinished );
        assertEquals( "Expected index to have exactly 1 crash pointer from root to successor of root",
                1, monitor.numberOfCleanedCrashPointers );
        assertEquals( "Expected index to have exactly 2 tree node pages, root and successor of root",
                2, monitor.numberOfPagesVisited ); // Root and successor of root
    }

    /* Dirty state tests */

    @Test
    public void indexMustBeCleanOnFirstInitialization() throws Exception
    {
        // GIVEN
        assertFalse( fs.get().fileExists( indexFile ) );
        MonitorDirty monitorDirty = new MonitorDirty();

        // WHEN
        try ( GBPTree<MutableLong, MutableLong> ignored = index().with( monitorDirty ).build() )
        {
        }

        // THEN
        assertTrue( "Expected to be clean on start", monitorDirty.cleanOnStart() );
    }

    @Test
    public void indexMustBeCleanWhenClosedWithoutAnyChanges() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong, MutableLong> ignored = index().build() )
        {
        }

        // WHEN
        MonitorDirty monitorDirty = new MonitorDirty();
        try ( GBPTree<MutableLong, MutableLong> ignored = index().with( monitorDirty ).build() )
        {
        }

        // THEN
        assertTrue( "Expected to be clean on start after close with no changes", monitorDirty.cleanOnStart() );
    }

    @Test
    public void indexMustBeCleanWhenClosedAfterCheckpoint() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong, MutableLong> index = index().build() )
        {
            insert( index, 0, 1 );

            index.checkpoint( unlimited() );
        }

        // WHEN
        MonitorDirty monitorDirty = new MonitorDirty();
        try ( GBPTree<MutableLong, MutableLong> ignored = index().with( monitorDirty ).build() )
        {
        }

        // THEN
        assertTrue( "Expected to be clean on start after close with checkpoint", monitorDirty.cleanOnStart() );
    }

    @Test
    public void indexMustBeDirtyWhenClosedWithChangesSinceLastCheckpoint() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong, MutableLong> index = index().build() )
        {
            insert( index, 0, 1 );

            // no checkpoint
        }

        // WHEN
        MonitorDirty monitorDirty = new MonitorDirty();
        try ( GBPTree<MutableLong, MutableLong> ignored = index().with( monitorDirty ).build() )
        {
        }

        // THEN
        assertFalse( "Expected to be dirty on start after close without checkpoint",
                monitorDirty.cleanOnStart() );
    }

    @Test
    public void indexMustBeDirtyWhenCrashedWithChangesSinceLastCheckpoint() throws Exception
    {
        // GIVEN
        try ( EphemeralFileSystemAbstraction ephemeralFs = new EphemeralFileSystemAbstraction() )
        {
            ephemeralFs.mkdirs( indexFile.getParentFile() );
            PageCache pageCache = pageCacheRule.getPageCache( ephemeralFs );
            EphemeralFileSystemAbstraction snapshot;
            try ( GBPTree<MutableLong, MutableLong> index = index( pageCache ).build() )
            {
                insert( index, 0, 1 );

                // WHEN
                // crash
                snapshot = ephemeralFs.snapshot();
            }
            pageCache.close();

            // THEN
            MonitorDirty monitorDirty = new MonitorDirty();
            pageCache = pageCacheRule.getPageCache( snapshot );
            try ( GBPTree<MutableLong, MutableLong> ignored = index( pageCache ).with( monitorDirty ).build() )
            {
            }
            assertFalse( "Expected to be dirty on start after crash",
                    monitorDirty.cleanOnStart() );
        }
    }

    @Test
    public void cleanCrashPointersMustTriggerOnDirtyStart() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong, MutableLong> index = index().build() )
        {
            insert( index, 0, 1 );

            // No checkpoint
        }

        // WHEN
        MonitorCleanup monitor = new MonitorCleanup();
        try ( GBPTree<MutableLong, MutableLong> ignored = index().with( monitor ).build() )
        {
            // THEN
            assertTrue( "Expected cleanup to be called when starting on dirty tree", monitor.cleanupCalled() );
        }
    }

    @Test
    public void cleanCrashPointersMustNotTriggerOnCleanStart() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong, MutableLong> index = index().build() )
        {
            insert( index, 0, 1 );

            index.checkpoint( IOLimiter.unlimited() );
        }

        // WHEN
        MonitorCleanup monitor = new MonitorCleanup();
        try ( GBPTree<MutableLong, MutableLong> ignored = index().with( monitor ).build() )
        {
            // THEN
            assertFalse( "Expected cleanup not to be called when starting on clean tree", monitor.cleanupCalled() );
        }
    }

    /* TreeState has outdated root */

    @Test
    public void shouldThrowIfTreeStatePointToRootWithValidSuccessor() throws Exception
    {
        // GIVEN
        int pageSize = DEFAULT_PAGE_SIZE;
        try ( PageCache specificPageCache = createPageCache( pageSize ) )
        {
            try ( GBPTree<MutableLong,MutableLong> ignore = index( specificPageCache ).build() )
            {
            }

            // a tree state pointing to root with valid successor
            try ( PagedFile pagedFile = specificPageCache.map( indexFile, specificPageCache.pageSize() );
                  PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                Pair<TreeState,TreeState> treeStates =
                        TreeStatePair.readStatePages( cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B );
                TreeState newestState = TreeStatePair.selectNewestValidState( treeStates );
                long rootId = newestState.rootId();
                long stableGeneration = newestState.stableGeneration();
                long unstableGeneration = newestState.unstableGeneration();

                TreeNode.goTo( cursor, "root", rootId );
                TreeNode.setSuccessor( cursor, 42, stableGeneration + 1, unstableGeneration + 1 );
            }

            // WHEN
            try ( GBPTree<MutableLong,MutableLong> index = index( specificPageCache ).build() )
            {
                try ( Writer<MutableLong, MutableLong> ignored = index.writer() )
                {
                    fail( "Expected to throw because root pointed to by tree state should have a valid successor." );
                }
            }
            catch ( TreeInconsistencyException e )
            {
                assertThat( e.getMessage(), containsString( PointerChecking.WRITER_TRAVERSE_OLD_STATE_MESSAGE ) );
            }
        }
    }

    /* IO failure on close */

    @Test
    public void mustRetryCloseIfFailure() throws Exception
    {
        // GIVEN
        AtomicBoolean throwOnNext = new AtomicBoolean();
        IOException exception = new IOException( "My failure" );
        PageCache pageCache = pageCacheThatThrowExceptionWhenToldTo( exception, throwOnNext );
        try ( GBPTree<MutableLong, MutableLong> index = index( pageCache ).build() )
        {
            // WHEN
            throwOnNext.set( true );
        }
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnCallingNextAfterClose() throws Exception
    {
        // given
        try ( GBPTree<MutableLong,MutableLong> tree = index().build() )
        {
            try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
            {
                MutableLong value = new MutableLong();
                for ( int i = 0; i < 10; i++ )
                {
                    value.setValue( i );
                    writer.put( value, value );
                }
            }

            RawCursor<Hit<MutableLong,MutableLong>,IOException> seek =
                    tree.seek( new MutableLong( 0 ), new MutableLong( MAX_VALUE ) );
            assertTrue( seek.next() );
            assertTrue( seek.next() );
            seek.close();

            for ( int i = 0; i < 2; i++ )
            {
                try
                {
                    // when
                    seek.next();
                    fail( "Should have failed" );
                }
                catch ( IllegalStateException e )
                {
                    // then good
                }
            }
        }
    }

    /* Inconsistency tests */

    @Test( timeout = 60_000L )
    public void mustThrowIfStuckInInfiniteRootCatchup() throws IOException
    {
        // Create a tree with root and two children.
        // Corrupt one of the children and make it look like a freelist node.
        // This will cause seekCursor to start from root in an attempt, believing it went wrong because of concurrent updates.
        // When seekCursor comes back to the same corrupt child again and again it should eventually escape from that loop
        // with an exception.

        List<Long> trace = new ArrayList<>();
        PageCache pageCache = pageCacheWithTrace( trace );

        // Build a tree with root and two children.
        try ( GBPTree<MutableLong,MutableLong> tree = index( pageCache ).build() )
        {
            // Insert data until we have a split in root
            treeWithRootSplit( trace, tree );
            long corruptChild = trace.get( 1 );

            // Corrupt the child
            corruptTheChild( pageCache, corruptChild );

            // when seek end up in this corrupt child we should eventually fail with a tree inconsistency exception
            try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> seek = tree.seek( new MutableLong( 0 ), new MutableLong( 0 ) ) )
            {
                seek.next();
                fail( "Expected to throw" );
            }
            catch ( TreeInconsistencyException e )
            {
                // then good
                assertThat( e.getMessage(), CoreMatchers.containsString(
                        "Index traversal aborted due to being stuck in infinite loop. This is most likely caused by an inconsistency in the index. " +
                                "Loop occurred when restarting search from root from page " + corruptChild + "." ) );
            }
        }
    }

    @Test( timeout = 5_000L )
    public void mustThrowIfStuckInInfiniteRootCatchupMultipleConcurrentSeekers() throws IOException, InterruptedException
    {
        FeatureToggles.set( TripCountingRootCatchup.class, TripCountingRootCatchup.MAX_TRIP_COUNT_NAME, 10000 );
        try
        {
            List<Long> trace = new ArrayList<>();
            PageCache pageCache = pageCacheWithTrace( trace );

            // Build a tree with root and two children.
            try ( GBPTree<MutableLong,MutableLong> tree = index( pageCache ).build() )
            {
                // Insert data until we have a split in root
                treeWithRootSplit( trace, tree );
                long leftChild = trace.get( 1 );
                long rightChild = trace.get( 2 );

                // Corrupt the child
                corruptTheChild( pageCache, leftChild );
                corruptTheChild( pageCache, rightChild );

                // When seek end up in this corrupt child we should eventually fail with a tree inconsistency exception
                // even if we have multiple seeker that traverse different part of the tree and both get stuck in start from root loop.
                ExecutorService executor = Executors.newFixedThreadPool( 2 );
                CountDownLatch go = new CountDownLatch( 2 );
                Future<Object> execute1 = executor.submit( () ->
                {
                    go.countDown();
                    go.await();
                    try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> seek = tree.seek( new MutableLong( 0 ), new MutableLong( 0 ) ) )
                    {
                        seek.next();
                    }
                    return null;
                } );

                Future<Object> execute2 = executor.submit( () ->
                {
                    go.countDown();
                    go.await();
                    try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> seek = tree.seek( new MutableLong( MAX_VALUE ), new MutableLong( MAX_VALUE ) ) )
                    {
                        seek.next();
                    }
                    return null;
                } );

                assertFutureFailsWithTreeInconsistencyException( execute1 );
                assertFutureFailsWithTreeInconsistencyException( execute2 );
            }
        }
        finally
        {
            FeatureToggles.clear( TripCountingRootCatchup.class, TripCountingRootCatchup.MAX_TRIP_COUNT_NAME );
        }
    }

    private void assertFutureFailsWithTreeInconsistencyException( Future<Object> execute1 ) throws InterruptedException
    {
        try
        {
            execute1.get();
            fail( "Expected to fail" );
        }
        catch ( ExecutionException e )
        {
            Throwable cause = e.getCause();
            if ( !(cause instanceof TreeInconsistencyException) )
            {
                fail( "Expected cause to be " + TreeInconsistencyException.class + " but was " + Exceptions.stringify( cause ) );
            }
        }
    }

    private void corruptTheChild( PageCache pageCache, long corruptChild ) throws IOException
    {
        try ( PagedFile pagedFile = pageCache.map( indexFile, DEFAULT_PAGE_SIZE );
              PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
        {
            assertTrue( cursor.next( corruptChild ) );
            assertTrue( TreeNode.isLeaf( cursor ) );

            // Make child look like freelist node
            cursor.putByte( TreeNode.BYTE_POS_NODE_TYPE, TreeNode.NODE_TYPE_FREE_LIST_NODE );
        }
    }

    /**
     * When split is done, trace contain:
     * trace.get( 0 ) - root
     * trace.get( 1 ) - leftChild
     * trace.get( 2 ) - rightChild
     */
    private void treeWithRootSplit( List<Long> trace, GBPTree<MutableLong,MutableLong> tree ) throws IOException
    {
        long count = 0;
        do
        {
            try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
            {
                writer.put( new MutableLong( count ), new MutableLong( count ) );
                count++;
            }
            trace.clear();
            try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> seek = tree.seek( new MutableLong( 0 ), new MutableLong( 0 ) ) )
            {
                seek.next();
            }
        }
        while ( trace.size() <= 1 );

        trace.clear();
        try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> seek = tree.seek( new MutableLong( 0 ), new MutableLong( MAX_VALUE ) ) )
        {
            //noinspection StatementWithEmptyBody
            while ( seek.next() )
            {
            }
        }
    }

    private PageCache pageCacheWithTrace( List<Long> trace )
    {
        // A page cache tracer that we can use to see when tree has seen enough updates and to figure out on which page the child sits.Trace( trace );
        PageCursorTracer pageCursorTracer = new DefaultPageCursorTracer()
        {
            @Override
            public PinEvent beginPin( boolean writeLock, long filePageId, PageSwapper swapper )
            {
                trace.add( filePageId );
                return super.beginPin( writeLock, filePageId, swapper );
            }
        };
        PageCursorTracerSupplier pageCursorTracerSupplier = () -> pageCursorTracer;
        return createPageCache( DEFAULT_PAGE_SIZE, pageCursorTracerSupplier );
    }

    private static class ControlledRecoveryCleanupWorkCollector extends RecoveryCleanupWorkCollector
    {
        Queue<CleanupJob> jobs = new LinkedList<>();
        List<CleanupJob> startedJobs = new LinkedList<>();

        @Override
        public void start()
        {
            executeWithExecutor( executor ->
            {
                CleanupJob job;
                while ( (job = jobs.poll()) != null )
                {
                    try
                    {
                        job.run( executor );
                        startedJobs.add( job );
                    }
                    finally
                    {
                        job.close();
                    }
                }
            } );
        }

        @Override
        public void add( CleanupJob job )
        {
            jobs.add( job );
        }

        List<CleanupJob> allStartedJobs()
        {
            return startedJobs;
        }
    }

    private PageCache pageCacheThatThrowExceptionWhenToldTo( final IOException e, final AtomicBoolean throwOnNextIO )
    {
        return new DelegatingPageCache( createPageCache( DEFAULT_PAGE_SIZE ) )
        {
            @Override
            public PagedFile map( File file, int pageSize, OpenOption... openOptions ) throws IOException
            {
                return new DelegatingPagedFile( super.map( file, pageSize, openOptions ) )
                {
                    @Override
                    public PageCursor io( long pageId, int pf_flags ) throws IOException
                    {
                        maybeThrow();
                        return super.io( pageId, pf_flags );
                    }

                    @Override
                    public void flushAndForce( IOLimiter limiter ) throws IOException
                    {
                        maybeThrow();
                        super.flushAndForce( limiter );
                    }

                    private void maybeThrow() throws IOException
                    {
                        if ( throwOnNextIO.get() )
                        {
                            throwOnNextIO.set( false );
                            assert e != null;
                            throw e;
                        }
                    }
                };
            }
        };
    }

    private PageCache pageCacheThatBlockWhenToldTo( final Barrier barrier, final AtomicBoolean blockOnNextIO )
    {
        return new DelegatingPageCache( createPageCache( DEFAULT_PAGE_SIZE ) )
        {
            @Override
            public PagedFile map( File file, int pageSize, OpenOption... openOptions ) throws IOException
            {
                return new DelegatingPagedFile( super.map( file, pageSize, openOptions ) )
                {
                    @Override
                    public PageCursor io( long pageId, int pf_flags ) throws IOException
                    {
                        maybeBlock();
                        return super.io( pageId, pf_flags );
                    }

                    private void maybeBlock()
                    {
                        if ( blockOnNextIO.get() )
                        {
                            barrier.reached();
                        }
                    }
                };
            }
        };
    }

    private void makeDirty() throws IOException
    {
        makeDirty( createPageCache( DEFAULT_PAGE_SIZE ) );
    }

    private void makeDirty( PageCache pageCache ) throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index( pageCache ).build() )
        {
            // Make dirty
            index.writer().close();
        }
    }

    private void insert( GBPTree<MutableLong,MutableLong> index, long key, long value ) throws IOException
    {
        try ( Writer<MutableLong, MutableLong> writer = index.writer() )
        {
            writer.put( new MutableLong( key ), new MutableLong( value ) );
        }
    }

    private void shouldWait( Future<?> future ) throws InterruptedException, ExecutionException
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

    private PageCache createPageCache( int pageSize, PageCursorTracerSupplier pageCursorTracerSupplier )
    {
        return pageCacheRule.getPageCache( fs.get(), config().withPageSize( pageSize ).withCursorTracerSupplier( pageCursorTracerSupplier ) );
    }

    private static class CleanJobControlledMonitor extends Monitor.Adaptor
    {
        private final Barrier.Control barrier = new Barrier.Control();

        @Override
        public void cleanupFinished( long numberOfPagesVisited, long numberOfCleanedCrashPointers, long durationMillis )
        {
            barrier.reached();
        }
    }

    // The most common tree builds in this test
    private GBPTreeBuilder<MutableLong,MutableLong> index()
    {
        return index( DEFAULT_PAGE_SIZE );
    }

    private GBPTreeBuilder<MutableLong,MutableLong> index( int pageSize )
    {
        return index( createPageCache( pageSize ) );
    }

    private GBPTreeBuilder<MutableLong,MutableLong> index( PageCache pageCache )
    {
        return new GBPTreeBuilder<>( pageCache, indexFile, layout );
    }

    private static class CheckpointControlledMonitor extends Monitor.Adaptor
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

    private static class CheckpointCounter extends Monitor.Adaptor
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

    private static class MonitorDirty extends Monitor.Adaptor
    {
        private boolean called;
        private boolean cleanOnStart;

        @Override
        public void startupState( boolean clean )
        {
            if ( called )
            {
                throw new IllegalStateException( "State has already been set. Can't set it again." );
            }
            called = true;
            cleanOnStart = clean;
        }

        boolean cleanOnStart()
        {
            if ( !called )
            {
                throw new IllegalStateException( "State has not been set" );
            }
            return cleanOnStart;
        }
    }

    private static class MonitorCleanup extends Monitor.Adaptor
    {
        private boolean cleanupCalled;

        @Override
        public void cleanupFinished( long numberOfPagesVisited, long numberOfCleanedCrashPointers, long durationMillis )
        {
            cleanupCalled = true;
        }

        boolean cleanupCalled()
        {
            return cleanupCalled;
        }
    }
}
