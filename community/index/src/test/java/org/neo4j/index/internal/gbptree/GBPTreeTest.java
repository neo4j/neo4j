/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

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
import org.neo4j.index.internal.gbptree.GBPTree.Monitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.DelegatingPagedFile;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.test.Barrier;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.rule.PageCacheConfig;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.time.Duration.ofSeconds;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;
import static org.neo4j.index.internal.gbptree.ThrowingRunnable.throwing;
import static org.neo4j.io.pagecache.IOLimiter.UNLIMITED;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.test.rule.PageCacheConfig.config;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class, RandomExtension.class} )
class GBPTreeTest
{
    private static final int DEFAULT_PAGE_SIZE = 256;
    private static final Layout<MutableLong,MutableLong> layout = longLayout().build();

    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension( config().withAccessChecks( true ) );
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private RandomRule random;

    private File indexFile;
    private ExecutorService executor;

    @BeforeEach
    void setUp()
    {
        executor = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
        indexFile = testDirectory.file( "index" );
    }

    @AfterEach
    void teardown()
    {
        executor.shutdown();
    }

    /* Meta and state page tests */

    @Test
    void shouldReadWrittenMetaData() throws Exception
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
    void shouldFailToOpenOnDifferentMetaData() throws Exception
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
    void shouldFailToOpenOnDifferentLayout() throws Exception
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
    void shouldFailToOpenOnDifferentMajorVersion() throws Exception
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
    void shouldFailToOpenOnDifferentMinorVersion() throws Exception
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
    void shouldFailOnOpenWithDifferentPageSize() throws Exception
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
    void shouldFailOnStartingWithPageSizeLargerThanThatOfPageCache() throws Exception
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
    void shouldMapIndexFileWithProvidedPageSizeIfLessThanOrEqualToCachePageSize() throws Exception
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
    void shouldFailWhenTryingToRemapWithPageSizeLargerThanCachePageSize() throws Exception
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
    void shouldRemapFileIfMappedWithPageSizeLargerThanCreationSize() throws Exception
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
            index.checkpoint( UNLIMITED );
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
    void shouldFailWhenTryingToOpenWithDifferentFormatIdentifier() throws Exception
    {
        // GIVEN
        PageCache pageCache = createPageCache( DEFAULT_PAGE_SIZE );
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
    void shouldReturnNoResultsOnEmptyIndex() throws Exception
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
    void shouldNotBeAbleToAcquireModifierTwice() throws Exception
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
    void shouldNotAllowClosingWriterMultipleTimes() throws Exception
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
    void failureDuringInitializeWriterShouldNotFailNextInitialize() throws Exception
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
    void shouldAllowClosingTreeMultipleTimes() throws Exception
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
    void shouldPutHeaderDataInCheckPoint() throws Exception
    {
        BiConsumer<GBPTree<MutableLong,MutableLong>,byte[]> beforeClose = ( index, expected ) ->
        {
            ThrowingRunnable throwingRunnable = () ->
                    index.checkpoint( UNLIMITED, cursor -> cursor.putBytes( expected ) );
            ThrowingRunnable.throwing( throwingRunnable ).run();
        };
        verifyHeaderDataAfterClose( beforeClose );
    }

    @Test
    void shouldCarryOverHeaderDataInCheckPoint() throws Exception
    {
        BiConsumer<GBPTree<MutableLong,MutableLong>,byte[]> beforeClose = ( index, expected ) ->
        {
            ThrowingRunnable throwingRunnable = () ->
            {
                index.checkpoint( UNLIMITED, cursor -> cursor.putBytes( expected ) );
                insert( index, 0, 1 );

                // WHEN
                // Should carry over header data
                index.checkpoint( UNLIMITED );
            };
            ThrowingRunnable.throwing( throwingRunnable ).run();
        };
        verifyHeaderDataAfterClose( beforeClose );
    }

    @Test
    void shouldCarryOverHeaderDataOnDirtyClose() throws Exception
    {
        BiConsumer<GBPTree<MutableLong,MutableLong>,byte[]> beforeClose = ( index, expected ) ->
        {
            ThrowingRunnable throwingRunnable = () ->
            {
                index.checkpoint( UNLIMITED, cursor -> cursor.putBytes( expected ) );
                insert( index, 0, 1 );

                // No checkpoint
            };
            ThrowingRunnable.throwing( throwingRunnable ).run();
        };
        verifyHeaderDataAfterClose( beforeClose );
    }

    @Test
    void shouldReplaceHeaderDataInNextCheckPoint() throws Exception
    {
        BiConsumer<GBPTree<MutableLong,MutableLong>,byte[]> beforeClose = ( index, expected ) ->
        {
            ThrowingRunnable throwingRunnable = () ->
            {
                index.checkpoint( UNLIMITED, cursor -> cursor.putBytes( expected ) );
                ThreadLocalRandom.current().nextBytes( expected );
                index.checkpoint( UNLIMITED, cursor -> cursor.putBytes( expected ) );
            };
            ThrowingRunnable.throwing( throwingRunnable ).run();
        };

        verifyHeaderDataAfterClose( beforeClose );
    }

    @Test
    void mustWriteHeaderOnInitialization() throws Exception
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
    void mustNotOverwriteHeaderOnExistingTree() throws Exception
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

    @Test
    void writeHeaderInDirtyTreeMustNotDeadlock() throws Exception
    {
        assertTimeoutPreemptively( ofSeconds( 10 ), () ->
        {
            PageCache pageCache = createPageCache( 256 );
            makeDirty( pageCache );

            Consumer<PageCursor> headerWriter = pc -> pc.putBytes( "failed".getBytes() );
            try ( GBPTree<MutableLong,MutableLong> index = index( pageCache ).with( RecoveryCleanupWorkCollector.ignore() ).build() )
            {
                index.checkpoint( UNLIMITED, headerWriter );
            }

            verifyHeader( pageCache, "failed".getBytes() );
        } );
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
    void readHeaderMustThrowIfFileDoesNotExist() throws Exception
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
    void openWithReadHeaderMustThrowMetadataMismatchExceptionIfFileIsEmpty() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfFileIsEmpty( pageCache -> GBPTree.readHeader( pageCache, indexFile, NO_HEADER_READER ) );
    }

    @Test
    void openWithConstructorMustThrowMetadataMismatchExceptionIfFileIsEmpty() throws Exception
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
    void readHeaderMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing(
                pageCache -> GBPTree.readHeader( pageCache, indexFile, NO_HEADER_READER ) );
    }

    @Test
    void constructorMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing( pageCache -> index( pageCache ).build() );
    }

    private void openMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing( ThrowingConsumer<PageCache,IOException> opener ) throws Exception
    {
        // given an existing index with only the first page in it
        PageCache pageCache = createPageCache( DEFAULT_PAGE_SIZE );
        try ( GBPTree<MutableLong,MutableLong> ignored = index( pageCache ).build() )
        {   // Just for creating it
        }
        fileSystem.truncate( indexFile, DEFAULT_PAGE_SIZE /*truncate right after the first page*/ );

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
    void readHeaderMustThrowIOExceptionIfStatePagesAreAllZeros() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros(
                pageCache -> GBPTree.readHeader( pageCache, indexFile, NO_HEADER_READER ) );
    }

    @Test
    void constructorMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros( pageCache -> index( pageCache ).build() );
    }

    private void openMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros( ThrowingConsumer<PageCache,IOException> opener ) throws Exception
    {
        // given an existing index with all-zero state pages
        PageCache pageCache = createPageCache( DEFAULT_PAGE_SIZE );
        try ( GBPTree<MutableLong,MutableLong> ignored = index( pageCache ).build() )
        {   // Just for creating it
        }
        fileSystem.truncate( indexFile, DEFAULT_PAGE_SIZE /*truncate right after the first page*/ );
        try ( OutputStream out = fileSystem.openAsOutputStream( indexFile, true ) )
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
    void readHeaderMustWorkWithOpenIndex() throws Exception
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

    @Test
    void checkPointShouldLockOutWriter() throws Exception
    {
        assertTimeoutPreemptively( ofSeconds( 5 ), () ->
        {
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
                Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( UNLIMITED ) ) );
                monitor.barrier.awaitUninterruptibly();
                // now we're in the smack middle of a checkpoint
                Future<?> writerClose = executor.submit( throwing( () -> index.writer().close() ) );

                // THEN
                shouldWait( writerClose );
                monitor.barrier.release();

                writerClose.get();
                checkpoint.get();
            }
        } );
    }

    @Test
    void checkPointShouldWaitForWriter() throws Exception
    {
        assertTimeoutPreemptively( ofSeconds( 5 ), () ->
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
                Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( UNLIMITED ) ) );
                shouldWait( checkpoint );

                // THEN
                barrier.release();
                checkpoint.get();
                write.get();
            }
        } );
    }

    @Test
    void closeShouldLockOutWriter() throws Exception
    {
        assertTimeoutPreemptively( ofSeconds( 50 ), () ->
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
            assertTrue( writerError.get() instanceof FileIsNotMappedException, "Writer should not be able to acquired after close" );
        } );
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
                    public void close()
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
    void writerShouldLockOutClose() throws Exception
    {
        assertTimeoutPreemptively( ofSeconds( 5 ), () ->
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
        } );
    }

    @Test
    void dirtyIndexIsNotCleanOnNextStartWithoutRecovery() throws IOException
    {
        makeDirty();

        try ( GBPTree<MutableLong, MutableLong> index = index().with( RecoveryCleanupWorkCollector.ignore() ).build() )
        {
            assertTrue( index.wasDirtyOnStartup() );
        }
    }

    @Test
    void correctlyShutdownIndexIsClean() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                writer.put( new MutableLong( 1L ), new MutableLong( 2L ) );
            }
            index.checkpoint( UNLIMITED );
        }
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            assertFalse( index.wasDirtyOnStartup() );
        }
    }

    @Test
    void cleanJobShouldLockOutCheckpoint() throws Exception
    {
        assertTimeoutPreemptively( ofSeconds( 5 ), () -> {
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
                Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( UNLIMITED ) ) );
                shouldWait( checkpoint );

                monitor.barrier.release();
                cleanup.get();
                checkpoint.get();
            }
        } );
    }

    @Test
    void cleanJobShouldLockOutCheckpointOnNoUpdate() throws Exception
    {
        assertTimeoutPreemptively( ofSeconds( 5 ), () -> {
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
                Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( UNLIMITED ) ) );
                shouldWait( checkpoint );

                monitor.barrier.release();
                cleanup.get();
                checkpoint.get();
            }
        } );
    }

    @Test
    void cleanJobShouldNotLockOutClose() throws Exception
    {
        assertTimeoutPreemptively( ofSeconds( 5 ), () -> {
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
        } );
    }

    @Test
    void cleanJobShouldNotLockOutWriter() throws Exception
    {
        assertTimeoutPreemptively( ofSeconds( 5 ), () -> {
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
        } );
    }

    @Test
    void writerShouldNotLockOutCleanJob() throws Exception
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
    void cleanerShouldDieSilentlyOnClose() throws Throwable
    {
        // GIVEN
        makeDirty();

        AtomicBoolean blockOnNextIO = new AtomicBoolean();
        Barrier.Control control = new Barrier.Control();
        PageCache pageCache = pageCacheThatBlockWhenToldTo( control, blockOnNextIO );
        ControlledRecoveryCleanupWorkCollector collector = new ControlledRecoveryCleanupWorkCollector();
        collector.init();

        Future<List<CleanupJob>> cleanJob;
        try ( GBPTree<MutableLong, MutableLong> ignored = index( pageCache ).with( collector ).build() )
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
    void treeMustBeDirtyAfterCleanerDiedOnClose() throws Throwable
    {
        // GIVEN
        makeDirty();

        AtomicBoolean blockOnNextIO = new AtomicBoolean();
        Barrier.Control control = new Barrier.Control();
        PageCache pageCache = pageCacheThatBlockWhenToldTo( control, blockOnNextIO );
        ControlledRecoveryCleanupWorkCollector collector = new ControlledRecoveryCleanupWorkCollector();
        collector.init();

        Future<List<CleanupJob>> cleanJob;
        try ( GBPTree<MutableLong,MutableLong> ignored = index( pageCache ).with( collector ).build() )
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
        try ( GBPTree<MutableLong,MutableLong> ignored = index().with( monitor ).build() )
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
    void checkpointMustRecognizeFailedCleaning() throws Exception
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

            Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( UNLIMITED ) ) );
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
    void shouldCheckpointAfterInitialCreation() throws Exception
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
    void shouldNotCheckpointOnClose() throws Exception
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
            index.checkpoint( UNLIMITED );
            assertEquals( 1, checkpointCounter.count() );
        }

        // THEN
        assertEquals( 1, checkpointCounter.count() );
    }

    @Test
    void shouldCheckpointEvenIfNoChanges() throws Exception
    {
        // GIVEN
        CheckpointCounter checkpointCounter = new CheckpointCounter();

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index = index().with( checkpointCounter ).build() )
        {
            checkpointCounter.reset();
            index.checkpoint( UNLIMITED );

            // THEN
            assertEquals( 1, checkpointCounter.count() );
        }
    }

    @Test
    void mustNotSeeUpdatesThatWasNotCheckpointed() throws Exception
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
            MutableLong to = new MutableLong( Long.MAX_VALUE );
            try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> seek = index.seek( from, to ) )
            {
                assertFalse( seek.next() );
            }
        }
    }

    @Test
    void mustSeeUpdatesThatWasCheckpointed() throws Exception
    {
        // GIVEN
        int key = 1;
        int value = 2;
        try ( GBPTree<MutableLong, MutableLong> index = index().build() )
        {
            insert( index, key, value );

            // WHEN
            index.checkpoint( UNLIMITED );
        }

        // THEN
        try ( GBPTree<MutableLong, MutableLong> index = index().build() )
        {
            MutableLong from = new MutableLong( Long.MIN_VALUE );
            MutableLong to = new MutableLong( Long.MAX_VALUE );
            try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> seek = index.seek( from, to ) )
            {
                assertTrue( seek.next() );
                assertEquals( key, seek.get().key().longValue() );
                assertEquals( value, seek.get().value().longValue() );
            }
        }
    }

    @Test
    void mustBumpUnstableGenerationOnOpen() throws Exception
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
        assertTrue( monitor.cleanupFinished, "Expected monitor to get recovery complete message" );
        assertEquals( 1, monitor.numberOfCleanedCrashPointers, "Expected index to have exactly 1 crash pointer from root to successor of root" );
        assertEquals( 2, monitor.numberOfPagesVisited,
                "Expected index to have exactly 2 tree node pages, root and successor of root" ); // Root and successor of root
    }

    /* Dirty state tests */

    @Test
    void indexMustBeCleanOnFirstInitialization() throws Exception
    {
        // GIVEN
        assertFalse( fileSystem.fileExists( indexFile ) );
        MonitorDirty monitorDirty = new MonitorDirty();

        // WHEN
        try ( GBPTree<MutableLong, MutableLong> ignored = index().with( monitorDirty ).build() )
        {
        }

        // THEN
        assertTrue( monitorDirty.cleanOnStart(), "Expected to be clean on start" );
    }

    @Test
    void indexMustBeCleanWhenClosedWithoutAnyChanges() throws Exception
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
        assertTrue( monitorDirty.cleanOnStart(), "Expected to be clean on start after close with no changes" );
    }

    @Test
    void indexMustBeCleanWhenClosedAfterCheckpoint() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong, MutableLong> index = index().build() )
        {
            insert( index, 0, 1 );

            index.checkpoint( UNLIMITED );
        }

        // WHEN
        MonitorDirty monitorDirty = new MonitorDirty();
        try ( GBPTree<MutableLong, MutableLong> ignored = index().with( monitorDirty ).build() )
        {
        }

        // THEN
        assertTrue( monitorDirty.cleanOnStart(), "Expected to be clean on start after close with checkpoint" );
    }

    @Test
    void indexMustBeDirtyWhenClosedWithChangesSinceLastCheckpoint() throws Exception
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
        assertFalse( monitorDirty.cleanOnStart(), "Expected to be dirty on start after close without checkpoint" );
    }

    @Test
    void indexMustBeDirtyWhenCrashedWithChangesSinceLastCheckpoint() throws Exception
    {
        // GIVEN
        try ( EphemeralFileSystemAbstraction ephemeralFs = new EphemeralFileSystemAbstraction() )
        {
            ephemeralFs.mkdirs( indexFile.getParentFile() );
            PageCache pageCache = pageCacheExtension.getPageCache( ephemeralFs );
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
            pageCache = pageCacheExtension.getPageCache( snapshot );
            try ( GBPTree<MutableLong, MutableLong> ignored = index( pageCache ).with( monitorDirty ).build() )
            {
            }
            assertFalse( monitorDirty.cleanOnStart(), "Expected to be dirty on start after crash" );
        }
    }

    @Test
    void cleanCrashPointersMustTriggerOnDirtyStart() throws Exception
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
            assertTrue( monitor.cleanupCalled(), "Expected cleanup to be called when starting on dirty tree" );
        }
    }

    @Test
    void cleanCrashPointersMustNotTriggerOnCleanStart() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong, MutableLong> index = index().build() )
        {
            insert( index, 0, 1 );

            index.checkpoint( UNLIMITED );
        }

        // WHEN
        MonitorCleanup monitor = new MonitorCleanup();
        try ( GBPTree<MutableLong, MutableLong> ignored = index().with( monitor ).build() )
        {
            // THEN
            assertFalse( monitor.cleanupCalled(), "Expected cleanup not to be called when starting on clean tree" );
        }
    }

    /* TreeState has outdated root */

    @Test
    void shouldThrowIfTreeStatePointToRootWithValidSuccessor() throws Exception
    {
        // GIVEN
        try ( PageCache specificPageCache = createPageCache( DEFAULT_PAGE_SIZE ) )
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
    void mustRetryCloseIfFailure() throws Exception
    {
        // GIVEN
        AtomicBoolean throwOnNext = new AtomicBoolean();
        IOException exception = new IOException( "My failure" );
        PageCache pageCache = pageCacheThatThrowExceptionWhenToldTo( exception, throwOnNext );
        try ( GBPTree<MutableLong, MutableLong> ignored = index( pageCache ).build() )
        {
            // WHEN
            throwOnNext.set( true );
        }
    }

    @Test
    void shouldThrowIllegalStateExceptionOnCallingNextAfterClose() throws Exception
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
                    tree.seek( new MutableLong( 0 ), new MutableLong( Long.MAX_VALUE ) );
            assertTrue( seek.next() );
            assertTrue( seek.next() );
            seek.close();

            for ( int i = 0; i < 2; i++ )
            {
                assertThrows( IllegalStateException.class, seek::next );
            }
        }
    }

    @Test
    void skipFlushingPageFileOnCloseWhenPageFileMarkForDeletion() throws IOException
    {
        DefaultPageCacheTracer defaultPageCacheTracer = new DefaultPageCacheTracer();
        PageCacheConfig config = config().withTracer( defaultPageCacheTracer );
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fileSystem, config );
              GBPTree<MutableLong,MutableLong> tree = index( pageCache ).with( RecoveryCleanupWorkCollector.ignore() ).build() )
        {
            List<PagedFile> pagedFiles = pageCache.listExistingMappings();
            assertThat( pagedFiles, hasSize( 1 ) );

            long flushesBefore = defaultPageCacheTracer.flushes();

            PagedFile indexPageFile = pagedFiles.get( 0 );
            indexPageFile.setDeleteOnClose( true );
            tree.close();

            assertEquals( flushesBefore, defaultPageCacheTracer.flushes() );
        }
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
        return pageCacheExtension.getPageCache( fileSystem, config().withPageSize( pageSize ) );
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
