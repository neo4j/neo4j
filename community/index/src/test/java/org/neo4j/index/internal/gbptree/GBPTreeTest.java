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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
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

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.index.internal.gbptree.GBPTree.Monitor;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.DelegatingPagedFile;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.test.Barrier;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.PageCacheConfig;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Math.toIntExact;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
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
import static org.neo4j.io.fs.FileUtils.blockSize;
import static org.neo4j.io.pagecache.IOLimiter.UNLIMITED;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.test.rule.PageCacheConfig.config;

@TestDirectoryExtension
@ExtendWith( RandomExtension.class )
class GBPTreeTest
{
    private static final Layout<MutableLong,MutableLong> layout = longLayout().build();

    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension( config().withAccessChecks( true ) );
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;

    private File indexFile;
    private ExecutorService executor;
    private int defaultPageSize;

    @BeforeEach
    void setUp() throws IOException
    {
        executor = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
        indexFile = testDirectory.file( "index" );
        defaultPageSize = toIntExact( blockSize( testDirectory.homeDir() ) );
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
        // open/close is enough
        index().build().close();

        // WHEN
        // open/close is enough
        index().build().close();

        // THEN being able to open validates that the same meta data was read
    }

    @Test
    void shouldFailToOpenOnDifferentMetaData() throws Exception
    {
        // GIVEN
        index( 4 * defaultPageSize ).build().close();

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
        index().build().close();

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
        index( 4 * defaultPageSize ).build().close();

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
        index().build().close();

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
        int pageSize = 2 * defaultPageSize;
        index( pageSize ).build().close();

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> ignored = index( pageSize / 2 ).build() )
        {
            fail( "Should not load" );
        }
        catch ( MetadataMismatchException e )
        {
            // THEN good
            assertThat( e.getMessage() ).contains( "page size" );
        }
    }

    @Test
    void shouldFailOnStartingWithPageSizeLargerThanThatOfPageCache() throws Exception
    {
        // WHEN
        int pageCachePageSize = defaultPageSize * 4;
        try ( GBPTree<MutableLong,MutableLong> ignored = index( pageCachePageSize )
                .withIndexPageSize( 2 * pageCachePageSize )
                .build() )
        {
            fail( "Shouldn't have been created" );
        }
        catch ( MetadataMismatchException e )
        {
            // THEN good
            assertThat( e.getMessage() ).contains( "page size" );
        }
    }

    @Test
    void shouldMapIndexFileWithProvidedPageSizeIfLessThanOrEqualToCachePageSize() throws Exception
    {
        // WHEN
        int pageCachePageSize = defaultPageSize * 4;
        index( pageCachePageSize ).withIndexPageSize( pageCachePageSize / 2 ).build().close();
    }

    @Test
    void shouldFailWhenTryingToRemapWithPageSizeLargerThanCachePageSize() throws Exception
    {
        // WHEN
        int pageCachePageSize = defaultPageSize * 4;
        index( pageCachePageSize ).build().close();

        try ( GBPTree<MutableLong, MutableLong> ignored = index( pageCachePageSize / 2 )
                .withIndexPageSize( pageCachePageSize )
                .build() )
        {
            fail( "Expected to fail" );
        }
        catch ( MetadataMismatchException e )
        {
            // THEN Good
            assertThat( e.getMessage() ).contains( "page size" );
        }
    }

    @Test
    void shouldRemapFileIfMappedWithPageSizeLargerThanCreationSize() throws Exception
    {
        // WHEN
        int pageSize = defaultPageSize * 4;
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
            try ( Writer<MutableLong,MutableLong> writer = index.writer( NULL ) )
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
            index.checkpoint( UNLIMITED, NULL );
        }

        // THEN
        try ( GBPTree<MutableLong,MutableLong> index = index( pageSize ).build() )
        {
            MutableLong fromInclusive = new MutableLong( 0L );
            MutableLong toExclusive = new MutableLong( 200L );
            try ( Seeker<MutableLong,MutableLong> seek = index.seek( fromInclusive, toExclusive, NULL ) )
            {
                int i = 0;
                while ( seek.next() )
                {
                    assertEquals( seek.key().getValue(), expectedData.get( i ) );
                    assertEquals( seek.value().getValue(), expectedData.get( i ) );
                    i++;
                }
            }
        }
    }

    @Test
    void shouldFailWhenTryingToOpenWithDifferentFormatIdentifier() throws Exception
    {
        // GIVEN
        PageCache pageCache = createPageCache( defaultPageSize );
        GBPTreeBuilder<MutableLong,MutableLong> builder = index( pageCache );
        builder.build().close();

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
            Seeker<MutableLong,MutableLong> result = index.seek( new MutableLong( 0 ), new MutableLong( 10 ), NULL );

            // THEN
            assertFalse( result.next() );
        }
    }

    @Test
    void shouldPickUpOpenOptions() throws IOException
    {
        // given
        try ( GBPTree<MutableLong,MutableLong> ignored = index().with( StandardOpenOption.DELETE_ON_CLOSE ).build() )
        {
            // when just closing it with the deletion flag
            assertTrue( fileSystem.fileExists( indexFile ) );
        }

        // then
        assertFalse( fileSystem.fileExists( indexFile ) );
    }

    /* Lifecycle tests */

    @Test
    void shouldNotBeAbleToAcquireModifierTwice() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            Writer<MutableLong,MutableLong> writer = index.writer( NULL );

            // WHEN
            try
            {
                index.writer( NULL );
                fail( "Should have failed" );
            }
            catch ( IllegalStateException e )
            {
                // THEN good
            }

            // Should be able to close old writer
            writer.close();
            // And open and closing a new one
            index.writer( NULL ).close();
        }
    }

    @Test
    void shouldNotAllowClosingWriterMultipleTimes() throws Exception
    {
        // GIVEN
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            Writer<MutableLong,MutableLong> writer = index.writer( NULL );
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
                assertThat( e.getMessage() ).contains( "already closed" );
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
            try ( Writer<MutableLong,MutableLong> ignored = index.writer( NULL ) )
            {
                fail( "Expected to throw" );
            }
            catch ( IOException e )
            {
                assertSame( no, e );
            }

            // THEN
            try ( Writer<MutableLong,MutableLong> writer = index.writer( NULL ) )
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
                    index.checkpoint( UNLIMITED, cursor -> cursor.putBytes( expected ), NULL );
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
                index.checkpoint( UNLIMITED, cursor -> cursor.putBytes( expected ), NULL );
                insert( index, 0, 1 );

                // WHEN
                // Should carry over header data
                index.checkpoint( UNLIMITED, NULL );
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
                index.checkpoint( UNLIMITED, cursor -> cursor.putBytes( expected ), NULL );
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
                index.checkpoint( UNLIMITED, cursor -> cursor.putBytes( expected ), NULL );
                ThreadLocalRandom.current().nextBytes( expected );
                index.checkpoint( UNLIMITED, cursor -> cursor.putBytes( expected ), NULL );
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
        PageCache pageCache = createPageCache( defaultPageSize );
        index( pageCache ).with( headerWriter ).build().close();

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
        PageCache pageCache = createPageCache( defaultPageSize );
        index( pageCache ).with( headerWriter ).build().close();

        // WHEN
        byte[] fraudulentBytes = new byte[12];
        do
        {
            ThreadLocalRandom.current().nextBytes( fraudulentBytes );
        }
        while ( Arrays.equals( expectedBytes, fraudulentBytes ) );

        index( pageCache ).with( headerWriter ).build().close();

        // THEN
        verifyHeader( pageCache, expectedBytes );
    }

    @Test
    void overwriteHeaderMustOnlyOverwriteHeaderNotState() throws Exception
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        byte[] initialHeader = new byte[random.nextInt( 100 )];
        random.nextBytes( initialHeader );
        Consumer<PageCursor> headerWriter = pc -> pc.putBytes( initialHeader );
        PageCache pageCache = createPageCache( defaultPageSize );
        index( pageCache ).with( headerWriter ).build().close();

        Pair<TreeState,TreeState> treeStatesBeforeOverwrite = readTreeStates( pageCache );

        byte[] newHeader = new byte[random.nextInt( 100 )];
        ThreadLocalRandom.current().nextBytes( newHeader );
        GBPTree.overwriteHeader( pageCache, indexFile, pc -> pc.putBytes( newHeader ), NULL );

        Pair<TreeState,TreeState> treeStatesAfterOverwrite = readTreeStates( pageCache );

        // TreeStates are the same
        assertEquals( treeStatesBeforeOverwrite.getLeft(), treeStatesAfterOverwrite.getLeft(),
                "expected tree state to exactly the same before and after overwriting header" );
        assertEquals( treeStatesBeforeOverwrite.getRight(), treeStatesAfterOverwrite.getRight(),
                "expected tree state to exactly the same before and after overwriting header" );

        // Verify header was actually overwritten. Do this after reading tree states because it will bump tree generation.
        verifyHeader( pageCache, newHeader );
    }

    private Pair<TreeState,TreeState> readTreeStates( PageCache pageCache ) throws IOException
    {
        Pair<TreeState,TreeState> treeStatesBeforeOverwrite;
        try ( PagedFile pagedFile = pageCache.map( indexFile, pageCache.pageSize() );
              PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
        {
            treeStatesBeforeOverwrite = TreeStatePair.readStatePages( cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B );
        }
        return treeStatesBeforeOverwrite;
    }

    private void verifyHeaderDataAfterClose( BiConsumer<GBPTree<MutableLong,MutableLong>,byte[]> beforeClose )
            throws IOException
    {
        byte[] expectedHeader = new byte[12];
        ThreadLocalRandom.current().nextBytes( expectedHeader );
        PageCache pageCache = createPageCache( defaultPageSize );

        // GIVEN
        try ( GBPTree<MutableLong,MutableLong> index = index( pageCache ).build() )
        {
            beforeClose.accept( index, expectedHeader );
        }

        verifyHeader( pageCache, expectedHeader );
    }

    @Test
    void writeHeaderInDirtyTreeMustNotDeadlock()
    {
        assertTimeoutPreemptively( ofSeconds( 10 ), () ->
        {
            PageCache pageCache = createPageCache( defaultPageSize * 4 );
            makeDirty( pageCache );

            Consumer<PageCursor> headerWriter = pc -> pc.putBytes( "failed".getBytes() );
            try ( GBPTree<MutableLong,MutableLong> index = index( pageCache ).with( RecoveryCleanupWorkCollector.ignore() ).build() )
            {
                index.checkpoint( UNLIMITED, headerWriter, NULL );
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
        // open/close is enough to read header
        index( pageCache ).with( headerReader ).build().close();

        // THEN
        assertEquals( expectedHeader.length, length.get() );
        assertArrayEquals( expectedHeader, readHeader );

        // WHEN
        // Read separate
        GBPTree.readHeader( pageCache, indexFile, headerReader, NULL );

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
            GBPTree.readHeader( createPageCache( defaultPageSize ), doesNotExist, NO_HEADER_READER, NULL );
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
        openMustThrowMetadataMismatchExceptionIfFileIsEmpty( pageCache -> GBPTree.readHeader( pageCache, indexFile, NO_HEADER_READER, NULL ) );
    }

    @Test
    void openWithConstructorMustThrowMetadataMismatchExceptionIfFileIsEmpty() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfFileIsEmpty( pageCache -> index( pageCache ).build() );
    }

    private void openMustThrowMetadataMismatchExceptionIfFileIsEmpty( ThrowingConsumer<PageCache,IOException> opener ) throws Exception
    {
        // given an existing empty file
        PageCache pageCache = createPageCache( defaultPageSize );
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
                pageCache -> GBPTree.readHeader( pageCache, indexFile, NO_HEADER_READER, NULL ) );
    }

    @Test
    void constructorMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing( pageCache -> index( pageCache ).build() );
    }

    private void openMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing( ThrowingConsumer<PageCache,IOException> opener ) throws Exception
    {
        // given an existing index with only the first page in it
        PageCache pageCache = createPageCache( defaultPageSize );
        index( pageCache ).build().close();
        fileSystem.truncate( indexFile, defaultPageSize /*truncate right after the first page*/ );

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
                pageCache -> GBPTree.readHeader( pageCache, indexFile, NO_HEADER_READER, NULL ) );
    }

    @Test
    void constructorMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros( pageCache -> index( pageCache ).build() );
    }

    private void openMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros( ThrowingConsumer<PageCache,IOException> opener ) throws Exception
    {
        // given an existing index with all-zero state pages
        PageCache pageCache = createPageCache( defaultPageSize );
        index( pageCache ).build().close();
        fileSystem.truncate( indexFile, defaultPageSize /*truncate right after the first page*/ );
        try ( OutputStream out = fileSystem.openAsOutputStream( indexFile, true ) )
        {
            byte[] allZeroPage = new byte[defaultPageSize];
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
        PageCache pageCache = createPageCache( defaultPageSize );

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
            GBPTree.readHeader( pageCache, indexFile, headerReader, NULL );

            // THEN
            assertEquals( headerBytes.length, length.get() );
            assertArrayEquals( headerBytes, readHeader);
        }
    }

    /* Mutex tests */

    @Test
    void checkPointShouldLockOutWriter()
    {
        assertTimeoutPreemptively( ofSeconds( 5 ), () ->
        {
            CheckpointControlledMonitor monitor = new CheckpointControlledMonitor();
            try ( GBPTree<MutableLong,MutableLong> index = index().with( monitor ).build() )
            {
                long key = 10;
                try ( Writer<MutableLong,MutableLong> writer = index.writer( NULL ) )
                {
                    writer.put( new MutableLong( key ), new MutableLong( key ) );
                }

                // WHEN
                monitor.enabled = true;
                Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( UNLIMITED, NULL ) ) );
                monitor.barrier.awaitUninterruptibly();
                // now we're in the smack middle of a checkpoint
                Future<?> writerClose = executor.submit( throwing( () -> index.writer( NULL ).close() ) );

                // THEN
                shouldWait( writerClose );
                monitor.barrier.release();

                writerClose.get();
                checkpoint.get();
            }
        } );
    }

    @Test
    void checkPointShouldWaitForWriter()
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
                    try ( Writer<MutableLong,MutableLong> writer = index.writer( NULL ) )
                    {
                        writer.put( new MutableLong( 1 ), new MutableLong( 1 ) );
                        barrier.reached();
                    }
                } ) );
                barrier.awaitUninterruptibly();
                Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( UNLIMITED, NULL ) ) );
                shouldWait( checkpoint );

                // THEN
                barrier.release();
                checkpoint.get();
                write.get();
            }
        } );
    }

    @Test
    void closeShouldLockOutWriter()
    {
        assertTimeoutPreemptively( ofSeconds( 50 ), () ->
        {
            // GIVEN
            AtomicBoolean enabled = new AtomicBoolean();
            Barrier.Control barrier = new Barrier.Control();
            PageCache pageCacheWithBarrier = pageCacheWithBarrierInClose( enabled, barrier );
            GBPTree<MutableLong,MutableLong> index = index( pageCacheWithBarrier ).build();
            long key = 10;
            try ( Writer<MutableLong,MutableLong> writer = index.writer( NULL ) )
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
                    index.writer( NULL ).close();
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

    @Test
    void writerShouldLockOutClose()
    {
        assertTimeoutPreemptively( ofSeconds( 5 ), () ->
        {
            // GIVEN
            GBPTree<MutableLong,MutableLong> index = index().build();

            // WHEN
            Barrier.Control barrier = new Barrier.Control();
            Future<?> write = executor.submit( throwing( () ->
            {
                try ( Writer<MutableLong,MutableLong> writer = index.writer( NULL ) )
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

        assertCleanOnStartup( false );
    }

    @Test
    void correctlyShutdownIndexIsClean() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            try ( Writer<MutableLong,MutableLong> writer = index.writer( NULL ) )
            {
                writer.put( new MutableLong( 1L ), new MutableLong( 2L ) );
            }
            index.checkpoint( UNLIMITED, NULL );
        }
        assertCleanOnStartup( true );
    }

    private void assertCleanOnStartup( boolean expected ) throws IOException
    {
        MutableBoolean cleanOnStartup = new MutableBoolean( true );
        try ( GBPTree<MutableLong, MutableLong> index = index().with( RecoveryCleanupWorkCollector.ignore() ).build() )
        {
            index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
            {
                @Override
                public void dirtyOnStartup( File file )
                {
                    cleanOnStartup.setFalse();
                }
            }, NULL );
            assertEquals( expected, cleanOnStartup.booleanValue() );
        }
    }

    @Test
    void cleanJobShouldLockOutCheckpoint()
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
                Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( UNLIMITED, NULL ) ) );
                shouldWait( checkpoint );

                monitor.barrier.release();
                cleanup.get();
                checkpoint.get();
            }
        } );
    }

    @Test
    void cleanJobShouldLockOutCheckpointOnNoUpdate()
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
                Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( UNLIMITED, NULL ) ) );
                shouldWait( checkpoint );

                monitor.barrier.release();
                cleanup.get();
                checkpoint.get();
            }
        } );
    }

    @Test
    void cleanJobShouldNotLockOutClose()
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
    void cleanJobShouldLockOutWriter()
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
                Future<?> writer = executor.submit( throwing( () -> index.writer( NULL ).close() ) );
                shouldWait( writer );

                monitor.barrier.release();
                cleanup.get();
                writer.get();
            }
        } );
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

    @Test
    void writerMustRecognizeFailedCleaning() throws Exception
    {
        mustRecognizeFailedCleaning( tree -> tree.writer( NULL ) );
    }

    @Test
    void checkpointMustRecognizeFailedCleaning() throws Exception
    {
        mustRecognizeFailedCleaning( index -> index.checkpoint( IOLimiter.UNLIMITED, NULL ) );
    }

    private void mustRecognizeFailedCleaning( ThrowingConsumer<GBPTree<MutableLong,MutableLong>,IOException> operation ) throws Exception
    {
        // given
        makeDirty();
        RuntimeException cleanupException = new RuntimeException( "Fail cleaning job" );
        CleanJobControlledMonitor cleanupMonitor = new CleanJobControlledMonitor()
        {
            @Override
            public void cleanupFinished( long numberOfPagesVisited, long numberOfTreeNodes, long numberOfCleanedCrashPointers,
                    long durationMillis )
            {
                super.cleanupFinished( numberOfPagesVisited, numberOfTreeNodes, numberOfCleanedCrashPointers, durationMillis );
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
            Future<?> cleanup = executor.submit( throwing( collector::start ) );
            shouldWait( cleanup );

            Future<?> checkpoint = executor.submit( throwing( () -> operation.accept( index ) ) );
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
                assertThat( e.getMessage() ).contains( "cleaning" ).contains( "failed" );
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
            try ( Writer<MutableLong,MutableLong> writer = index.writer( NULL ) )
            {
                writer.put( new MutableLong( 0 ), new MutableLong( 1 ) );
            }
            index.checkpoint( UNLIMITED, NULL );
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
            index.checkpoint( UNLIMITED, NULL );

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
            MutableLong to = new MutableLong( MAX_VALUE );
            try ( Seeker<MutableLong,MutableLong> seek = index.seek( from, to, NULL ) )
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
            index.checkpoint( UNLIMITED, NULL );
        }

        // THEN
        try ( GBPTree<MutableLong, MutableLong> index = index().build() )
        {
            MutableLong from = new MutableLong( Long.MIN_VALUE );
            MutableLong to = new MutableLong( MAX_VALUE );
            try ( Seeker<MutableLong,MutableLong> seek = index.seek( from, to, NULL ) )
            {
                assertTrue( seek.next() );
                assertEquals( key, seek.key().longValue() );
                assertEquals( value, seek.value().longValue() );
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
        index().with( monitor ).build().close();

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
        index().with( monitorDirty ).build().close();

        // THEN
        assertTrue( monitorDirty.cleanOnStart(), "Expected to be clean on start" );
    }

    @Test
    void indexMustBeCleanWhenClosedWithoutAnyChanges() throws Exception
    {
        // GIVEN
        index().build().close();

        // WHEN
        MonitorDirty monitorDirty = new MonitorDirty();
        index().with( monitorDirty ).build().close();

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

            index.checkpoint( UNLIMITED, NULL );
        }

        // WHEN
        MonitorDirty monitorDirty = new MonitorDirty();
        index().with( monitorDirty ).build().close();

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
        index().with( monitorDirty ).build().close();

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
            index( pageCache ).with( monitorDirty ).build().close();
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

            index.checkpoint( UNLIMITED, NULL );
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
        try ( PageCache specificPageCache = createPageCache( defaultPageSize ) )
        {
            index( specificPageCache ).build().close();

            // a tree state pointing to root with valid successor
            try ( PagedFile pagedFile = specificPageCache.map( indexFile, specificPageCache.pageSize() );
                  PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
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
                try ( Writer<MutableLong, MutableLong> ignored = index.writer( NULL ) )
                {
                    fail( "Expected to throw because root pointed to by tree state should have a valid successor." );
                }
            }
            catch ( TreeInconsistencyException e )
            {
                assertThat( e.getMessage() ).contains( PointerChecking.WRITER_TRAVERSE_OLD_STATE_MESSAGE );
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
            try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
            {
                MutableLong value = new MutableLong();
                for ( int i = 0; i < 10; i++ )
                {
                    value.setValue( i );
                    writer.put( value, value );
                }
            }

            Seeker<MutableLong,MutableLong> seek =
                    tree.seek( new MutableLong( 0 ), new MutableLong( MAX_VALUE ), NULL );
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
        DefaultPageCacheTracer defaultPageCacheTracer = DefaultPageCacheTracer.TRACER;
        PageCacheConfig config = config().withTracer( defaultPageCacheTracer );
        long initialPins = defaultPageCacheTracer.pins();
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fileSystem, config );
              GBPTree<MutableLong,MutableLong> tree = index( pageCache ).with( RecoveryCleanupWorkCollector.ignore() )
                      .with( defaultPageCacheTracer )
                      .build() )
        {
            List<PagedFile> pagedFiles = pageCache.listExistingMappings();
            assertThat( pagedFiles ).hasSize( 1 );

            long flushesBefore = defaultPageCacheTracer.flushes();

            PagedFile indexPageFile = pagedFiles.get( 0 );
            indexPageFile.setDeleteOnClose( true );
            tree.close();

            assertEquals( flushesBefore, defaultPageCacheTracer.flushes() );
            assertEquals( defaultPageCacheTracer.pins(), defaultPageCacheTracer.unpins() );
            assertThat( defaultPageCacheTracer.pins() ).isGreaterThan( initialPins );
            assertThat( defaultPageCacheTracer.pins() ).isGreaterThan( 1 );
        }
    }

    /* Inconsistency tests */

    @Test
    @Disabled
    void mustThrowIfStuckInInfiniteRootCatchup() throws IOException
    {
        // Create a tree with root and two children.
        // Corrupt one of the children and make it look like a freelist node.
        // This will cause seekCursor to start from root in an attempt, believing it went wrong because of concurrent updates.
        // When seekCursor comes back to the same corrupt child again and again it should eventually escape from that loop
        // with an exception.

        List<Long> trace = new ArrayList<>();
        MutableBoolean onOffSwitch = new MutableBoolean( true );
        PageCursorTracer pageCursorTracer = trackingPageCursorTracer( trace, onOffSwitch );
        PageCache pageCache = createPageCache( defaultPageSize );

        // Build a tree with root and two children.
        try ( GBPTree<MutableLong,MutableLong> tree = index( pageCache ).build() )
        {
            // Insert data until we have a split in root
            treeWithRootSplit( trace, tree );
            long corruptChild = trace.get( 1 );

            // We are not interested in further trace tracking
            onOffSwitch.setFalse();

            // Corrupt the child
            corruptTheChild( pageCache, corruptChild );

            assertTimeoutPreemptively( Duration.ofSeconds( 60 ), () ->
            {
                TreeInconsistencyException e = assertThrows( TreeInconsistencyException.class, () ->
                {
                    // when seek end up in this corrupt child we should eventually fail with a tree inconsistency exception
                    try ( Seeker<MutableLong,MutableLong> seek = tree.seek( new MutableLong( 0 ), new MutableLong( 0 ), pageCursorTracer ) )
                    {
                        seek.next();
                    }
                } );
                assertThat( e.getMessage() ).contains(
                        "Index traversal aborted due to being stuck in infinite loop. This is most likely caused by an inconsistency in the index. " +
                                "Loop occurred when restarting search from root from page " + corruptChild + "." );
            } );
        }
    }

    @Test
    @Disabled
    void mustThrowIfStuckInInfiniteRootCatchupMultipleConcurrentSeekers() throws IOException
    {
        List<Long> trace = new ArrayList<>();
        MutableBoolean onOffSwitch = new MutableBoolean( true );
        //TODO: pass this page cursor tracer to cursor tracers
        PageCursorTracer pageCursorTracer = trackingPageCursorTracer( trace, onOffSwitch );
        PageCache pageCache = createPageCache( defaultPageSize );

        // Build a tree with root and two children.
        try ( GBPTree<MutableLong,MutableLong> tree = index( pageCache ).build() )
        {
            // Insert data until we have a split in root
            treeWithRootSplit( trace, tree );
            long leftChild = trace.get( 1 );
            long rightChild = trace.get( 2 );

            // Stop trace tracking because we will soon start pinning pages from different threads
            onOffSwitch.setFalse();

            // Corrupt the child
            corruptTheChild( pageCache, leftChild );
            corruptTheChild( pageCache, rightChild );

            assertTimeoutPreemptively( Duration.ofSeconds( 5 ), () ->
            {
                // When seek end up in this corrupt child we should eventually fail with a tree inconsistency exception
                // even if we have multiple seeker that traverse different part of the tree and both get stuck in start from root loop.
                ExecutorService executor = null;
                try
                {
                    executor = Executors.newFixedThreadPool( 2 );
                    CountDownLatch go = new CountDownLatch( 2 );
                    Future<Object> execute1 = executor.submit( () ->
                    {
                        go.countDown();
                        go.await();
                        try ( Seeker<MutableLong,MutableLong> seek = tree.seek( new MutableLong( 0 ), new MutableLong( 0 ), NULL ) )
                        {
                            seek.next();
                        }
                        return null;
                    } );

                    Future<Object> execute2 = executor.submit( () ->
                    {
                        go.countDown();
                        go.await();
                        try ( Seeker<MutableLong,MutableLong> seek = tree
                                .seek( new MutableLong( MAX_VALUE ), new MutableLong( MAX_VALUE ), NULL ) )
                        {
                            seek.next();
                        }
                        return null;
                    } );

                    assertFutureFailsWithTreeInconsistencyException( execute1 );
                    assertFutureFailsWithTreeInconsistencyException( execute2 );
                }
                finally
                {
                    if ( executor != null )
                    {
                        executor.shutdown();
                    }
                }
            } );
        }
    }

    /* ReadOnly */

    @Test
    void mustNotMakeAnyChangesInReadOnlyMode() throws IOException
    {
        // given
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        PageCache pageCache = createPageCache( defaultPageSize );
        try ( GBPTree<MutableLong,MutableLong> tree = index( pageCache ).build() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                for ( int j = 0; j < 100; j++ )
                {
                    insert( tree, random.nextLong(), random.nextLong() );
                }
                tree.checkpoint( UNLIMITED, NULL );
            }
        }
        byte[] before = fileContent( indexFile );

        try ( GBPTree<MutableLong,MutableLong> tree = index( pageCache ).withReadOnly( true ).build() )
        {
            UnsupportedOperationException e = assertThrows( UnsupportedOperationException.class, () -> tree.writer( NULL ) );
            assertThat( e.getMessage() ).contains( "GBPTree was opened in read only mode and can not finish operation: " );

            MutableBoolean ioLimitChecked = new MutableBoolean();
            tree.checkpoint( ( previousStamp, recentlyCompletedIOs, flushable ) -> {
                ioLimitChecked.setTrue();
                return 0;
            }, NULL );
            assertFalse( ioLimitChecked.getValue(), "Expected checkpoint to be a no-op in read only mode." );
        }
        byte[] after = fileContent( indexFile );
        assertArrayEquals( before, after, "Expected file content to be identical before and after opening GBPTree in read only mode." );
    }

    @Test
    void mustFailGracefullyIfFileNotExistInReadOnlyMode()
    {
        // given
        PageCache pageCache = createPageCache( defaultPageSize );
        TreeFileNotFoundException e = assertThrows( TreeFileNotFoundException.class, () -> index( pageCache ).withReadOnly( true ).build() );
        assertThat( e.getMessage() ).contains( "Can not create new tree file in read only mode" );
        assertThat( e.getMessage() ).contains( indexFile.getAbsolutePath() );
    }

    private byte[] fileContent( File indexFile ) throws IOException
    {
        Set<OpenOption> options = new HashSet<>();
        options.add( StandardOpenOption.READ );
        try ( StoreChannel storeChannel = fileSystem.open( indexFile, options ) )
        {
            int fileSize = (int) storeChannel.size();
            ByteBuffer expectedContent = ByteBuffers.allocate( fileSize );
            storeChannel.readAll( expectedContent );
            expectedContent.flip();
            byte[] bytes = new byte[fileSize];
            expectedContent.get( bytes );
            return bytes;
        }
    }

    private DefaultPageCursorTracer trackingPageCursorTracer( List<Long> trace, MutableBoolean onOffSwitch )
    {
        return new DefaultPageCursorTracer( DefaultPageCacheTracer.TRACER, "tracking" )
        {
            @Override
            public PinEvent beginPin( boolean writeLock, long filePageId, PageSwapper swapper )
            {
                if ( onOffSwitch.isTrue() )
                {
                    trace.add( filePageId );
                }
                return super.beginPin( writeLock, filePageId, swapper );
            }
        };
    }

    private void assertFutureFailsWithTreeInconsistencyException( Future<Object> future )
    {
        ExecutionException e = assertThrows( ExecutionException.class, future::get );
        Throwable cause = e.getCause();
        if ( !(cause instanceof TreeInconsistencyException) )
        {
            fail( "Expected cause to be " + TreeInconsistencyException.class + " but was " + Exceptions.stringify( cause ) );
        }
    }

    private void corruptTheChild( PageCache pageCache, long corruptChild ) throws IOException
    {
        try ( PagedFile pagedFile = pageCache.map( indexFile, defaultPageSize );
              PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
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
            try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
            {
                writer.put( new MutableLong( count ), new MutableLong( count ) );
                count++;
            }
            trace.clear();
            try ( Seeker<MutableLong,MutableLong> seek = tree.seek( new MutableLong( 0 ), new MutableLong( 0 ), NULL ) )
            {
                seek.next();
            }
        }
        while ( trace.size() <= 1 );

        trace.clear();
        try ( Seeker<MutableLong,MutableLong> seek = tree.seek( new MutableLong( 0 ), new MutableLong( MAX_VALUE ), NULL ) )
        {
            //noinspection StatementWithEmptyBody
            while ( seek.next() )
            {
            }
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
        return new DelegatingPageCache( createPageCache( defaultPageSize ) )
        {
            @Override
            public PagedFile map( File file, int pageSize, OpenOption... openOptions ) throws IOException
            {
                return new DelegatingPagedFile( super.map( file, pageSize, openOptions ) )
                {
                    @Override
                    public PageCursor io( long pageId, int pf_flags, PageCursorTracer tracer ) throws IOException
                    {
                        maybeThrow();
                        return super.io( pageId, pf_flags, tracer );
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
        return new DelegatingPageCache( createPageCache( defaultPageSize ) )
        {
            @Override
            public PagedFile map( File file, int pageSize, OpenOption... openOptions ) throws IOException
            {
                return new DelegatingPagedFile( super.map( file, pageSize, openOptions ) )
                {
                    @Override
                    public PageCursor io( long pageId, int pf_flags, PageCursorTracer tracer ) throws IOException
                    {
                        maybeBlock();
                        return super.io( pageId, pf_flags, tracer );
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
        makeDirty( createPageCache( defaultPageSize ) );
    }

    private void makeDirty( PageCache pageCache ) throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index( pageCache ).build() )
        {
            // Make dirty
            index.writer( NULL ).close();
        }
    }

    private void insert( GBPTree<MutableLong,MutableLong> index, long key, long value ) throws IOException
    {
        try ( Writer<MutableLong, MutableLong> writer = index.writer( NULL ) )
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
        public void cleanupFinished( long numberOfPagesVisited, long numberOfTreeNodes, long numberOfCleanedCrashPointers, long durationMillis )
        {
            barrier.reached();
        }
    }

    // The most common tree builds in this test
    private GBPTreeBuilder<MutableLong,MutableLong> index()
    {
        return index( defaultPageSize );
    }

    private GBPTreeBuilder<MutableLong,MutableLong> index( int pageSize )
    {
        return index( createPageCache( pageSize ) );
    }

    private GBPTreeBuilder<MutableLong,MutableLong> index( PageCache pageCache )
    {
        return new GBPTreeBuilder<>( pageCache, indexFile, layout );
    }

    private PageCache pageCacheWithBarrierInClose( final AtomicBoolean enabled, final Barrier.Control barrier )
    {
        return new DelegatingPageCache( createPageCache( defaultPageSize * 4 ) )
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
            assertThat( job.getCause().getMessage() ).contains( "File" ).contains( "unmapped" );
        }
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
        public void cleanupFinished( long numberOfPagesVisited, long numberOfTreeNodes, long numberOfCleanedCrashPointers, long durationMillis )
        {
            cleanupCalled = true;
        }

        boolean cleanupCalled()
        {
            return cleanupCalled;
        }
    }
}
