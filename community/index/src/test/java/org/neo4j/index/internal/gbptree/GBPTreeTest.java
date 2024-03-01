/*
 * Copyright (c) "Neo4j"
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
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.database.readonly.ConfigBasedLookupFactory;
import org.neo4j.configuration.database.readonly.ConfigReadOnlyDatabaseListener;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.index.internal.gbptree.GBPTree.Monitor;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.DelegatingPagedFile;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.Barrier;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.PageCacheConfig;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.utils.TestDirectory;

import static java.lang.Long.min;
import static java.lang.Math.toIntExact;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTreeStructure.visitState;
import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;
import static org.neo4j.index.internal.gbptree.ThrowingRunnable.throwing;
import static org.neo4j.io.fs.FileUtils.blockSize;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.utils.PageCacheConfig.config;

@TestDirectoryExtension
@ExtendWith( {RandomExtension.class, LifeExtension.class} )
class GBPTreeTest
{
    private static final Layout<MutableLong,MutableLong> layout = longLayout().build();

    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension( config().withAccessChecks( true ) );
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private RandomSupport random;
    @Inject
    private LifeSupport lifeSupport;

    private Path indexFile;
    private ExecutorService executor;
    private int defaultPageSize;

    @BeforeEach
    void setUp() throws IOException
    {
        executor = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
        indexFile = testDirectory.file( "index" );
        defaultPageSize = toIntExact( blockSize( testDirectory.homePath() ) );
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
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).build().close();
        }

        // WHEN
        // open/close is enough
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).build().close();
        }

        // THEN being able to open validates that the same meta data was read
    }

    @Test
    void shouldFailToOpenOnDifferentLayout() throws Exception
    {
        // GIVEN
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).build().close();
        }

        // WHEN
        SimpleLongLayout otherLayout = longLayout().withIdentifier( 123456 ).build();
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            assertThatThrownBy( () -> index( pageCache ).with( otherLayout ).build(), "Should not load" ).isInstanceOf( MetadataMismatchException.class );
        }
    }

    @Test
    void shouldFailToOpenOnDifferentMajorVersion() throws Exception
    {
        // GIVEN
        try ( PageCache pageCache = createPageCache( 4 * defaultPageSize ) )
        {
            index( pageCache ).build().close();
        }

        // WHEN
        SimpleLongLayout otherLayout = longLayout().withMajorVersion( 123 ).build();
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            assertThatThrownBy( () -> index( pageCache ).with( otherLayout ).build(), "Should not load" ).isInstanceOf( MetadataMismatchException.class );
        }
    }

    @Test
    void shouldFailToOpenOnDifferentMinorVersion() throws Exception
    {
        // GIVEN
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).build().close();
        }

        // WHEN
        SimpleLongLayout otherLayout = longLayout().withMinorVersion( 123 ).build();
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            assertThatThrownBy( () -> index( pageCache ).with( otherLayout ).build(), "Should not load" ).isInstanceOf( MetadataMismatchException.class );
        }
    }

    @Test
    void shouldFailOnOpenWithSmallerPageSize() throws Exception
    {
        // GIVEN
        int pageSize = 2 * defaultPageSize;
        try ( PageCache pageCache = createPageCache( pageSize ) )
        {
            index( pageCache ).build().close();
        }

        // WHEN
        int smallerPageSize = pageSize / 2;
        try ( PageCache pageCache = createPageCache( smallerPageSize ) )
        {
            assertThatThrownBy( () -> index( pageCache ).build() )
                    .isInstanceOf( MetadataMismatchException.class )
                    .hasMessageContaining(
                            "Tried to open the tree using page size %d, but the tree was original created with page size %d so cannot be opened.",
                            smallerPageSize, pageSize );
        }
    }

    @Test
    void shouldFailOnOpenWithLargerPageSize() throws Exception
    {
        // GIVEN
        int pageSize = 2 * defaultPageSize;
        try ( PageCache pageCache = createPageCache( pageSize ) )
        {
            index( pageCache ).build().close();
        }

        // WHEN
        int largerPageSize = 2 * pageSize;
        try ( PageCache pageCache = createPageCache( largerPageSize ) )
        {
            assertThatThrownBy( () -> index( pageCache ).build() )
                    .isInstanceOf( MetadataMismatchException.class )
                    .hasMessageContaining(
                            "Tried to open the tree using page size %d, but the tree was original created with page size %d so cannot be opened.",
                            largerPageSize, pageSize );
        }
    }

    @Test
    void shouldFailOnOpenWithUnreasonablePageSize() throws IOException
    {
        int pageSize = 2 * defaultPageSize;
        int unreasonablePageSize = pageSize + 1;
        try ( PageCache pageCache = createPageCache( pageSize ) )
        {
            index( pageCache ).build().close();

            try ( PagedFile pagedFile = pageCache.map( indexFile, pageSize, DEFAULT_DATABASE_NAME );
                    PageCursor cursor = pagedFile.io( IdSpace.META_PAGE_ID, PF_SHARED_WRITE_LOCK, NULL ) )
            {
                assertTrue( cursor.next() );

                Meta meta = Meta.read( cursor );
                Meta newMeta = new Meta( meta.getFormatIdentifier(), meta.getFormatVersion(), unreasonablePageSize, layout );

                cursor.setOffset( 0 );
                newMeta.write( cursor );
            }

            assertThatThrownBy( () -> index( pageCache ).build() )
                    .isInstanceOf( MetadataMismatchException.class )
                    .hasMessageContaining(
                            "Tried to open the tree using page size %d, but the tree was original created with page size %d so cannot be opened.", pageSize,
                            unreasonablePageSize );
        }
    }

    @Test
    void shouldFailWhenTryingToOpenWithDifferentFormatIdentifier() throws Exception
    {
        // GIVEN
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            GBPTreeBuilder<MutableLong,MutableLong> builder = index( pageCache );
            builder.build().close();

            assertThatThrownBy( () -> builder.with( longLayout().withFixedSize( false ).build() ).build() ).isInstanceOf( MetadataMismatchException.class );
        }
    }

    @Test
    void shouldReturnNoResultsOnEmptyIndex() throws Exception
    {
        // GIVEN
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> index = index( pageCache ).build() )
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
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> ignored = index( pageCache ).with( immutable.of( DELETE_ON_CLOSE ) ).build() )
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
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> index = index( pageCache ).build() )
        {
            Writer<MutableLong,MutableLong> writer = index.writer( NULL );

            // WHEN
            assertThatThrownBy( () -> index.writer( NULL ) ).isInstanceOf( IllegalStateException.class );

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
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> index = index( pageCache ).build() )
        {
            Writer<MutableLong,MutableLong> writer = index.writer( NULL );
            writer.put( new MutableLong( 0 ), new MutableLong( 1 ) );
            writer.close();

            assertThatThrownBy( writer::close ).isInstanceOf( IllegalStateException.class ).hasMessageContaining( "already closed" );
        }
    }

    @Test
    void failureDuringInitializeWriterShouldNotFailNextInitialize() throws Exception
    {
        // GIVEN
        IOException no = new IOException( "No" );
        AtomicBoolean throwOnNextIO = new AtomicBoolean();
        try ( PageCache controlledPageCache = pageCacheThatThrowExceptionWhenToldTo( no, throwOnNextIO );
                GBPTree<MutableLong,MutableLong> index = index( controlledPageCache ).build() )
        {
            // WHEN
            assertTrue( throwOnNextIO.compareAndSet( false, true ) );
            assertThatThrownBy( () -> index.writer( NULL ) ).isSameAs( no );

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
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            // GIVEN
            GBPTree<MutableLong,MutableLong> index = index( pageCache ).build();

            // WHEN
            index.close();

            // THEN
            index.close(); // should be OK
        }
    }

    @Test
    void shouldNotFlushPagedFileIfDeleteOnClose() throws IOException
    {
        // GIVEN
        var tracer = new DefaultPageCacheTracer();
        try ( PageCache pageCache = PageCacheSupportExtension.getPageCache( fileSystem, config().withTracer( tracer ) ) )
        {
            // WHEN
            // Closing tree we should see flush happen
            long flushesBeforeClose;
            try ( GBPTree<MutableLong,MutableLong> index = index( pageCache ).build() )
            {
                index.setDeleteOnClose( false );
                flushesBeforeClose = tracer.flushes();
            }

            // THEN
            assertThat( tracer.flushes() ).isGreaterThan( flushesBeforeClose );

            // WHEN
            // Closing with set delete on close we should see no flush
            try ( GBPTree<MutableLong,MutableLong> index = index( pageCache ).build() )
            {
                index.setDeleteOnClose( true );
                flushesBeforeClose = tracer.flushes();
            }

            // THEN
            assertThat( tracer.flushes() ).isEqualTo( flushesBeforeClose );
        }
    }

    /* Header test */

    @Test
    void shouldPutHeaderDataInCheckPoint() throws Exception
    {
        BiConsumer<GBPTree<MutableLong,MutableLong>,byte[]> beforeClose = ( index, expected ) ->
        {
            ThrowingRunnable throwingRunnable = () ->
                    index.checkpoint( cursor -> cursor.putBytes( expected ), NULL );
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
                index.checkpoint( cursor -> cursor.putBytes( expected ), NULL );
                insert( index, 0, 1 );

                // WHEN
                // Should carry over header data
                index.checkpoint( NULL );
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
                index.checkpoint( cursor -> cursor.putBytes( expected ), NULL );
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
                index.checkpoint( cursor -> cursor.putBytes( expected ), NULL );
                random.nextBytes( expected );
                index.checkpoint( cursor -> cursor.putBytes( expected ), NULL );
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
        random.nextBytes( headerBytes );
        Consumer<PageCursor> headerWriter = pc -> pc.putBytes( headerBytes );

        // WHEN
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).with( headerWriter ).build().close();

            // THEN
            verifyHeader( pageCache, headerBytes );
        }
    }

    @Test
    void mustNotOverwriteHeaderOnExistingTree() throws Exception
    {
        // GIVEN
        byte[] expectedBytes = new byte[12];
        random.nextBytes( expectedBytes );
        Consumer<PageCursor> headerWriter = pc -> pc.putBytes( expectedBytes );
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).with( headerWriter ).build().close();

            // WHEN
            byte[] fraudulentBytes = new byte[12];
            do
            {
                random.nextBytes( fraudulentBytes );
            }
            while ( Arrays.equals( expectedBytes, fraudulentBytes ) );

            index( pageCache ).with( headerWriter ).build().close();

            // THEN
            verifyHeader( pageCache, expectedBytes );
        }
    }

    @Test
    void overwriteHeaderMustOnlyOverwriteHeaderNotState() throws Exception
    {
        byte[] initialHeader = new byte[random.nextInt( 100 )];
        random.nextBytes( initialHeader );
        Consumer<PageCursor> headerWriter = pc -> pc.putBytes( initialHeader );
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).with( headerWriter ).build().close();

            Pair<TreeState,TreeState> treeStatesBeforeOverwrite = readTreeStates( pageCache );

            byte[] newHeader = new byte[random.nextInt( 100 )];
            random.nextBytes( newHeader );
            GBPTree.overwriteHeader( pageCache, indexFile, pc -> pc.putBytes( newHeader ), DEFAULT_DATABASE_NAME, NULL );

            Pair<TreeState,TreeState> treeStatesAfterOverwrite = readTreeStates( pageCache );

            // TreeStates are the same
            assertEquals( treeStatesBeforeOverwrite.getLeft(), treeStatesAfterOverwrite.getLeft(),
                    "expected tree state to exactly the same before and after overwriting header" );
            assertEquals( treeStatesBeforeOverwrite.getRight(), treeStatesAfterOverwrite.getRight(),
                    "expected tree state to exactly the same before and after overwriting header" );

            // Verify header was actually overwritten. Do this after reading tree states because it will bump tree generation.
            verifyHeader( pageCache, newHeader );
        }
    }

    private Pair<TreeState,TreeState> readTreeStates( PageCache pageCache ) throws IOException
    {
        Pair<TreeState,TreeState> treeStatesBeforeOverwrite;
        try ( PagedFile pagedFile = pageCache.map( indexFile, pageCache.pageSize(), DEFAULT_DATABASE_NAME );
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
        random.nextBytes( expectedHeader );
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {

            // GIVEN
            try ( GBPTree<MutableLong,MutableLong> index = index( pageCache ).build() )
            {
                beforeClose.accept( index, expectedHeader );
            }

            verifyHeader( pageCache, expectedHeader );
        }
    }

    @Test
    void writeHeaderInDirtyTreeMustNotDeadlock() throws IOException
    {
        try ( PageCache pageCache = createPageCache( defaultPageSize * 4 ) )
        {
            makeDirty( pageCache );

            Consumer<PageCursor> headerWriter = pc -> pc.putBytes( "failed".getBytes() );
            try ( GBPTree<MutableLong,MutableLong> index = index( pageCache ).with( RecoveryCleanupWorkCollector.ignore() ).build() )
            {
                index.checkpoint( headerWriter, NULL );
            }

            verifyHeader( pageCache, "failed".getBytes() );
        }
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
        GBPTree.readHeader( pageCache, indexFile, headerReader, DEFAULT_DATABASE_NAME, NULL );

        assertEquals( expectedHeader.length, length.get() );
        assertArrayEquals( expectedHeader, readHeader );
    }

    @Test
    void readHeaderMustThrowIfFileDoesNotExist()
    {
        // given
        Path doesNotExist = Path.of( "Does not exist" );
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            assertThatThrownBy( () -> GBPTree.readHeader( pageCache, doesNotExist, NO_HEADER_READER, DEFAULT_DATABASE_NAME, NULL ) )
                    .isInstanceOf( NoSuchFileException.class );
        }
    }

    @Test
    void openWithReadHeaderMustThrowMetadataMismatchExceptionIfFileIsEmpty() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfFileIsEmpty( pageCache -> GBPTree.readHeader( pageCache, indexFile, NO_HEADER_READER,
                DEFAULT_DATABASE_NAME, NULL ) );
    }

    @Test
    void openWithConstructorMustThrowMetadataMismatchExceptionIfFileIsEmpty() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfFileIsEmpty( pageCache -> index( pageCache ).build() );
    }

    private void openMustThrowMetadataMismatchExceptionIfFileIsEmpty( ThrowingConsumer<PageCache,IOException> opener ) throws Exception
    {
        // given an existing empty file
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            pageCache.map( indexFile, pageCache.pageSize(), DEFAULT_DATABASE_NAME, immutable.of( CREATE ) ).close();

            assertThatThrownBy( () -> opener.accept( pageCache ) ).isInstanceOf( MetadataMismatchException.class );
        }
    }

    @Test
    void readHeaderMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing(
                pageCache -> GBPTree.readHeader( pageCache, indexFile, NO_HEADER_READER, DEFAULT_DATABASE_NAME, NULL ) );
    }

    @Test
    void constructorMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing( pageCache -> index( pageCache ).build() );
    }

    private void openMustThrowMetadataMismatchExceptionIfSomeMetaPageIsMissing( ThrowingConsumer<PageCache,IOException> opener ) throws Exception
    {
        // given an existing index with only the first page in it
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).build().close();
            /*truncate right after the first page*/
            fileSystem.truncate( indexFile, defaultPageSize );

            assertThatThrownBy( () -> opener.accept( pageCache ) ).isInstanceOf( MetadataMismatchException.class );
        }
    }

    @Test
    void readHeaderMustThrowIOExceptionIfStatePagesAreAllZeros() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros(
                pageCache -> GBPTree.readHeader( pageCache, indexFile, NO_HEADER_READER, DEFAULT_DATABASE_NAME, NULL ) );
    }

    @Test
    void constructorMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros() throws Exception
    {
        openMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros( pageCache -> index( pageCache ).build() );
    }

    private void openMustThrowMetadataMismatchExceptionIfStatePagesAreAllZeros( ThrowingConsumer<PageCache,IOException> opener ) throws Exception
    {
        // given an existing index with all-zero state pages
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).build().close();
            /*truncate right after the first page*/
            fileSystem.truncate( indexFile, defaultPageSize );
            try ( OutputStream out = fileSystem.openAsOutputStream( indexFile, true ) )
            {
                byte[] allZeroPage = new byte[defaultPageSize];
                out.write( allZeroPage ); // page A
                out.write( allZeroPage ); // page B
            }

            assertThatThrownBy( () -> opener.accept( pageCache ) ).isInstanceOf( MetadataMismatchException.class );
        }
    }

    @Test
    void readHeaderMustWorkWithOpenIndex() throws Exception
    {
        // GIVEN
        byte[] headerBytes = new byte[12];
        random.nextBytes( headerBytes );
        Consumer<PageCursor> headerWriter = pc -> pc.putBytes( headerBytes );
        // WHEN
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> ignore = index( pageCache ).with( headerWriter ).build() )
        {
            byte[] readHeader = new byte[headerBytes.length];
            AtomicInteger length = new AtomicInteger();
            Header.Reader headerReader = headerData ->
            {
                length.set( headerData.limit() );
                headerData.get( readHeader );
            };
            GBPTree.readHeader( pageCache, indexFile, headerReader, DEFAULT_DATABASE_NAME, NULL );

            // THEN
            assertEquals( headerBytes.length, length.get() );
            assertArrayEquals( headerBytes, readHeader );
        }
    }

    /* Mutex tests */

    @Test
    void checkPointShouldLockOutWriter() throws IOException, ExecutionException, InterruptedException
    {
        CheckpointControlledMonitor monitor = new CheckpointControlledMonitor();
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> index = index( pageCache ).with( monitor ).build() )
        {
            long key = 10;
            try ( Writer<MutableLong,MutableLong> writer = index.writer( NULL ) )
            {
                writer.put( new MutableLong( key ), new MutableLong( key ) );
            }

            // WHEN
            monitor.enabled = true;
            Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( NULL ) ) );
            monitor.barrier.awaitUninterruptibly();
            // now we're in the smack middle of a checkpoint
            Future<?> writerClose = executor.submit( throwing( () -> index.writer( NULL ).close() ) );

            // THEN
            shouldWait( writerClose );
            monitor.barrier.release();

            writerClose.get();
            checkpoint.get();
        }
    }

    @Test
    void checkPointShouldWaitForWriter() throws IOException, ExecutionException, InterruptedException
    {
        // GIVEN
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> index = index( pageCache ).build() )
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
            Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( NULL ) ) );
            shouldWait( checkpoint );

            // THEN
            barrier.release();
            checkpoint.get();
            write.get();
        }
    }

    @Test
    void closeShouldLockOutWriter() throws ExecutionException, InterruptedException, IOException
    {
        // GIVEN
        AtomicBoolean enabled = new AtomicBoolean();
        Barrier.Control barrier = new Barrier.Control();
        try ( PageCache pageCacheWithBarrier = pageCacheWithBarrierInClose( enabled, barrier ) )
        {
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
        }
    }

    @Test
    void writerShouldLockOutClose() throws ExecutionException, InterruptedException
    {
        // GIVEN
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            GBPTree<MutableLong,MutableLong> index = index( pageCache ).build();

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
        }
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
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> index = index( pageCache ).build() )
        {
            try ( Writer<MutableLong,MutableLong> writer = index.writer( NULL ) )
            {
                writer.put( new MutableLong( 1L ), new MutableLong( 2L ) );
            }
            index.checkpoint( NULL );
        }
        assertCleanOnStartup( true );
    }

    private void assertCleanOnStartup( boolean expected ) throws IOException
    {
        MutableBoolean cleanOnStartup = new MutableBoolean( true );
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong, MutableLong> index = index( pageCache ).with( RecoveryCleanupWorkCollector.ignore() ).build() )
        {
            index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
            {
                @Override
                public void dirtyOnStartup( Path file )
                {
                    cleanOnStartup.setFalse();
                }
            }, NULL );
            assertEquals( expected, cleanOnStartup.booleanValue() );
        }
    }

    @Test
    void cleanJobShouldLockOutCheckpoint() throws IOException, ExecutionException, InterruptedException
    {
        // GIVEN
        makeDirty();

        RecoveryCleanupWorkCollector cleanupWork = new ControlledRecoveryCleanupWorkCollector();
        CleanJobControlledMonitor monitor = new CleanJobControlledMonitor();
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> index = index( pageCache ).with( monitor ).with( cleanupWork ).build() )
        {
            // WHEN cleanup not finished
            Future<?> cleanup = executor.submit( throwing( cleanupWork::start ) );
            monitor.barrier.awaitUninterruptibly();

            // THEN
            Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( NULL ) ) );
            shouldWait( checkpoint );

            monitor.barrier.release();
            cleanup.get();
            checkpoint.get();
        }
    }

    @Test
    void cleanJobShouldLockOutCheckpointOnNoUpdate() throws IOException, ExecutionException, InterruptedException
    {
        // GIVEN
        makeDirty();

        RecoveryCleanupWorkCollector cleanupWork = new ControlledRecoveryCleanupWorkCollector();
        CleanJobControlledMonitor monitor = new CleanJobControlledMonitor();
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> index = index( pageCache ).with( monitor ).with( cleanupWork ).build() )
        {
            // WHEN cleanup not finished
            Future<?> cleanup = executor.submit( throwing( cleanupWork::start ) );
            monitor.barrier.awaitUninterruptibly();

            // THEN
            Future<?> checkpoint = executor.submit( throwing( () -> index.checkpoint( NULL ) ) );
            shouldWait( checkpoint );

            monitor.barrier.release();
            cleanup.get();
            checkpoint.get();
        }
    }

    @Test
    void cleanJobShouldNotLockOutClose() throws IOException, ExecutionException, InterruptedException
    {
        // GIVEN
        makeDirty();

        RecoveryCleanupWorkCollector cleanupWork = new ControlledRecoveryCleanupWorkCollector();
        CleanJobControlledMonitor monitor = new CleanJobControlledMonitor();
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            GBPTree<MutableLong,MutableLong> index = index( pageCache ).with( monitor ).with( cleanupWork ).build();

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
    }

    @Test
    void cleanJobShouldLockOutWriter() throws IOException, ExecutionException, InterruptedException
    {
        // GIVEN
        makeDirty();

        RecoveryCleanupWorkCollector cleanupWork = new ControlledRecoveryCleanupWorkCollector();
        CleanJobControlledMonitor monitor = new CleanJobControlledMonitor();
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> index = index( pageCache ).with( monitor ).with( cleanupWork ).build() )
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
    }

    /* Cleaner test */

    @Test
    void cleanerShouldDieSilentlyOnClose() throws Throwable
    {
        // GIVEN
        makeDirty();

        AtomicBoolean blockOnNextIO = new AtomicBoolean();
        Barrier.Control control = new Barrier.Control();
        Future<List<CleanupJob>> cleanJob;
        try ( PageCache pageCache = pageCacheThatBlockWhenToldTo( control, blockOnNextIO ) )
        {
            ControlledRecoveryCleanupWorkCollector collector = new ControlledRecoveryCleanupWorkCollector();
            collector.init();

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
        }
    }

    @Test
    void treeMustBeDirtyAfterCleanerDiedOnClose() throws Throwable
    {
        // GIVEN
        makeDirty();

        AtomicBoolean blockOnNextIO = new AtomicBoolean();
        Barrier.Control control = new Barrier.Control();
        Future<List<CleanupJob>> cleanJob;
        try ( PageCache pageCache = pageCacheThatBlockWhenToldTo( control, blockOnNextIO ) )
        {
            ControlledRecoveryCleanupWorkCollector collector = new ControlledRecoveryCleanupWorkCollector();
            collector.init();

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
            try ( PageCache pageCache1 = createPageCache( defaultPageSize );
                    GBPTree<MutableLong,MutableLong> ignored = index( pageCache1 ).with( monitor ).build() )
            {
                assertFalse( monitor.cleanOnStart() );
            }
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
        mustRecognizeFailedCleaning( index -> index.checkpoint( NULL ) );
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

        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> index = index( pageCache )
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

            assertThatThrownBy( checkpoint::get, "Expected checkpoint to fail because of failed cleaning job" )
                    .isInstanceOf( ExecutionException.class )
                    .hasMessageContaining( "cleaning" )
                    .hasMessageContaining( "failed" );
        }
    }

    /* Checkpoint tests */

    @Test
    void shouldCheckpointAfterInitialCreation() throws Exception
    {
        // GIVEN
        CheckpointCounter checkpointCounter = new CheckpointCounter();

        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            // WHEN
            GBPTree<MutableLong,MutableLong> index = index( pageCache ).with( checkpointCounter ).build();

            // THEN
            assertEquals( 1, checkpointCounter.count() );
            index.close();
        }
    }

    @Test
    void shouldNotCheckpointOnClose() throws Exception
    {
        // GIVEN
        CheckpointCounter checkpointCounter = new CheckpointCounter();

        // WHEN
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> index = index( pageCache ).with( checkpointCounter ).build() )
        {
            checkpointCounter.reset();
            try ( Writer<MutableLong,MutableLong> writer = index.writer( NULL ) )
            {
                writer.put( new MutableLong( 0 ), new MutableLong( 1 ) );
            }
            index.checkpoint( NULL );
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
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> index = index( pageCache ).with( checkpointCounter ).build() )
        {
            checkpointCounter.reset();
            index.checkpoint( NULL );

            // THEN
            assertEquals( 1, checkpointCounter.count() );
        }
    }

    @Test
    void mustNotSeeUpdatesThatWasNotCheckpointed() throws Exception
    {
        // GIVEN
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong, MutableLong> index = index( pageCache ).build() )
        {
            insert( index, 0, 1 );

            // WHEN
            // No checkpoint before close
        }

        // THEN
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong, MutableLong> index = index( pageCache ).build() )
        {
            MutableLong from = new MutableLong( Long.MIN_VALUE );
            MutableLong to = new MutableLong( Long.MAX_VALUE );
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
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong, MutableLong> index = index( pageCache ).build() )
        {
            insert( index, key, value );

            // WHEN
            index.checkpoint( NULL );
        }

        // THEN
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong, MutableLong> index = index( pageCache ).build() )
        {
            MutableLong from = new MutableLong( Long.MIN_VALUE );
            MutableLong to = new MutableLong( Long.MAX_VALUE );
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
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong, MutableLong> index = index( pageCache ).build() )
        {
            insert( index, 0, 1 );

            // no checkpoint
        }

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).with( monitor ).build().close();
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
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).with( monitorDirty ).build().close();
        }

        // THEN
        assertTrue( monitorDirty.cleanOnStart(), "Expected to be clean on start" );
    }

    @Test
    void indexMustBeCleanWhenClosedWithoutAnyChanges() throws Exception
    {
        // GIVEN
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).build().close();
        }

        // WHEN
        MonitorDirty monitorDirty = new MonitorDirty();
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).with( monitorDirty ).build().close();
        }

        // THEN
        assertTrue( monitorDirty.cleanOnStart(), "Expected to be clean on start after close with no changes" );
    }

    @Test
    void indexMustBeCleanWhenClosedAfterCheckpoint() throws Exception
    {
        // GIVEN
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong, MutableLong> index = index( pageCache ).build() )
        {
            insert( index, 0, 1 );

            index.checkpoint( NULL );
        }

        // WHEN
        MonitorDirty monitorDirty = new MonitorDirty();
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).with( monitorDirty ).build().close();
        }

        // THEN
        assertTrue( monitorDirty.cleanOnStart(), "Expected to be clean on start after close with checkpoint" );
    }

    @Test
    void indexMustBeDirtyWhenClosedWithChangesSinceLastCheckpoint() throws Exception
    {
        // GIVEN
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong, MutableLong> index = index( pageCache ).build() )
        {
            insert( index, 0, 1 );

            // no checkpoint
        }

        // WHEN
        MonitorDirty monitorDirty = new MonitorDirty();
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            index( pageCache ).with( monitorDirty ).build().close();
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
            ephemeralFs.mkdirs( indexFile.getParent() );
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
            pageCache.close();
        }
    }

    @Test
    void cleanCrashPointersMustTriggerOnDirtyStart() throws Exception
    {
        // GIVEN
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong, MutableLong> index = index( pageCache ).build() )
        {
            insert( index, 0, 1 );

            // No checkpoint
        }

        // WHEN
        MonitorCleanup monitor = new MonitorCleanup();
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong, MutableLong> ignored = index( pageCache ).with( monitor ).build() )
        {
            // THEN
            assertTrue( monitor.cleanupCalled(), "Expected cleanup to be called when starting on dirty tree" );
        }
    }

    @Test
    void cleanCrashPointersMustNotTriggerOnCleanStart() throws Exception
    {
        // GIVEN
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong, MutableLong> index = index( pageCache ).build() )
        {
            insert( index, 0, 1 );

            index.checkpoint(  NULL );
        }

        // WHEN
        MonitorCleanup monitor = new MonitorCleanup();
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong, MutableLong> ignored = index( pageCache ).with( monitor ).build() )
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
            try ( PagedFile pagedFile = specificPageCache.map( indexFile, specificPageCache.pageSize(), DEFAULT_DATABASE_NAME );
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
                assertThatThrownBy( () -> index.writer( NULL ), "Expected to throw because root pointed to by tree state should have a valid successor." )
                        .isInstanceOf( TreeInconsistencyException.class )
                        .hasMessageContaining( PointerChecking.WRITER_TRAVERSE_OLD_STATE_MESSAGE );
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
        try ( PageCache pageCache = pageCacheThatThrowExceptionWhenToldTo( exception, throwOnNext );
                GBPTree<MutableLong, MutableLong> ignored = index( pageCache ).build() )
        {
            // WHEN
            throwOnNext.set( true );
        }
    }

    @Test
    void shouldReturnFalseOnCallingNextAfterClose() throws Exception
    {
        // given
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> tree = index( pageCache ).build() )
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
                    tree.seek( new MutableLong( 0 ), new MutableLong( Long.MAX_VALUE ), NULL );
            assertTrue( seek.next() );
            assertTrue( seek.next() );
            seek.close();

            for ( int i = 0; i < 2; i++ )
            {
                assertFalse( seek.next() );
            }
        }
    }

    @Test
    void shouldReturnFalseOnCallingNextAfterExhausting() throws Exception
    {
        int amount = 10;
        // given
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> tree = index( pageCache ).build() )
        {
            try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
            {
                MutableLong value = new MutableLong();
                for ( int i = 0; i < amount; i++ )
                {
                    value.setValue( i );
                    writer.put( value, value );
                }
            }

            Seeker<MutableLong,MutableLong> seek =
                    tree.seek( new MutableLong( 0 ), new MutableLong( Long.MAX_VALUE ), NULL );
            //noinspection StatementWithEmptyBody
            while ( seek.next() )
            {
            }

            try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
            {
                MutableLong value = new MutableLong();
                value.setValue( amount + 1 );
                writer.put( value, value );
            }

            for ( int i = 0; i < 2; i++ )
            {
                assertFalse( seek.next() );
            }
        }
    }

    @Test
    void shouldReturnFalseOnCallingNextAfterExhaustingAndClose() throws Exception
    {
        int amount = 10;
        // given
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> tree = index( pageCache ).build() )
        {
            try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
            {
                MutableLong value = new MutableLong();
                for ( int i = 0; i < amount; i++ )
                {
                    value.setValue( i );
                    writer.put( value, value );
                }
            }

            Seeker<MutableLong,MutableLong> seek =
                    tree.seek( new MutableLong( 0 ), new MutableLong( Long.MAX_VALUE ), NULL );
            //noinspection StatementWithEmptyBody
            while ( seek.next() )
            {
            }
            seek.close();

            try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
            {
                MutableLong value = new MutableLong();
                value.setValue( amount + 1 );
                writer.put( value, value );
            }

            for ( int i = 0; i < 2; i++ )
            {
                assertFalse( seek.next() );
            }
        }
    }

    @Test
    void skipFlushingPageFileOnCloseWhenPageFileMarkForDeletion() throws IOException
    {
        var tracer = new DefaultPageCacheTracer();
        PageCacheConfig config = config().withTracer( tracer );
        long initialPins = tracer.pins();
        try ( PageCache pageCache = PageCacheSupportExtension.getPageCache( fileSystem, config );
              GBPTree<MutableLong,MutableLong> tree = index( pageCache ).with( RecoveryCleanupWorkCollector.ignore() )
                      .with( tracer )
                      .build() )
        {
            List<PagedFile> pagedFiles = pageCache.listExistingMappings();
            assertThat( pagedFiles ).hasSize( 1 );

            long flushesBefore = tracer.flushes();

            PagedFile indexPageFile = pagedFiles.get( 0 );
            indexPageFile.setDeleteOnClose( true );
            tree.close();

            assertEquals( flushesBefore, tracer.flushes() );
            assertEquals( tracer.pins(), tracer.unpins() );
            assertThat( tracer.pins() ).isGreaterThan( initialPins );
            assertThat( tracer.pins() ).isGreaterThan( 1 );
        }
    }

    /* Inconsistency tests */

    @Test
    void mustThrowIfStuckInInfiniteRootCatchup() throws IOException
    {
        // Create a tree with root and two children.
        // Corrupt one of the children and make it look like a freelist node.
        // This will cause seekCursor to start from root in an attempt, believing it went wrong because of concurrent updates.
        // When seekCursor comes back to the same corrupt child again and again it should eventually escape from that loop
        // with an exception.

        List<Long> trace = new ArrayList<>();
        MutableBoolean onOffSwitch = new MutableBoolean( true );
        CursorContext cursorContext = new CursorContext( trackingPageCursorTracer( trace, onOffSwitch ) );

        // Build a tree with root and two children.
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> tree = index( pageCache ).build() )
        {
            // Insert data until we have a split in root
            treeWithRootSplit( trace, tree, cursorContext );
            long corruptChild = trace.get( 1 );

            // We are not interested in further trace tracking
            onOffSwitch.setFalse();

            // Corrupt the child
            corruptTheChild( pageCache, corruptChild );

            assertThatThrownBy( () ->
                        {
                            // when seek end up in this corrupt child we should eventually fail with a tree inconsistency exception
                            try ( Seeker<MutableLong,MutableLong> seek = tree.seek( new MutableLong( 0 ), new MutableLong( 0 ), cursorContext ) )
                            {
                                seek.next();
                            }
                        } )
                    .isInstanceOf( TreeInconsistencyException.class )
                    .hasMessageContaining( "Index traversal aborted due to being stuck in infinite loop. This is most likely caused by an inconsistency " +
                            "in the index. Loop occurred when restarting search from root from page " + corruptChild + "." );
        }
    }

    @Test
    void mustThrowIfStuckInInfiniteRootCatchupMultipleConcurrentSeekers() throws IOException
    {
        List<Long> trace = new ArrayList<>();
        MutableBoolean onOffSwitch = new MutableBoolean( true );
        CursorContext cursorContext = new CursorContext( trackingPageCursorTracer( trace, onOffSwitch ) );

        // Build a tree with root and two children.
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                GBPTree<MutableLong,MutableLong> tree = index( pageCache ).build() )
        {
            // Insert data until we have a split in root
            treeWithRootSplit( trace, tree, cursorContext );
            long leftChild = trace.get( 1 );
            long rightChild = trace.get( 2 );

            // Stop trace tracking because we will soon start pinning pages from different threads
            onOffSwitch.setFalse();

            // Corrupt the child
            corruptTheChild( pageCache, leftChild );
            corruptTheChild( pageCache, rightChild );

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
                            .seek( new MutableLong( Long.MAX_VALUE ), new MutableLong( Long.MAX_VALUE ), NULL ) )
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
        }
    }

    @Test
    void preserveChangesEvenInTemporaryReadOnlyMode() throws IOException
    {
        // given
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            try ( GBPTree<MutableLong,MutableLong> tree = index( pageCache ).build() )
            {
                for ( int i = 0; i < 10; i++ )
                {
                    for ( int j = 0; j < 100; j++ )
                    {
                        insert( tree, random.nextLong(), random.nextLong() );
                    }
                    tree.checkpoint( NULL );
                }
            }
            byte[] before = fileContent( indexFile );

            NamedDatabaseId namedDatabaseId = DatabaseIdFactory.from( "db", UUID.randomUUID() );
            DatabaseReadOnlyChecker temporaryReadOnlyChecker = new ReadOnlyDatabases( () -> namedDatabaseId::equals ).forDatabase( namedDatabaseId );
            try ( GBPTree<MutableLong,MutableLong> tree = index( pageCache ).with( temporaryReadOnlyChecker ).build() )
            {
                assertThatThrownBy( () -> tree.writer( NULL ) )
                        .isInstanceOf( UnsupportedOperationException.class )
                        .hasMessageContaining( "GBPTree currently is in read only mode and can not finish operation: " );

                tree.checkpoint( NULL );
            }
            byte[] after = fileContent( indexFile );
            assertThat( after ).isNotEqualTo( before ).describedAs( "Expected file content to be diff since even read only mode can do checkpoints." );
        }
    }

    @Test
    void shouldNotBumpGenerationInReadOnlyMode() throws IOException
    {
        // given
        try ( var pageCache = createPageCache( defaultPageSize ) )
        {
            try ( var tree = index( pageCache ).build() )
            {
                tree.checkpoint( NULL );
            }

            // when
            var stateBeforeOpenReadOnly = captureTreeState( pageCache );
            index( pageCache ).with( readOnly() ).build().close();

            // then
            var stateAfterOpenReadOnly = captureTreeState( pageCache );
            assertThat( stateAfterOpenReadOnly ).isEqualTo( stateBeforeOpenReadOnly );
        }
    }

    @Test
    void shouldThrowOnWritingInReadOnlyMode() throws IOException
    {
        // given
        try ( var pageCache = createPageCache( defaultPageSize ) )
        {
            try ( var tree = index( pageCache ).build() )
            {
                tree.checkpoint( NULL );
            }

            // when
            try ( var tree = index( pageCache ).with( readOnly() ).build() )
            {
                // then
                assertThatThrownBy( () -> tree.writer( NULL ) ).isInstanceOf( UnsupportedOperationException.class );
            }
        }
    }

    @Test
    void mustFailGracefullyIfFileNotExistInReadOnlyMode()
    {
        // given
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            assertThatThrownBy( () -> index( pageCache ).with( readOnly() ).build() )
                    .isInstanceOf( TreeFileNotFoundException.class )
                    .hasMessageContaining( "Can not create new tree file in read only mode" )
                    .hasMessageContaining( indexFile.toAbsolutePath().toString() );
        }
    }

    @Test
    void trackPageCacheAccessOnVisit() throws IOException
    {
        var pageCacheTracer = new DefaultPageCacheTracer();

        try ( PageCache pageCache = createPageCache( defaultPageSize );
                var tree = index( pageCache ).with( pageCacheTracer ).build() )
        {
            var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "traverseTree" ) );
            tree.visit( new GBPTreeVisitor.Adaptor<>(), cursorContext );

            var cursorTracer = cursorContext.getCursorTracer();
            assertThat( cursorTracer.pins() ).isEqualTo( 5 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 5 );
            assertThat( cursorTracer.hits() ).isEqualTo( 5 );
        }
    }

    @Test
    void trackPageCacheAccessOnTreeSeek() throws IOException
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( PageCache pageCache = createPageCache( (int) ByteUnit.kibiBytes( 4 ) );
                var tree = index( pageCache ).with( pageCacheTracer ).build() )
        {
            for ( int i = 0; i < 1000; i++ )
            {
                insert( tree, i, 1 );
            }

            var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "trackPageCacheAccessOnTreeSeek" ) );

            try ( var seeker = tree.seek( new MutableLong( 0 ), new MutableLong( Integer.MAX_VALUE ), cursorContext ) )
            {
                while ( seeker.next() )
                {
                    // just scroll over the results
                }
            }
            var cursorTracer = cursorContext.getCursorTracer();

            assertThat( cursorTracer.hits() ).isEqualTo( 8 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 8 );
            assertThat( cursorTracer.pins() ).isEqualTo( 8 );
            assertThat( cursorTracer.faults() ).isEqualTo( 0 );
        }
    }

    @Test
    void trackPageCacheAccessOnEmptyTreeSeek() throws IOException
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                var tree = index( pageCache ).with( pageCacheTracer ).build() )
        {
            var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "trackPageCacheAccessOnTreeSeek" ) );
            try ( var seeker = tree.seek( new MutableLong( 0 ), new MutableLong( 1000 ), cursorContext ) )
            {
                while ( seeker.next() )
                {
                    // just scroll over the results
                }
            }
            var cursorTracer = cursorContext.getCursorTracer();
            assertThat( cursorTracer.hits() ).isEqualTo( 1 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 1 );
            assertThat( cursorTracer.pins() ).isEqualTo( 1 );
            assertThat( cursorTracer.faults() ).isEqualTo( 0 );
        }
    }

    @Test
    void readOnlyTreeStillFlushesStateWhenReadOnly() throws IOException
    {
        var config = Config.defaults();
        var defaultDatabaseId = DatabaseIdFactory.from( DEFAULT_DATABASE_NAME, UUID.randomUUID() ); //UUID required, but ignored by config lookup
        DatabaseIdRepository databaseIdRepository = Mockito.mock( DatabaseIdRepository.class );
        Mockito.when( databaseIdRepository.getByName( DEFAULT_DATABASE_NAME ) ).thenReturn( Optional.of( defaultDatabaseId ) );
        var readOnlyLookup =  new ConfigBasedLookupFactory( config, databaseIdRepository );
        var globalChecker = new ReadOnlyDatabases( readOnlyLookup );

        var checker = globalChecker.forDatabase( defaultDatabaseId );
        var listener = new ConfigReadOnlyDatabaseListener( globalChecker, config );
        lifeSupport.add( listener );
        var cursorContext = NULL;
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                var tree = index( pageCache ).with( checker ).build() )
        {
            try ( var writer = tree.writer( cursorContext ) )
            {
                writer.put( new MutableLong( 1 ), new MutableLong( 42 ) );
            }
            config.set( GraphDatabaseSettings.read_only_databases, Set.of( DEFAULT_DATABASE_NAME ) );

            assertThatThrownBy( () -> tree.writer( cursorContext ) ).isInstanceOf( Exception.class );

            tree.checkpoint( cursorContext );
        }

        try ( PageCache pageCache = createPageCache( defaultPageSize );
                var reopenedTree = index( pageCache ).with( checker ).build() )
        {
            assertThatThrownBy( () -> reopenedTree.writer( cursorContext ) ).isInstanceOf( Exception.class );

            try ( Seeker<MutableLong,MutableLong> seeker = reopenedTree.seek( new MutableLong( 0 ), new MutableLong( 77 ), cursorContext ) )
            {
                assertTrue( seeker.next() );
                assertEquals( 1, seeker.key().longValue() );
                assertEquals( 42, seeker.value().longValue() );
            }
        }
    }

    @Test
    void shouldReuseSeekCursorInstancesForMultipleSeeks() throws IOException
    {
        try ( PageCache pageCache = createPageCache( defaultPageSize );
                var tree = index( pageCache ).build() )
        {
            // given
            int count = 1_000;
            for ( int i = 0; i < count; i++ )
            {
                insert( tree, i, i * 100 );
            }

            // when
            try ( var seeker = tree.allocateSeeker( NULL ) )
            {
                long from = 0;
                while ( from < count )
                {
                    long to = min( count, from + random.nextInt( 1, count / 10 ) );
                    tree.seek( seeker, new MutableLong( from ), new MutableLong( to ) );
                    for ( long expected = from; expected < to; expected++ )
                    {
                        assertThat( seeker.next() ).isTrue();
                        assertThat( seeker.key().longValue() ).isEqualTo( expected );
                        assertThat( seeker.value().longValue() ).isEqualTo( expected * 100 );
                    }
                    assertThat( seeker.next() ).isFalse();
                    from = to;
                }
            }
        }
    }

    private byte[] fileContent( Path indexFile ) throws IOException
    {
        Set<OpenOption> options = new HashSet<>();
        options.add( StandardOpenOption.READ );
        try ( StoreChannel storeChannel = fileSystem.open( indexFile, options ) )
        {
            int fileSize = (int) storeChannel.size();
            ByteBuffer expectedContent = ByteBuffers.allocate( fileSize, INSTANCE );
            storeChannel.readAll( expectedContent );
            expectedContent.flip();
            byte[] bytes = new byte[fileSize];
            expectedContent.get( bytes );
            return bytes;
        }
    }

    private DefaultPageCursorTracer trackingPageCursorTracer( List<Long> trace, MutableBoolean onOffSwitch )
    {
        return new DefaultPageCursorTracer( new DefaultPageCacheTracer(), "tracking" )
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

    private static void assertFutureFailsWithTreeInconsistencyException( Future<Object> future )
    {
        assertThatThrownBy( future::get ).hasCauseInstanceOf( TreeInconsistencyException.class );
    }

    private void corruptTheChild( PageCache pageCache, long corruptChild ) throws IOException
    {
        try ( PagedFile pagedFile = pageCache.map( indexFile, defaultPageSize, DEFAULT_DATABASE_NAME );
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
    private static void treeWithRootSplit( List<Long> trace, GBPTree<MutableLong,MutableLong> tree, CursorContext cursorContext ) throws IOException
    {
        long count = 0;
        do
        {
            try ( Writer<MutableLong,MutableLong> writer = tree.writer( cursorContext ) )
            {
                writer.put( new MutableLong( count ), new MutableLong( count ) );
                count++;
            }
            trace.clear();
            try ( Seeker<MutableLong,MutableLong> seek = tree.seek( new MutableLong( 0 ), new MutableLong( 0 ), cursorContext ) )
            {
                seek.next();
            }
        }
        while ( trace.size() <= 1 );

        trace.clear();
        try ( Seeker<MutableLong,MutableLong> seek = tree.seek( new MutableLong( 0 ), new MutableLong( Long.MAX_VALUE ), cursorContext ) )
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
                        job.run( new CleanupJob.Executor()
                        {
                            @Override
                            public <T> CleanupJob.JobResult<T> submit( String jobDescription, Callable<T> job )
                            {
                                var future = executor.submit( job );
                                return future::get;
                            }
                        } );
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
            public PagedFile map( Path path, int pageSize, String databaseName, ImmutableSet<OpenOption> openOptions ) throws IOException
            {
                return new DelegatingPagedFile( super.map( path, pageSize, databaseName, openOptions ) )
                {
                    @Override
                    public PageCursor io( long pageId, int pf_flags, CursorContext context ) throws IOException
                    {
                        maybeThrow();
                        return super.io( pageId, pf_flags, context );
                    }

                    @Override
                    public void flushAndForce() throws IOException
                    {
                        maybeThrow();
                        super.flushAndForce();
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
            public PagedFile map( Path path, int pageSize, String databaseName, ImmutableSet<OpenOption> openOptions ) throws IOException
            {
                return new DelegatingPagedFile( super.map( path, pageSize, databaseName, openOptions ) )
                {
                    @Override
                    public PageCursor io( long pageId, int pf_flags, CursorContext context ) throws IOException
                    {
                        maybeBlock();
                        return super.io( pageId, pf_flags, context );
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
        try ( PageCache pageCache = createPageCache( defaultPageSize ) )
        {
            makeDirty( pageCache );
        }
    }

    private void makeDirty( PageCache pageCache ) throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index( pageCache ).build() )
        {
            // Make dirty
            index.writer( NULL ).close();
        }
    }

    private static void insert( GBPTree<MutableLong,MutableLong> index, long key, long value ) throws IOException
    {
        try ( Writer<MutableLong, MutableLong> writer = index.writer( NULL ) )
        {
            writer.put( new MutableLong( key ), new MutableLong( value ) );
        }
    }

    private static void shouldWait( Future<?> future )
    {
        assertThatThrownBy( () -> future.get( 200, TimeUnit.MILLISECONDS ), "Expected timeout" ).isInstanceOf( TimeoutException.class );
    }

    private PageCache createPageCache( int pageSize )
    {
        return PageCacheSupportExtension.getPageCache( fileSystem, config().withPageSize( pageSize ) );
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

    private GBPTreeBuilder<MutableLong,MutableLong> index( PageCache pageCache )
    {
        return new GBPTreeBuilder<>( pageCache, indexFile, layout );
    }

    private PageCache pageCacheWithBarrierInClose( final AtomicBoolean enabled, final Barrier.Control barrier )
    {
        return new DelegatingPageCache( createPageCache( defaultPageSize * 4 ) )
        {
            @Override
            public PagedFile map( Path path, int pageSize, String databaseName, ImmutableSet<OpenOption> openOptions ) throws IOException
            {
                return new DelegatingPagedFile( super.map( path, pageSize, databaseName, openOptions ) )
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

    private static Callable<List<CleanupJob>> startAndReturnStartedJobs( ControlledRecoveryCleanupWorkCollector collector )
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

    private static void assertFailedDueToUnmappedFile( Future<List<CleanupJob>> cleanJob )
            throws InterruptedException, ExecutionException
    {
        for ( CleanupJob job : cleanJob.get() )
        {
            assertTrue( job.hasFailed() );
            assertThat( job.getCause().getMessage() ).contains( "File" ).contains( "unmapped" );
        }
    }

    private Pair<TreeState,TreeState> captureTreeState( PageCache pageCache ) throws IOException
    {
        MutableObject<Pair<TreeState,TreeState>> state = new MutableObject<>();
        visitState(
                pageCache,
                indexFile,
                new GBPTreeVisitor.Adaptor<MutableLong,MutableLong>()
                {
                    @Override
                    public void treeState( Pair<TreeState,TreeState> statePair )
                    {
                        state.setValue( statePair );
                    }
                },
                "db",
                NULL );
        return state.getValue();
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
