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

import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.Hit;
import org.neo4j.index.Index;
import org.neo4j.index.IndexWriter;
import org.neo4j.index.ValueMerger;
import org.neo4j.index.ValueMergers;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static java.lang.String.format;
import static org.neo4j.index.gbptree.Generation.generation;
import static org.neo4j.index.gbptree.Generation.stableGeneration;
import static org.neo4j.index.gbptree.Generation.unstableGeneration;
import static org.neo4j.index.gbptree.PageCursorUtil.checkOutOfBounds;

/**
 * A generation-aware B+tree (GB+Tree) implementation directly atop a {@link PageCache} with no caching in between.
 * Additionally internal and leaf nodes on same level are linked both left and right (sibling pointers),
 * this to provide correct reading when concurrently {@link #writer() modifying}
 * the tree.
 * <p>
 * Generation is incremented on {@link Index#checkpoint(IOLimiter) check-pointing}.
 * Generation awareness allows for recovery from last {@link Index#checkpoint(IOLimiter)}, provided the same updates
 * will be replayed onto the index since that point in time.
 * <p>
 * Changes to tree nodes are made so that stable nodes (i.e. nodes that have survived at least one checkpoint)
 * are immutable w/ regards to keys values and child/sibling pointers.
 * Making a change in a stable node will copy the node to an unstable generation first and then make the change
 * in that unstable version. Further change in that node in the same generation will not require a copy since
 * it's already unstable.
 * <p>
 * Every pointer to another node (child/sibling pointer) consists of two pointers, one to a stable version and
 * one to a potentially unstable version. A stable -> unstable node copy will have its parent redirect one of its
 * two pointers to the new unstable version, redirecting readers and writers to the new unstable version,
 * while at the same time keeping one pointer to the stable version, in case there's a crash or non-clean
 * shutdown, followed by recovery.
 * <p>
 * Currently no leaves will be removed or merged as part of {@link IndexWriter#remove(Object) removals}.
 * <p>
 * A single writer w/ multiple concurrent readers is supported. Assuming usage adheres to this
 * constraint neither writer nor readers are blocking. Readers are virtually garbage-free.
 * <p>
 * An reader of GB+Tree is a {@link SeekCursor} that returns result as it finds them.
 * As the cursor move over keys/values, returned results are considered "behind" it
 * and likewise keys not yet returned "in front of".
 * Readers will always read latest written changes in front of it but will not see changes that appear behind.
 * The isolation level is thus read committed.
 * The tree have no knowledge about transactions and apply updates as isolated units of work one entry at the time.
 * Therefore, readers can see parts of transactions that are not fully applied yet.
 * <p>
 * A note on recovery:
 * <p>
 * {@link GBPTree} is designed to be able to handle non-clean shutdown / crash, but needs external help
 * in order to do so.
 * {@link #writer() Writes} happen to the tree and are made durable and
 * safe on next call to {@link #checkpoint(IOLimiter)}. Writes which happens after the last
 * {@link #checkpoint(IOLimiter)} are not safe if there's a {@link #close()} or JVM crash in between, i.e:
 *
 * <pre>
 * w: write
 * c: checkpoint
 * x: crash or {@link #close()}
 *
 * TIME |--w--w----w--c--ww--w-c-w--w-ww--w--w---x------|
 *         ^------ safe -----^   ^- unsafe --^
 * </pre>

 * The writes that happened before the last checkpoint are durable and safe, but the writes after it are not.
 * The tree can however get back to a consistent state by:
 * <ol>
 * <li>Creator of this tree detects that recovery is required (i.e. non-clean shutdown) and if so must call
 * {@link #prepareForRecovery()} ones, before any writes during recovery are made.</li>
 * <li>Replaying all the writes, exactly as they were made, since the last checkpoint all the way up
 * to the crash ({@code x}). Even including writes before the last checkpoint is OK, important is that
 * <strong>at least</strong> writes since last checkpoint are included.
 * </ol>
 *
 * Failure to follow the above steps will result in unknown state of the tree after a crash.
 * <p>
 * The reason as to why {@link #close()} doesn't do a checkpoint is that checkpointing as a whole should
 * be managed externally, keeping multiple resources in sync w/ regards to checkpoints.
 *
 * @param <KEY> type of keys
 * @param <VALUE> type of values
 */
public class GBPTree<KEY,VALUE> implements Index<KEY,VALUE>
{
    /**
     * Version of the format that makes up the tree. This includes:
     * <ul>
     * <li>{@link TreeNode} format, header, keys, children, values</li>
     * <li>{@link GenSafePointer} and {@link GenSafePointerPair}</li>
     * <li>{@link IdSpace} i.e. which pages are fixed</li>
     * <li>{@link TreeState} and {@link TreeStatePair}</li>
     * </ul>
     * If any of the above changes the on-page format then this version should be bumped, so that opening
     * an index on wrong format version fails and user will need to rebuild.
     */
    static final int FORMAT_VERSION = 1;

    /**
     * For monitoring {@link GBPTree}.
     */
    public interface Monitor
    {
        /**
         * Called when a {@link GBPTree#checkpoint(IOLimiter)} has been completed, but right before
         * {@link GBPTree#writer() writers} are re-enabled.
         */
        default void checkpointCompleted()
        {   // no-op by default
        }
    }

    /**
     * No-op {@link Monitor}.
     */
    public static final Monitor NO_MONITOR = new Monitor()
    {   // does nothing
    };

    /**
     * Paged file in a {@link PageCache} providing the means of storage.
     */
    private final PagedFile pagedFile;

    /**
     * {@link File} to map in {@link PageCache} for storing this tree.
     */
    private final File indexFile;

    /**
     * User-provided layout of key/value as well as custom additional meta information.
     * This allows for custom key/value and comparison representation. The layout provided during index
     * creation, i.e. the first time constructor is called for the given paged file, will be stored
     * in the meta page and it's asserted that the same layout is passed to the constructor when opening the tree.
     */
    private final Layout<KEY,VALUE> layout;

    /**
     * Instance of {@link TreeNode} which handles reading/writing physical bytes from pages representing tree nodes.
     */
    private final TreeNode<KEY,VALUE> bTreeNode;

    /**
     * A free-list of released ids. Acquiring new ids involves first trying out the free-list and then,
     * as a fall-back allocate a new id at the end of the store.
     */
    private final FreeListIdProvider freeList;

    /**
     * A single instance {@link IndexWriter} because tree only supports single writer.
     */
    private final SingleIndexWriter writer;

    /**
     * Check-pointing flushes updates to stable storage.
     * There's a critical section in check-pointing where, in order to guarantee a consistent check-pointed state
     * on stable storage, no writes are allowed to happen.
     * For this reason both writer and check-pointing acquires this lock.
     */
    private final Lock writerCheckpointMutex = new ReentrantLock();

    /**
     * Currently an index only supports one concurrent writer and so this boolean will act as
     * guard so that only one thread can have it at any given time.
     */
    private final AtomicBoolean writerTaken = new AtomicBoolean();

    /**
     * Page size, i.e. tree node size, of the tree nodes in this tree. The page size is determined on
     * tree creation, stored in meta page and read when opening tree later.
     */
    private int pageSize;

    /**
     * Whether or not the tree was created this time it was instantiated.
     */
    private boolean created;

    /**
     * Generation of the tree. This variable contains both stable and unstable generation and is
     * represented as one long to get atomic updates of both stable and unstable generation for readers.
     * Both stable and unstable generation are unsigned ints, i.e. 32 bits each.
     *
     * <ul>
     * <li>stable generation, generation which has survived the last {@link Index#checkpoint(IOLimiter)}</li>
     * <li>unstable generation, current generation under evolution. This generation will be the
     * {@link Generation#stableGeneration(long)} after the next {@link Index#checkpoint(IOLimiter)}</li>
     * </ul>
     */
    private volatile long generation;

    /**
     * Current root (id and generation where it was assigned). In the rare event of creating a new root
     * a new {@link Root} instance will be created and assigned to this variable.
     *
     * For reading id and generation atomically a reader can first grab a local reference to this variable
     * and then call {@link Root#id()} and {@link Root#generation()}, or use {@link Root#goTo(PageCursor)}
     * directly, which moves the page cursor to the id and returns the generation.
     */
    private volatile Root root;

    /**
     * Catchup for {@link SeekCursor} to become aware of new roots since it started.
     */
    private final Supplier<Root> rootCatchup = () -> root;

    /**
     * Supplier of generation to readers. This supplier will actually very rarely be used, because normally
     * a {@link SeekCursor} is bootstrapped from {@link #generation}. The only time this supplier will be
     * used is so that a long-running {@link SeekCursor} can keep up with a generation change after
     * a checkpoint, if the cursor lives that long.
     */
    private final LongSupplier generationSupplier = () -> generation;

    /**
     * Called on certain events.
     */
    private final Monitor monitor;

    /**
     * Opens an index {@code indexFile} in the {@code pageCache}, creating and initializing it if it doesn't exist.
     * If the index doesn't exist it will be created and the {@link Layout} and {@code pageSize} will
     * be written in index header.
     * If the index exists it will be opened and the {@link Layout} will be matched with the information
     * in the header. At the very least {@link Layout#identifier()} will be matched, but also if the
     * index has {@link Layout#writeMetaData(PageCursor)} additional meta data it will be
     * {@link Layout#readMetaData(PageCursor)}.
     *
     * @param pageCache {@link PageCache} to use to map index file
     * @param indexFile {@link File} containing the actual index
     * @param layout {@link Layout} to use in the tree, this must match the existing layout
     * we're just opening the index
     * @param tentativePageSize page size, i.e. tree node size. Must be less than or equal to that of the page cache.
     * A pageSize of {@code 0} means to use whatever the page cache has (at creation)
     * @param monitor {@link Monitor} for monitoring {@link GBPTree}.
     * @throws IOException on page cache error
     */
    public GBPTree( PageCache pageCache, File indexFile, Layout<KEY,VALUE> layout, int tentativePageSize,
            Monitor monitor ) throws IOException
    {
        this.indexFile = indexFile;
        this.monitor = monitor;
        this.generation = Generation.generation( GenSafePointer.MIN_GENERATION, GenSafePointer.MIN_GENERATION + 1 );
        long rootId = IdSpace.MIN_TREE_NODE_ID;
        setRoot( rootId, Generation.unstableGeneration( generation ) );
        this.layout = layout;
        this.pagedFile = openOrCreate( pageCache, indexFile, tentativePageSize, layout );
        this.bTreeNode = new TreeNode<>( pageSize, layout );
        this.freeList = new FreeListIdProvider( pagedFile, pageSize, rootId, FreeListIdProvider.NO_MONITOR );
        this.writer = new SingleIndexWriter( new InternalTreeLogic<>( freeList, bTreeNode, layout ) );

        if ( created )
        {
            initializeAfterCreation( layout );
        }
        else
        {
            loadState( pagedFile );
        }
    }

    private void initializeAfterCreation( Layout<KEY,VALUE> layout ) throws IOException
    {
        // Write meta
        writeMeta( layout, pagedFile );

        // Initialize index root node to a leaf node.
        try ( PageCursor cursor = openRootCursor( PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            long stableGeneration = stableGeneration( generation );
            long unstableGeneration = unstableGeneration( generation );
            bTreeNode.initializeLeaf( cursor, stableGeneration, unstableGeneration );
            checkOutOfBounds( cursor );
        }

        // Initialize free-list
        freeList.initializeAfterCreation();
        checkpoint( IOLimiter.unlimited() );
    }

    private PagedFile openOrCreate( PageCache pageCache, File indexFile,
            int pageSizeForCreation, Layout<KEY,VALUE> layout ) throws IOException
    {
        try
        {
            PagedFile pagedFile = pageCache.map( indexFile, pageCache.pageSize() );
            // This index already exists, verify the header with what we got passed into the constructor this time

            try
            {
                readMeta( indexFile, layout, pagedFile );
                pagedFile = mapWithCorrectPageSize( pageCache, indexFile, pagedFile );
                return pagedFile;
            }
            catch ( Throwable t )
            {
                try
                {
                    pagedFile.close();
                }
                catch ( IOException e )
                {
                    t.addSuppressed( e );
                }
                throw t;
            }
        }
        catch ( NoSuchFileException e )
        {
            // First time
            pageSize = pageSizeForCreation == 0 ? pageCache.pageSize() : pageSizeForCreation;
            if ( pageSize > pageCache.pageSize() )
            {
                throw new MetadataMismatchException( "Tree in " + indexFile.getAbsolutePath() +
                        " was about to be created with page size:" + pageSize +
                        ", but page cache used to create it has a smaller page size:" +
                        pageCache.pageSize() + " so cannot be created" );
            }

            // We need to create this index
            PagedFile pagedFile = pageCache.map( indexFile, pageSize, StandardOpenOption.CREATE );
            created = true;
            return pagedFile;
        }
    }

    private void loadState( PagedFile pagedFile ) throws IOException
    {
        Pair<TreeState,TreeState> states = readStatePages( pagedFile );
        TreeState state = TreeStatePair.selectNewestValidState( states );
        generation = Generation.generation( state.stableGeneration(), state.unstableGeneration() );
        setRoot( state.rootId(), state.rootGen() );

        long lastId = state.lastId();
        long freeListWritePageId = state.freeListWritePageId();
        long freeListReadPageId = state.freeListReadPageId();
        int freeListWritePos = state.freeListWritePos();
        int freeListReadPos = state.freeListReadPos();
        freeList.initialize( lastId, freeListWritePageId, freeListReadPageId, freeListWritePos, freeListReadPos );
    }

    private void writeState( PagedFile pagedFile ) throws IOException
    {
        Pair<TreeState,TreeState> states = readStatePages( pagedFile );
        TreeState oldestState = TreeStatePair.selectOldestOrInvalid( states );
        long pageToOverwrite = oldestState.pageId();
        Root root = this.root;
        try ( PageCursor cursor = pagedFile.io( pageToOverwrite, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            PageCursorUtil.goTo( cursor, "state page", pageToOverwrite );
            TreeState.write( cursor, stableGeneration( generation ), unstableGeneration( generation ),
                    root.id(), root.generation(),
                    freeList.lastId(), freeList.writePageId(), freeList.readPageId(),
                    freeList.writePos(), freeList.readPos() );
            checkOutOfBounds( cursor );
        }
    }

    private static Pair<TreeState,TreeState> readStatePages( PagedFile pagedFile ) throws IOException
    {
        Pair<TreeState,TreeState> states;
        try ( PageCursor cursor = pagedFile.io( 0L /*ignored*/, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            do
            {
                states = TreeStatePair.readStatePages(
                        cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B );
            }
            while ( cursor.shouldRetry() );
            checkOutOfBounds( cursor );
        }
        return states;
    }

    private static PageCursor openMetaPageCursor( PagedFile pagedFile, int pfFlags ) throws IOException
    {
        PageCursor metaCursor = pagedFile.io( IdSpace.META_PAGE_ID, pfFlags );
        PageCursorUtil.goTo( metaCursor, "meta page", IdSpace.META_PAGE_ID );
        return metaCursor;
    }

    private void readMeta( File indexFile, Layout<KEY,VALUE> layout, PagedFile pagedFile )
            throws IOException
    {
        // Read meta
        int formatVersion;
        long layoutIdentifier;
        int majorVersion;
        int minorVersion;
        try ( PageCursor metaCursor = openMetaPageCursor( pagedFile, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            do
            {
                formatVersion = metaCursor.getInt();
                pageSize = metaCursor.getInt();
                layoutIdentifier = metaCursor.getLong();
                majorVersion = metaCursor.getInt();
                minorVersion = metaCursor.getInt();
                layout.readMetaData( metaCursor );
            }
            while ( metaCursor.shouldRetry() );
            checkOutOfBounds( metaCursor );
        }
        if ( formatVersion != FORMAT_VERSION )
        {
            throw new MetadataMismatchException( "Tried to open %s with a different format version than " +
                    "what it was created with. Created with:%d, opened with %d",
                    indexFile, formatVersion, FORMAT_VERSION );
        }
        if ( layoutIdentifier != layout.identifier() )
        {
            throw new MetadataMismatchException( "Tried to open " + indexFile + " using different layout identifier " +
                    "than what it was created with. Created with:" + layoutIdentifier + ", opened with " +
                    layout.identifier() );
        }
        if ( majorVersion != layout.majorVersion() || minorVersion != layout.minorVersion() )
        {
            throw new MetadataMismatchException( "Tried to open " + indexFile + " using different layout version " +
                    "than what it was created with. Created with:" + majorVersion + "." + minorVersion +
                    ", opened with " + layout.majorVersion() + "." + layout.minorVersion() );
        }
    }

    private void writeMeta( Layout<KEY,VALUE> layout, PagedFile pagedFile ) throws IOException
    {
        try ( PageCursor metaCursor = openMetaPageCursor( pagedFile, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            metaCursor.putInt( FORMAT_VERSION );
            metaCursor.putInt( pageSize );
            metaCursor.putLong( layout.identifier() );
            metaCursor.putInt( layout.majorVersion() );
            metaCursor.putInt( layout.minorVersion() );
            layout.writeMetaData( metaCursor );
            checkOutOfBounds( metaCursor );
        }
    }

    private PagedFile mapWithCorrectPageSize( PageCache pageCache, File indexFile, PagedFile pagedFile )
            throws IOException
    {
        // This index was created with another page size, re-open with that actual page size
        if ( pageSize != pageCache.pageSize() )
        {
            if ( pageSize > pageCache.pageSize() )
            {
                throw new MetadataMismatchException( "Tree in " + indexFile.getAbsolutePath() +
                        " was created with page size:" + pageSize +
                        ", but page cache used to open it this time has a smaller page size:" +
                        pageCache.pageSize() + " so cannot be opened" );
            }
            pagedFile.close();
            return pageCache.map( indexFile, pageSize );
        }
        return pagedFile;
    }

    /**
     * Utility for {@link PagedFile#io(long, int) acquiring} a new {@link PageCursor},
     * placed at the current root id and which have had its {@link PageCursor#next()} called-
     *
     * @param pfFlags flags sent into {@link PagedFile#io(long, int)}.
     * @return {@link PageCursor} result from call to {@link PagedFile#io(long, int)} after it has been
     * placed at the current root and has had {@link PageCursor#next()} called.
     * @throws IOException on {@link PageCursor} error.
     */
    private PageCursor openRootCursor( int pfFlags ) throws IOException
    {
        PageCursor cursor = pagedFile.io( 0L /*Ignored*/, pfFlags );
        root.goTo( cursor );
        return cursor;
    }

    @Override
    public RawCursor<Hit<KEY,VALUE>,IOException> seek( KEY fromInclusive, KEY toExclusive ) throws IOException
    {
        KEY key = layout.newKey();
        VALUE value = layout.newValue();
        long generation = this.generation;
        long stableGeneration = stableGeneration( generation );
        long unstableGeneration = unstableGeneration( generation );

        PageCursor cursor = pagedFile.io( 0L /*ignored*/, PagedFile.PF_SHARED_READ_LOCK );
        long rootGen = root.goTo( cursor );

        // Returns cursor which is now initiated with left-most leaf node for the specified range
        return new SeekCursor<>( cursor, key, value, bTreeNode, fromInclusive, toExclusive, layout,
                stableGeneration, unstableGeneration, generationSupplier, rootCatchup, rootGen );
    }

    @Override
    public void checkpoint( IOLimiter ioLimiter ) throws IOException
    {
        // Flush dirty pages of the tree, do this before acquiring the lock so that writers won't be
        // blocked while we do this
        pagedFile.flushAndForce( ioLimiter );

        // Block writers, or if there's a current writer then wait for it to complete and then block
        // From this point and till the lock is released we know that the tree won't change.
        writerCheckpointMutex.lock();
        try
        {
            // Flush dirty pages since that last flush above. This should be a very small set of pages
            // and should be rather fast. In here writers are blocked and we want to minimize this
            // windows of time as much as possible, that's why there's an initial flush outside this lock.
            pagedFile.flushAndForce();

            // Increment generation, i.e. stable becomes current unstable and unstable increments by one
            // and write the tree state (rootId, lastId, generation a.s.o.) to state page.
            long unstableGeneration = unstableGeneration( generation );
            generation = Generation.generation( unstableGeneration, unstableGeneration + 1 );
            writeState( pagedFile );

            // Flush the state page.
            pagedFile.flushAndForce();

            // Expose this fact.
            monitor.checkpointCompleted();
        }
        finally
        {
            // Unblock writers, any writes after this point and up until the next checkpoint will have
            // the new unstable generation.
            writerCheckpointMutex.unlock();
        }
    }

    @Override
    public void close() throws IOException
    {
        pagedFile.close();
    }

    /**
     * @return the single {@link IndexWriter} for this index. The returned writer must be
     * {@link IndexWriter#close() closed} before another caller can acquire this writer.
     * @throws IllegalStateException for calls made between a successful call to this method and closing the
     * returned writer.
     */
    @Override
    public IndexWriter<KEY,VALUE> writer() throws IOException
    {
        if ( !writerTaken.compareAndSet( false, true ) )
        {
            throw new IllegalStateException( "Writer in " + this + " is already acquired by someone else. " +
                    "Only a single writer is allowed. The writer will become available as soon as " +
                    "acquired writer is closed" );
        }

        writerCheckpointMutex.lock();
        boolean success = false;
        try
        {
            writer.take();
            success = true;
            return writer;
        }
        finally
        {
            if ( !success )
            {
                releaseWriter();
            }
        }
    }

    private void releaseWriter()
    {
        writerCheckpointMutex.unlock();
        if ( !writerTaken.compareAndSet( true, false ) )
        {
            throw new IllegalStateException( "Tried to give back the writer of " + this +
                    ", but somebody else already did" );
        }
    }

    private void setRoot( long rootId, long rootGeneration )
    {
        this.root = new Root( rootId, rootGeneration );
    }

    /**
     * {@link GBPTree} class-level javadoc mentions how this method interacts with recovery,
     * it's an essential piece to be able to recover properly and must be called when external party
     * detects that recovery is required, before re-applying the recovered updates.
     *
     * @throws IOException on {@link PageCache} error.
     */
    public void prepareForRecovery() throws IOException
    {
        // Increment unstable generation, widening the gap between stable and unstable generation
        // so that generations in between are considered crash generation(s).
        generation = generation( stableGeneration( generation ), unstableGeneration( generation ) + 1 );
        writeState( pagedFile );
        pagedFile.flushAndForce();
    }

    void printTree() throws IOException
    {
        printTree( true );
    }

    // Utility method
    void printTree( boolean printValues ) throws IOException
    {
        try ( PageCursor cursor = openRootCursor( PagedFile.PF_SHARED_READ_LOCK ) )
        {
            TreePrinter.printTree( cursor, bTreeNode, layout,
                    stableGeneration( generation ), unstableGeneration( generation ), System.out, printValues );
        }
    }

    // Utility method
    boolean consistencyCheck() throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( 0L /*ignored*/, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            long unstableGeneration = unstableGeneration( generation );
            ConsistencyChecker<KEY> consistencyChecker = new ConsistencyChecker<>( bTreeNode, layout,
                    stableGeneration( generation ), unstableGeneration );

            long rootGen = root.goTo( cursor );
            boolean check = consistencyChecker.check( cursor, rootGen );
            root.goTo( cursor );

            PrimitiveLongSet freelistIds = Primitive.longSet();
            freeList.visitFreelistPageIds( freelistIds::add );
            freeList.visitUnacquiredIds( freelistIds::add, unstableGeneration );
            boolean checkSpace = consistencyChecker.checkSpace( cursor, freeList.lastId(), freelistIds.iterator() );

            return check & checkSpace;
        }
    }

    @Override
    public String toString()
    {
        long generation = this.generation;
        return format( "GB+Tree[file:%s, layout:%s, gen:%d/%d]",
                indexFile.getAbsolutePath(), layout,
                stableGeneration( generation ), unstableGeneration( generation ) );
    }

    private class SingleIndexWriter implements IndexWriter<KEY,VALUE>
    {
        private final InternalTreeLogic<KEY,VALUE> treeLogic;
        private final StructurePropagation<KEY> structurePropagation;
        private PageCursor cursor;

        // Writer can't live past a checkpoint because of the mutex with checkpoint,
        // therefore safe to locally cache these generation fields from the volatile generation in the tree
        private long stableGeneration;
        private long unstableGeneration;

        SingleIndexWriter( InternalTreeLogic<KEY,VALUE> treeLogic )
        {
            this.structurePropagation = new StructurePropagation<>( layout.newKey() );
            this.treeLogic = treeLogic;
        }

        void take() throws IOException
        {
            cursor = openRootCursor( PagedFile.PF_SHARED_WRITE_LOCK );
            stableGeneration = stableGeneration( generation );
            unstableGeneration = unstableGeneration( generation );
            treeLogic.initialize( cursor );
        }

        @Override
        public void put( KEY key, VALUE value ) throws IOException
        {
            merge( key, value, ValueMergers.overwrite() );
        }

        @Override
        public void merge( KEY key, VALUE value, ValueMerger<VALUE> valueMerger ) throws IOException
        {
            treeLogic.insert( cursor, structurePropagation, key, value, valueMerger,
                    stableGeneration, unstableGeneration );

            if ( structurePropagation.hasSplit )
            {
                // New root
                long newRootId = freeList.acquireNewId( stableGeneration, unstableGeneration );
                PageCursorUtil.goTo( cursor, "new root", newRootId );

                bTreeNode.initializeInternal( cursor, stableGeneration, unstableGeneration );
                bTreeNode.insertKeyAt( cursor, structurePropagation.primKey, 0, 0 );
                bTreeNode.setKeyCount( cursor, 1 );
                bTreeNode.setChildAt( cursor, structurePropagation.left, 0, stableGeneration, unstableGeneration );
                bTreeNode.setChildAt( cursor, structurePropagation.right, 1, stableGeneration, unstableGeneration );
                setRoot( newRootId );
            }
            else if ( structurePropagation.hasNewGen )
            {
                setRoot( structurePropagation.left );
            }
            structurePropagation.clear();

            checkOutOfBounds( cursor );
        }

        private void setRoot( long rootId )
        {
            GBPTree.this.setRoot( rootId, unstableGeneration );
            treeLogic.initialize( cursor );
        }

        @Override
        public VALUE remove( KEY key ) throws IOException
        {
            VALUE result = treeLogic.remove( cursor, structurePropagation, key, layout.newValue(),
                    stableGeneration, unstableGeneration );
            if ( structurePropagation.hasNewGen )
            {
                setRoot( structurePropagation.left );
            }
            structurePropagation.clear();

            checkOutOfBounds( cursor );
            return result;
        }

        @Override
        public void close() throws IOException
        {
            if ( cursor == null )
            {
                return;
            }

            cursor.close();
            cursor = null;
            releaseWriter();
        }
    }
}
