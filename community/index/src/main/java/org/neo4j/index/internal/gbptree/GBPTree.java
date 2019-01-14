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

import org.apache.commons.lang3.tuple.Pair;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.cursor.RawCursor;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static java.lang.String.format;
import static org.neo4j.helpers.Exceptions.withMessage;
import static org.neo4j.index.internal.gbptree.Generation.generation;
import static org.neo4j.index.internal.gbptree.Generation.stableGeneration;
import static org.neo4j.index.internal.gbptree.Generation.unstableGeneration;
import static org.neo4j.index.internal.gbptree.GenerationSafePointer.MIN_GENERATION;
import static org.neo4j.index.internal.gbptree.Header.CARRY_OVER_PREVIOUS_HEADER;
import static org.neo4j.index.internal.gbptree.Header.replace;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.checkOutOfBounds;
import static org.neo4j.index.internal.gbptree.PointerChecking.assertNoSuccessor;

/**
 * A generation-aware B+tree (GB+Tree) implementation directly atop a {@link PageCache} with no caching in between.
 * Additionally internal and leaf nodes on same level are linked both left and right (sibling pointers),
 * this to provide correct reading when concurrently {@link #writer() modifying}
 * the tree.
 * <p>
 * Generation is incremented on {@link #checkpoint(IOLimiter) check-pointing}.
 * Generation awareness allows for recovery from last {@link #checkpoint(IOLimiter)}, provided the same updates
 * will be replayed onto the index since that point in time.
 * <p>
 * Changes to tree nodes are made so that stable nodes (i.e. nodes that have survived at least one checkpoint)
 * are immutable w/ regards to keys values and child/sibling pointers.
 * Making a change in a stable node will copy the node to an unstable generation first and then make the change
 * in that unstable version. Further change in that node in the same generation will not require a copy since
 * it's already unstable.
 * <p>
 * Every pointer to another node (child/sibling pointer) consists of two pointers, one to a stable version and
 * one to a potentially unstable version. A stable -&gt; unstable node copy will have its parent redirect one of its
 * two pointers to the new unstable version, redirecting readers and writers to the new unstable version,
 * while at the same time keeping one pointer to the stable version, in case there's a crash or non-clean
 * shutdown, followed by recovery.
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
 * The tree can however get back to a consistent state by replaying all the writes since the last checkpoint
 * all the way up to the crash ({@code x}). Even including writes before the last checkpoint is OK,
 * important is that <strong>at least</strong> writes since last checkpoint are included. Note that the order
 * in which the changes are applied is not important as long as they do not affect the same key. The order of
 * updates targeting the same key needs to be preserved when replaying as only the last applied update will
 * be visible.
 *
 * If failing to replay missing writes, that data will simply be missing from the tree and most likely leave the
 * database inconsistent.
 * <p>
 * The reason as to why {@link #close()} doesn't do a checkpoint is that checkpointing as a whole should
 * be managed externally, keeping multiple resources in sync w/ regards to checkpoints. This is especially important
 * since a it is impossible to recognize crashed pointers after a checkpoint.
 *
 * @param <KEY> type of keys
 * @param <VALUE> type of values
 */
public class GBPTree<KEY,VALUE> implements Closeable
{
    /**
     * For monitoring {@link GBPTree}.
     */
    public interface Monitor
    {
        /**
         * Adapter for {@link Monitor}.
         */
        class Adaptor implements Monitor
        {
            @Override
            public void checkpointCompleted()
            {   // no-op
            }

            @Override
            public void noStoreFile()
            {   // no-op
            }

            @Override
            public void cleanupRegistered()
            {   // no-op
            }

            @Override
            public void cleanupStarted()
            {   // no-op
            }

            @Override
            public void cleanupFinished( long numberOfPagesVisited, long numberOfCleanedCrashPointers, long durationMillis )
            {   // no-op
            }

            @Override
            public void cleanupClosed()
            {   // no-op
            }

            @Override
            public void cleanupFailed( Throwable throwable )
            {   // no-op
            }

            @Override
            public void startupState( boolean clean )
            {   // no-op
            }
        }

        /**
         * Called when a {@link GBPTree#checkpoint(IOLimiter)} has been completed, but right before
         * {@link GBPTree#writer() writers} are re-enabled.
         */
        void checkpointCompleted();

        /**
         * Called when the tree was started on no existing store file and so will be created.
         */
        void noStoreFile();

        /**
         * Called after cleanup job has been created
         */
        void cleanupRegistered();

        /**
         * Called after cleanup job has been started
         */
        void cleanupStarted();

        /**
         * Called after recovery has completed and cleaning has been done.
         *
         * @param numberOfPagesVisited number of pages visited by the cleaner.
         * @param numberOfCleanedCrashPointers number of cleaned crashed pointers.
         * @param durationMillis time spent cleaning.
         */
        void cleanupFinished( long numberOfPagesVisited, long numberOfCleanedCrashPointers, long durationMillis );

        /**
         * Called when cleanup job is closed and lock is released
         */
        void cleanupClosed();

        /**
         * Called when cleanup job catches a throwable
         * @param throwable cause of failure
         */
        void cleanupFailed( Throwable throwable );

        /**
         * Report tree state on startup.
         *
         * @param clean true if tree was clean on startup.
         */
        void startupState( boolean clean );
    }

    /**
     * No-op {@link Monitor}.
     */
    public static final Monitor NO_MONITOR = new Monitor.Adaptor();

    /**
     * No-op header reader.
     */
    public static final Header.Reader NO_HEADER_READER = headerData ->
    {
    };

    /**
     * No-op header writer.
     */
    public static final Consumer<PageCursor> NO_HEADER_WRITER = pc ->
    {
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
     * A single instance {@link Writer} because tree only supports single writer.
     */
    private final SingleWriter writer;

    /**
     * Tells whether or not there have been made changes (using {@link #writer()}) to this tree
     * since last call to {@link #checkpoint(IOLimiter)}. This variable is set when calling {@link #writer()}
     * and cleared inside {@link #checkpoint(IOLimiter)}.
     */
    private volatile boolean changesSinceLastCheckpoint;

    /**
     * Lock with two individual parts. Writer lock and cleaner lock.
     * <p>
     * There are a few different scenarios that involve writing or flushing that can not be happen concurrently:
     * <ul>
     *     <li>Checkpoint and writing</li>
     *     <li>Checkpoint and close</li>
     *     <li>Write and checkpoint</li>
     * </ul>
     * For those scenarios, writer lock is taken.
     * <p>
     * If cleaning of crash pointers is needed the tree can not be allowed to perform a checkpoint until that job
     * has finished. For this scenario, cleaner lock is taken.
     */
    private final GBPTreeLock lock = new GBPTreeLock();

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
     * <li>stable generation, generation which has survived the last {@link #checkpoint(IOLimiter)}</li>
     * <li>unstable generation, current generation under evolution. This generation will be the
     * {@link Generation#stableGeneration(long)} after the next {@link #checkpoint(IOLimiter)}</li>
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
     * Whether or not this tree has been closed. Accessed and changed solely in
     * {@link #close()} to be able to close tree multiple times gracefully.
     */
    private boolean closed = true;

    /**
     * True if tree is clean, false if dirty
     */
    private boolean clean;

    /**
     * True if initial tree state was dirty
     */
    private boolean dirtyOnStartup;

    /**
     * State of cleanup job.
     */
    private final CleanupJob cleaning;

    /**
     * {@link Consumer} to hand out to others who want to decorate information about this tree
     * to exceptions thrown out from its surface.
     */
    private final Consumer<Throwable> exceptionDecorator = this::appendTreeInformation;

    /**
     * Opens an index {@code indexFile} in the {@code pageCache}, creating and initializing it if it doesn't exist.
     * If the index doesn't exist it will be created and the {@link Layout} and {@code pageSize} will
     * be written in index header.
     * If the index exists it will be opened and the {@link Layout} will be matched with the information
     * in the header. At the very least {@link Layout#identifier()} will be matched.
     * <p>
     * On start, tree can be in a clean or dirty state. If dirty, it will
     * {@link #createCleanupJob(RecoveryCleanupWorkCollector, boolean)} and clean crashed pointers as part of constructor. Tree is only clean if
     * since last time it was opened it was {@link #close() closed} without any non-checkpointed changes present.
     * Correct usage pattern of the GBPTree is:
     *
     * <pre>
     *     try ( GBPTree tree = new GBPTree(...) )
     *     {
     *         // Use the tree
     *         tree.checkpoint( ... );
     *     }
     * </pre>
     *
     * Expected state after first time tree is opened, where initial state is created:
     * <ul>
     * <li>StateA
     * <ul>
     * <li>stableGeneration=2</li>
     * <li>unstableGeneration=3</li>
     * <li>rootId=3</li>
     * <li>rootGeneration=2</li>
     * <li>lastId=4</li>
     * <li>freeListWritePageId=4</li>
     * <li>freeListReadPageId=4</li>
     * <li>freeListWritePos=0</li>
     * <li>freeListReadPos=0</li>
     * <li>clean=false</li>
     * </ul>
     * <li>StateB
     * <ul>
     * <li>stableGeneration=2</li>
     * <li>unstableGeneration=4</li>
     * <li>rootId=3</li>
     * <li>rootGeneration=2</li>
     * <li>lastId=4</li>
     * <li>freeListWritePageId=4</li>
     * <li>freeListReadPageId=4</li>
     * <li>freeListWritePos=0</li>
     * <li>freeListReadPos=0</li>
     * <li>clean=false</li>
     * </ul>
     * </ul>
     *
     * @param pageCache {@link PageCache} to use to map index file
     * @param indexFile {@link File} containing the actual index
     * @param layout {@link Layout} to use in the tree, this must match the existing layout
     * we're just opening the index
     * @param tentativePageSize page size, i.e. tree node size. Must be less than or equal to that of the page cache.
     * A pageSize of {@code 0} means to use whatever the page cache has (at creation)
     * @param monitor {@link Monitor} for monitoring {@link GBPTree}.
     * @param headerReader reads header data, previously written using {@link #checkpoint(IOLimiter, Consumer)}
     * or {@link #close()}
     * @param headerWriter writes header data if indexFile is created as a result of this call.
     * @param recoveryCleanupWorkCollector collects recovery cleanup jobs for execution after recovery.
     * @throws IOException on page cache error
     * @throws MetadataMismatchException if meta information does not match constructor parameters or meta page is missing
     */
    public GBPTree( PageCache pageCache, File indexFile, Layout<KEY,VALUE> layout, int tentativePageSize,
            Monitor monitor, Header.Reader headerReader, Consumer<PageCursor> headerWriter,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector ) throws IOException, MetadataMismatchException
    {
        this.indexFile = indexFile;
        this.monitor = monitor;
        this.generation = Generation.generation( MIN_GENERATION, MIN_GENERATION + 1 );
        long rootId = IdSpace.MIN_TREE_NODE_ID;
        setRoot( rootId, Generation.unstableGeneration( generation ) );
        this.layout = layout;

        boolean success = false;
        try
        {
            this.pagedFile = openOrCreate( pageCache, indexFile, tentativePageSize );
            this.pageSize = pagedFile.pageSize();
            closed = false;
            TreeNodeSelector.Factory format;
            if ( created )
            {
                format = TreeNodeSelector.selectByLayout( layout );
                writeMeta( layout, format, pagedFile );
            }
            else
            {
                Meta meta = readMeta( layout, pagedFile );
                meta.verify( layout );
                format = TreeNodeSelector.selectByFormat( meta.getFormatIdentifier(), meta.getFormatVersion() );
            }
            this.bTreeNode = format.create( pageSize, layout );
            this.freeList = new FreeListIdProvider( pagedFile, pageSize, rootId, FreeListIdProvider.NO_MONITOR );
            this.writer = new SingleWriter( new InternalTreeLogic<>( freeList, bTreeNode, layout ) );

            // Create or load state
            if ( created )
            {
                initializeAfterCreation( headerWriter );
            }
            else
            {
                loadState( pagedFile, headerReader );
            }
            this.monitor.startupState( clean );

            // Prepare tree for action
            dirtyOnStartup = !clean;
            clean = false;
            bumpUnstableGeneration();
            forceState();
            cleaning = createCleanupJob( recoveryCleanupWorkCollector, dirtyOnStartup );
            success = true;
        }
        catch ( Throwable t )
        {
            appendTreeInformation( t );
            throw t;
        }
        finally
        {
            if ( !success )
            {
                close();
            }
        }
    }

    private void initializeAfterCreation( Consumer<PageCursor> headerWriter ) throws IOException
    {
        // Initialize state
        try ( PageCursor cursor = pagedFile.io( 0 /*ignored*/, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            TreeStatePair.initializeStatePages( cursor );
        }

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
        changesSinceLastCheckpoint = true;

        // Checkpoint to make the created root node stable. Forcing tree state also piggy-backs on this.
        checkpoint( IOLimiter.unlimited(), headerWriter );
        clean = true;
    }

    private PagedFile openOrCreate( PageCache pageCache, File indexFile,
            int pageSizeForCreation ) throws IOException, MetadataMismatchException
    {
        try
        {
            return openExistingIndexFile( pageCache, indexFile );
        }
        catch ( NoSuchFileException e )
        {
            return createNewIndexFile( pageCache, indexFile, pageSizeForCreation );
        }
    }

    private static PagedFile openExistingIndexFile( PageCache pageCache, File indexFile ) throws IOException, MetadataMismatchException
    {
        PagedFile pagedFile = pageCache.map( indexFile, pageCache.pageSize() );
        // This index already exists, verify meta data aligns with expectations

        boolean success = false;
        try
        {
            // We're only interested in the page size really, so don't involve layout at this point
            Meta meta = readMeta( null, pagedFile );
            pagedFile = mapWithCorrectPageSize( pageCache, indexFile, pagedFile, meta.getPageSize() );
            success = true;
            return pagedFile;
        }
        catch ( IllegalStateException e )
        {
            throw new MetadataMismatchException( "Index is not fully initialized since it's missing the meta page", e );
        }
        finally
        {
            if ( !success )
            {
                pagedFile.close();
            }
        }
    }

    private PagedFile createNewIndexFile( PageCache pageCache, File indexFile, int pageSizeForCreation ) throws IOException
    {
        // First time
        monitor.noStoreFile();
        int pageSize = pageSizeForCreation == 0 ? pageCache.pageSize() : pageSizeForCreation;
        if ( pageSize > pageCache.pageSize() )
        {
            throw new MetadataMismatchException(
                    "Tried to create tree with page size %d" +
                    ", but page cache used to create it has a smaller page size %d" +
                    " so cannot be created", pageSize, pageCache.pageSize() );
        }

        // We need to create this index
        PagedFile pagedFile = pageCache.map( indexFile, pageSize, StandardOpenOption.CREATE );
        created = true;
        return pagedFile;
    }

    private void loadState( PagedFile pagedFile, Header.Reader headerReader ) throws IOException
    {
        Pair<TreeState,TreeState> states = loadStatePages( pagedFile );
        TreeState state = TreeStatePair.selectNewestValidState( states );
        try ( PageCursor cursor = pagedFile.io( state.pageId(), PagedFile.PF_SHARED_READ_LOCK ) )
        {
            PageCursorUtil.goTo( cursor, "header data", state.pageId() );
            doReadHeader( headerReader, cursor );
        }
        generation = Generation.generation( state.stableGeneration(), state.unstableGeneration() );
        setRoot( state.rootId(), state.rootGeneration() );

        long lastId = state.lastId();
        long freeListWritePageId = state.freeListWritePageId();
        long freeListReadPageId = state.freeListReadPageId();
        int freeListWritePos = state.freeListWritePos();
        int freeListReadPos = state.freeListReadPos();
        freeList.initialize( lastId, freeListWritePageId, freeListReadPageId, freeListWritePos, freeListReadPos );
        clean = state.isClean();
    }

    /**
     * Use when you are only interested in reading the header of existing index file without opening the index for writes.
     * Useful when reading header and the demands on matching layout can be relaxed a bit.
     *
     * @param pageCache {@link PageCache} to use to map index file
     * @param indexFile {@link File} containing the actual index
     * @param headerReader reads header data, previously written using {@link #checkpoint(IOLimiter, Consumer)}
     * or {@link #close()}
     * @throws IOException On page cache error
     * @throws MetadataMismatchException if some meta page is missing (tree not fully initialized)
     */
    public static void readHeader( PageCache pageCache, File indexFile, Header.Reader headerReader )
            throws IOException, MetadataMismatchException
    {
        try ( PagedFile pagedFile = openExistingIndexFile( pageCache, indexFile ) )
        {
            Pair<TreeState,TreeState> states = loadStatePages( pagedFile );
            TreeState state = TreeStatePair.selectNewestValidState( states );
            try ( PageCursor cursor = pagedFile.io( state.pageId(), PagedFile.PF_SHARED_READ_LOCK ) )
            {
                PageCursorUtil.goTo( cursor, "header data", state.pageId() );
                doReadHeader( headerReader, cursor );
            }
        }
        catch ( Throwable t )
        {
            // Decorate outgoing exceptions with basic tree information. This is similar to how the constructor
            // appends its information, but the constructor has read more information at that point so this one
            // is a bit more sparse on information.
            withMessage( t, t.getMessage() + " | " + format( "GBPTree[file:%s]", indexFile ) );
            throw t;
        }
    }

    private static void doReadHeader( Header.Reader headerReader, PageCursor cursor ) throws IOException
    {
        int headerDataLength;
        do
        {
            TreeState.read( cursor );
            headerDataLength = cursor.getInt();
        }
        while ( cursor.shouldRetry() );

        int headerDataOffset = cursor.getOffset();
        byte[] headerDataBytes = new byte[headerDataLength];
        do
        {
            cursor.setOffset( headerDataOffset );
            cursor.getBytes( headerDataBytes );
        }
        while ( cursor.shouldRetry() );

        headerReader.read( ByteBuffer.wrap( headerDataBytes ) );
    }

    private void writeState( PagedFile pagedFile, Header.Writer headerWriter ) throws IOException
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
                    freeList.writePos(), freeList.readPos(), clean );

            writerHeader( pagedFile, headerWriter, other( states, oldestState ), cursor );

            checkOutOfBounds( cursor );
        }
    }

    private static void writerHeader( PagedFile pagedFile, Header.Writer headerWriter,
            TreeState otherState, PageCursor cursor ) throws IOException
    {
        // Write/carry over header
        int headerOffset = cursor.getOffset();
        int headerDataOffset = headerOffset + Integer.BYTES; // will contain length of written header data (below)
        if ( otherState.isValid() || headerWriter != CARRY_OVER_PREVIOUS_HEADER )
        {
            PageCursor previousCursor = pagedFile.io( otherState.pageId(), PagedFile.PF_SHARED_READ_LOCK );
            PageCursorUtil.goTo( previousCursor, "previous state page", otherState.pageId() );
            checkOutOfBounds( cursor );
            do
            {
                // Clear any out-of-bounds from prior attempts
                cursor.checkAndClearBoundsFlag();
                // Place the previous state cursor after state data
                TreeState.read( previousCursor );
                // Read length of previous header
                int previousLength = previousCursor.getInt();
                // Reserve space to store length
                cursor.setOffset( headerDataOffset );
                // Write
                headerWriter.write( previousCursor, previousLength, cursor );
            }
            while ( previousCursor.shouldRetry() );
            checkOutOfBounds( previousCursor );
            checkOutOfBounds( cursor );

            int length = cursor.getOffset() - headerDataOffset;
            cursor.putInt( headerOffset, length );
        }
    }

    private static TreeState other( Pair<TreeState,TreeState> states, TreeState state )
    {
        return states.getLeft() == state ? states.getRight() : states.getLeft();
    }

    /**
     * Basically {@link #readStatePages(PagedFile)} with some more checks, suitable for when first opening an index file,
     * not while running it and check pointing.
     *
     * @param pagedFile {@link PagedFile} to read the state pages from.
     * @return both read state pages.
     * @throws MetadataMismatchException if state pages are missing (file is smaller than that) or if they are both empty.
     * @throws IOException on {@link PageCursor} error.
     */
    private static Pair<TreeState,TreeState> loadStatePages( PagedFile pagedFile ) throws MetadataMismatchException, IOException
    {
        try
        {
            Pair<TreeState,TreeState> states = readStatePages( pagedFile );
            if ( states.getLeft().isEmpty() && states.getRight().isEmpty() )
            {
                throw new MetadataMismatchException( "Index is not fully initialized since its state pages are empty" );
            }
            return states;
        }
        catch ( IllegalStateException e )
        {
            throw new MetadataMismatchException( "Index is not fully initialized since it's missing state pages", e );
        }
    }

    private static Pair<TreeState,TreeState> readStatePages( PagedFile pagedFile ) throws IOException
    {
        Pair<TreeState,TreeState> states;
        try ( PageCursor cursor = pagedFile.io( 0L /*ignored*/, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            states = TreeStatePair.readStatePages(
                    cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B );
        }
        return states;
    }

    private static PageCursor openMetaPageCursor( PagedFile pagedFile, int pfFlags ) throws IOException
    {
        PageCursor metaCursor = pagedFile.io( IdSpace.META_PAGE_ID, pfFlags );
        PageCursorUtil.goTo( metaCursor, "meta page", IdSpace.META_PAGE_ID );
        return metaCursor;
    }

    private static <KEY,VALUE> Meta readMeta( Layout<KEY,VALUE> layout, PagedFile pagedFile )
            throws IOException
    {
        try ( PageCursor metaCursor = openMetaPageCursor( pagedFile, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            return Meta.read( metaCursor, layout );
        }
    }

    private void writeMeta( Layout<KEY,VALUE> layout, TreeNodeSelector.Factory format, PagedFile pagedFile ) throws IOException
    {
        Meta meta = new Meta( format.formatIdentifier(), format.formatVersion(), pageSize, layout );
        try ( PageCursor metaCursor = openMetaPageCursor( pagedFile, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            meta.write( metaCursor, layout );
        }
    }

    private static PagedFile mapWithCorrectPageSize( PageCache pageCache, File indexFile, PagedFile pagedFile, int pageSize )
            throws IOException
    {
        // This index was created with another page size, re-open with that actual page size
        if ( pageSize != pageCache.pageSize() )
        {
            if ( pageSize > pageCache.pageSize() || pageSize < 0 )
            {
                throw new MetadataMismatchException(
                        "Tried to create tree with page size %d, but page cache used to open it this time " +
                        "has a smaller page size %d so cannot be opened",
                        pageSize, pageCache.pageSize() );
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

    /**
     * Seeks hits in this tree, given a key range. Hits are iterated over using the returned {@link RawCursor}.
     * There's no guarantee that neither the {@link Hit} nor key/value instances are immutable and so
     * if caller wants to cache the results it's safest to copy the instances, or rather their contents,
     * into its own result cache.
     * <p>
     * Seeks can go either forwards or backwards depending on the values of the key arguments.
     * <ul>
     * <li>
     * A {@code fromInclusive} that is smaller than the {@code toExclusive} results in results in ascending order.
     * </li>
     * <li>
     * A {@code fromInclusive} that is bigger than the {@code toExclusive} results in results in descending order.
     * </li>
     * </ul>
     *
     * @param fromInclusive lower bound of the range to seek (inclusive).
     * @param toExclusive higher bound of the range to seek (exclusive).
     * @return a {@link RawCursor} used to iterate over the hits within the specified key range.
     * @throws IOException on error reading from index.
     */
    public RawCursor<Hit<KEY,VALUE>,IOException> seek( KEY fromInclusive, KEY toExclusive ) throws IOException
    {
        long generation = this.generation;
        long stableGeneration = stableGeneration( generation );
        long unstableGeneration = unstableGeneration( generation );

        PageCursor cursor = pagedFile.io( 0L /*ignored*/, PagedFile.PF_SHARED_READ_LOCK );
        long rootGeneration = root.goTo( cursor );

        // Returns cursor which is now initiated with left-most leaf node for the specified range
        return new SeekCursor<>( cursor, bTreeNode, fromInclusive, toExclusive, layout,
                stableGeneration, unstableGeneration, generationSupplier, rootCatchup, rootGeneration,
                exceptionDecorator, SeekCursor.DEFAULT_MAX_READ_AHEAD );
    }

    /**
     * Checkpoints and flushes any pending changes to storage. After a successful call to this method
     * the data is durable and safe. {@link #writer() Changes} made after this call and until crashing or
     * otherwise non-clean shutdown (by omitting calling checkpoint before {@link #close()}) will need to be replayed
     * next time this tree is opened.
     * <p>
     * Header writer is expected to leave consumed {@link PageCursor} at end of written header for calculation of
     * header size.
     *
     * @param ioLimiter for controlling I/O usage.
     * @param headerWriter hook for writing header data, must leave cursor at end of written header.
     * @throws IOException on error flushing to storage.
     */
    public void checkpoint( IOLimiter ioLimiter, Consumer<PageCursor> headerWriter ) throws IOException
    {
        checkpoint( ioLimiter, replace( headerWriter ) );
    }

    /**
     * Performs a {@link #checkpoint(IOLimiter, Consumer) check point}, keeping any header information
     * written in previous check point.
     *
     * @param ioLimiter for controlling I/O usage.
     * @throws IOException on error flushing to storage.
     * @see #checkpoint(IOLimiter, Consumer)
     */
    public void checkpoint( IOLimiter ioLimiter ) throws IOException
    {
        checkpoint( ioLimiter, CARRY_OVER_PREVIOUS_HEADER );
    }

    private void checkpoint( IOLimiter ioLimiter, Header.Writer headerWriter ) throws IOException
    {
        // Flush dirty pages of the tree, do this before acquiring the lock so that writers won't be
        // blocked while we do this
        pagedFile.flushAndForce( ioLimiter );

        // Block writers, or if there's a current writer then wait for it to complete and then block
        // From this point and till the lock is released we know that the tree won't change.
        lock.writerAndCleanerLock();
        try
        {
            assertRecoveryCleanSuccessful();
            // Flush dirty pages since that last flush above. This should be a very small set of pages
            // and should be rather fast. In here writers are blocked and we want to minimize this
            // windows of time as much as possible, that's why there's an initial flush outside this lock.
            pagedFile.flushAndForce();

            // Increment generation, i.e. stable becomes current unstable and unstable increments by one
            // and write the tree state (rootId, lastId, generation a.s.o.) to state page.
            long unstableGeneration = unstableGeneration( generation );
            generation = Generation.generation( unstableGeneration, unstableGeneration + 1 );
            writeState( pagedFile, headerWriter );

            // Flush the state page.
            pagedFile.flushAndForce();

            // Expose this fact.
            monitor.checkpointCompleted();

            // Clear flag so that until next change there's no need to do another checkpoint.
            changesSinceLastCheckpoint = false;
        }
        finally
        {
            // Unblock writers, any writes after this point and up until the next checkpoint will have
            // the new unstable generation.
            lock.writerAndCleanerUnlock();
        }
    }

    private void assertRecoveryCleanSuccessful() throws IOException
    {
        if ( cleaning != null && cleaning.hasFailed() )
        {
            throw new IOException( "Pointer cleaning during recovery failed", cleaning.getCause() );
        }
    }

    /**
     * Closes this tree and its associated resources.
     * <p>
     * NOTE: No {@link #checkpoint(IOLimiter) checkpoint} is performed.
     *
     * @throws IOException on error closing resources.
     */
    @Override
    public void close() throws IOException
    {
        lock.writerLock();
        try
        {
            if ( closed )
            {
                return;
            }

            internalIndexClose();
        }
        catch ( IOException ioe )
        {
            try
            {
                pagedFile.flushAndForce();
                internalIndexClose();
            }
            catch ( IOException e )
            {
                ioe.addSuppressed( e );
                throw ioe;
            }
        }
        finally
        {
            lock.writerUnlock();
        }
    }

    private void internalIndexClose() throws IOException
    {
        if ( cleaning != null && !changesSinceLastCheckpoint && !cleaning.needed() )
        {
            clean = true;
            forceState();
        }
        pagedFile.close();
        closed = true;
    }

    /**
     * Returns a {@link Writer} able to modify the index, i.e. insert and remove keys/values.
     * After usage the returned writer must be closed, typically by using try-with-resource clause.
     *
     * @return the single {@link Writer} for this index. The returned writer must be
     * {@link Writer#close() closed} before another caller can acquire this writer.
     * @throws IOException on error accessing the index.
     * @throws IllegalStateException for calls made between a successful call to this method and closing the
     * returned writer.
     */
    public Writer<KEY,VALUE> writer() throws IOException
    {
        assertRecoveryCleanSuccessful();
        writer.initialize();
        changesSinceLastCheckpoint = true;
        return writer;
    }

    private void setRoot( long rootId, long rootGeneration )
    {
        this.root = new Root( rootId, rootGeneration );
    }

    /**
     * Bump unstable generation, increasing the gap between stable and unstable generation. All pointers and tree nodes
     * with generation in this gap are considered to be 'crashed' and will be cleaned up by {@link CleanupJob}
     * created in {@link #createCleanupJob(RecoveryCleanupWorkCollector, boolean)}.
     */
    private void bumpUnstableGeneration()
    {
        generation = generation( stableGeneration( generation ), unstableGeneration( generation ) + 1 );
    }

    private void forceState() throws IOException
    {
        if ( changesSinceLastCheckpoint )
        {
            throw new IllegalStateException( "It seems that this method has been called in the wrong state. " +
                    "It's expected that this is called after opening this tree, but before any changes " +
                    "have been made" );
        }

        writeState( pagedFile, CARRY_OVER_PREVIOUS_HEADER );
        pagedFile.flushAndForce();
    }

    /**
     * Called on start if tree was not clean.
     */
    private CleanupJob createCleanupJob( RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean needsCleaning )
    {
        if ( !needsCleaning )
        {
            return CleanupJob.CLEAN;
        }
        else
        {
            lock.cleanerLock();
            monitor.cleanupRegistered();

            long generation = this.generation;
            long stableGeneration = stableGeneration( generation );
            long unstableGeneration = unstableGeneration( generation );
            long highTreeNodeId = freeList.lastId() + 1;

            CrashGenerationCleaner crashGenerationCleaner =
                    new CrashGenerationCleaner( pagedFile, bTreeNode, IdSpace.MIN_TREE_NODE_ID, highTreeNodeId,
                            stableGeneration, unstableGeneration, monitor );
            GBPTreeCleanupJob cleanupJob = new GBPTreeCleanupJob( crashGenerationCleaner, lock, monitor, indexFile );
            recoveryCleanupWorkCollector.add( cleanupJob );
            return cleanupJob;
        }
    }

    @SuppressWarnings( "unused" )
    void printTree() throws IOException
    {
        printTree( false, false, false, false );
    }

    // Utility method
    /**
     * Prints the contents of the tree to System.out.
     *
     * @param printValues whether or not to print values in the leaf nodes.
     * @param printPosition whether or not to print position for each key.
     * @param printState whether or not to print the tree state.
     * @param printHeader whether or not to print header of each tree node
     * @throws IOException on I/O error.
     */
    @SuppressWarnings( "SameParameterValue" )
    void printTree( boolean printValues, boolean printPosition, boolean printState, boolean printHeader ) throws IOException
    {
        try ( PageCursor cursor = openRootCursor( PagedFile.PF_SHARED_READ_LOCK ) )
        {
            new TreePrinter<>( bTreeNode, layout, stableGeneration( generation ), unstableGeneration( generation ) )
                .printTree( cursor, writer.cursor, System.out, printValues, printPosition, printState, printHeader );
        }
    }
    // Utility method
    /**
     * Print node with given id to System.out, if node with id exists.
     * @param id the page id of node to print
     */
    void printNode( int id ) throws IOException
    {
        if ( id < freeList.lastId() )
        {
            // Use write lock to avoid adversary interference
            try ( PageCursor cursor = pagedFile.io( id, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                cursor.next();
                byte nodeType = TreeNode.nodeType( cursor );
                if ( nodeType == TreeNode.NODE_TYPE_TREE_NODE )
                {
                    bTreeNode.printNode( cursor, false, true, stableGeneration( generation ), unstableGeneration( generation ) );
                }
            }
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

            long rootGeneration = root.goTo( cursor );
            boolean check = consistencyChecker.check( cursor, rootGeneration );
            root.goTo( cursor );

            PrimitiveLongSet freelistIds = Primitive.longSet();
            freeList.visitFreelistPageIds( freelistIds::add );
            freeList.visitUnacquiredIds( freelistIds::add, unstableGeneration );
            boolean checkSpace = consistencyChecker.checkSpace( cursor, freeList.lastId(), freelistIds.iterator() );

            return check && checkSpace;
        }
    }

    @Override
    public String toString()
    {
        long generation = this.generation;
        return format( "GB+Tree[file:%s, layout:%s, generation:%d/%d]",
                indexFile.getAbsolutePath(), layout,
                stableGeneration( generation ), unstableGeneration( generation ) );
    }

    private <E extends Throwable> E appendTreeInformation( E e )
    {
        return Exceptions.withMessage( e, e.getMessage() + " | " + toString() );
    }

    private class SingleWriter implements Writer<KEY,VALUE>
    {
        /**
         * Currently an index only supports one concurrent writer and so this boolean will act as
         * guard so that only one writer ever exist.
         */
        private final AtomicBoolean writerTaken = new AtomicBoolean();
        private final InternalTreeLogic<KEY,VALUE> treeLogic;
        private final StructurePropagation<KEY> structurePropagation;
        private PageCursor cursor;

        // Writer can't live past a checkpoint because of the mutex with checkpoint,
        // therefore safe to locally cache these generation fields from the volatile generation in the tree
        private long stableGeneration;
        private long unstableGeneration;

        SingleWriter( InternalTreeLogic<KEY,VALUE> treeLogic )
        {
            this.structurePropagation = new StructurePropagation<>( layout.newKey(), layout.newKey(), layout.newKey() );
            this.treeLogic = treeLogic;
        }

        /**
         * When leaving initialize, writer should be in a fully consistent state.
         * <p>
         * Either fully initialized:
         * <ul>
         *    <li>{@link #writerTaken} - true</li>
         *    <li>{@link #lock} - writerLock locked</li>
         *    <li>{@link #cursor} - not null</li>
         * </ul>
         * Of fully closed:
         * <ul>
         *    <li>{@link #writerTaken} - false</li>
         *    <li>{@link #lock} - writerLock unlocked</li>
         *    <li>{@link #cursor} - null</li>
         * </ul>
         *
         * @throws IOException if fail to open {@link PageCursor}
         */
        void initialize() throws IOException
        {
            if ( !writerTaken.compareAndSet( false, true ) )
            {
                throw new IllegalStateException( "Writer in " + this + " is already acquired by someone else. " +
                        "Only a single writer is allowed. The writer will become available as soon as " +
                        "acquired writer is closed" );
            }

            boolean success = false;
            try
            {
                lock.writerLock();
                cursor = openRootCursor( PagedFile.PF_SHARED_WRITE_LOCK );
                stableGeneration = stableGeneration( generation );
                unstableGeneration = unstableGeneration( generation );
                assert assertNoSuccessor( cursor, stableGeneration, unstableGeneration );
                treeLogic.initialize( cursor );
                success = true;
            }
            catch ( Throwable e )
            {
                appendTreeInformation( e );
                throw e;
            }
            finally
            {
                if ( !success )
                {
                    close();
                }
            }
        }

        @Override
        public void put( KEY key, VALUE value ) throws IOException
        {
            merge( key, value, ValueMergers.overwrite() );
        }

        @Override
        public void merge( KEY key, VALUE value, ValueMerger<KEY,VALUE> valueMerger ) throws IOException
        {
            try
            {
                treeLogic.insert( cursor, structurePropagation, key, value, valueMerger,
                        stableGeneration, unstableGeneration );
            }
            catch ( Throwable e )
            {
                appendTreeInformation( e );
                throw e;
            }

            handleStructureChanges();

            checkOutOfBounds( cursor );
        }

        private void setRoot( long rootPointer )
        {
            long rootId = GenerationSafePointerPair.pointer( rootPointer );
            GBPTree.this.setRoot( rootId, unstableGeneration );
            treeLogic.initialize( cursor );
        }

        @Override
        public VALUE remove( KEY key ) throws IOException
        {
            VALUE result;
            try
            {
                result = treeLogic.remove( cursor, structurePropagation, key, layout.newValue(),
                        stableGeneration, unstableGeneration );
            }
            catch ( Throwable e )
            {
                appendTreeInformation( e );
                throw e;
            }

            handleStructureChanges();

            checkOutOfBounds( cursor );
            return result;
        }

        private void handleStructureChanges() throws IOException
        {
            if ( structurePropagation.hasRightKeyInsert )
            {
                // New root
                long newRootId = freeList.acquireNewId( stableGeneration, unstableGeneration );
                PageCursorUtil.goTo( cursor, "new root", newRootId );

                bTreeNode.initializeInternal( cursor, stableGeneration, unstableGeneration );
                bTreeNode.setChildAt( cursor, structurePropagation.midChild, 0,
                        stableGeneration, unstableGeneration );
                bTreeNode.insertKeyAndRightChildAt( cursor, structurePropagation.rightKey, structurePropagation.rightChild, 0, 0,
                        stableGeneration, unstableGeneration );
                TreeNode.setKeyCount( cursor, 1 );
                setRoot( newRootId );
            }
            else if ( structurePropagation.hasMidChildUpdate )
            {
                setRoot( structurePropagation.midChild );
            }
            structurePropagation.clear();
        }

        @Override
        public void close()
        {
            if ( !writerTaken.compareAndSet( true, false ) )
            {
                throw new IllegalStateException( "Tried to close writer of " + GBPTree.this +
                        ", but writer is already closed." );
            }
            closeCursor();
            lock.writerUnlock();
        }

        private void closeCursor()
        {
            if ( cursor != null )
            {
                cursor.close();
                cursor = null;
            }
        }
    }

    public boolean wasDirtyOnStartup()
    {
        return dirtyOnStartup;
    }

    /**
     * Total size limit for key and value.
     * This limit includes storage overhead that is specific to key implementation for example entity id or meta data about type.
     * @return Total size limit for key and value or {@link TreeNode#NO_KEY_VALUE_SIZE_CAP} if no such value exists.
     */
    public int keyValueSizeCap()
    {
        return bTreeNode.keyValueSizeCap();
    }
}
