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
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.set.ImmutableSet;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.Arrays.asList;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.index.internal.gbptree.GBPTreeOpenOptions.NO_FLUSH_ON_CLOSE;
import static org.neo4j.index.internal.gbptree.Generation.generation;
import static org.neo4j.index.internal.gbptree.Generation.stableGeneration;
import static org.neo4j.index.internal.gbptree.Generation.unstableGeneration;
import static org.neo4j.index.internal.gbptree.GenerationSafePointer.MIN_GENERATION;
import static org.neo4j.index.internal.gbptree.Header.CARRY_OVER_PREVIOUS_HEADER;
import static org.neo4j.index.internal.gbptree.Header.replace;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.checkOutOfBounds;
import static org.neo4j.index.internal.gbptree.PointerChecking.assertNoSuccessor;
import static org.neo4j.index.internal.gbptree.SeekCursor.DEFAULT_MAX_READ_AHEAD;
import static org.neo4j.index.internal.gbptree.SeekCursor.LEAF_LEVEL;
import static org.neo4j.internal.helpers.Exceptions.withMessage;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;

/**
 * A generation-aware B+tree (GB+Tree) implementation directly atop a {@link PageCache} with no caching in between.
 * Additionally internal and leaf nodes on same level are linked both left and right (sibling pointers),
 * this to provide correct reading when concurrently {@link #writer(CursorContext)}  modifying}
 * the tree.
 * <p>
 * Generation is incremented on {@link #checkpoint(CursorContext)} check-pointing}.
 * Generation awareness allows for recovery from last {@link #checkpoint(CursorContext)}, provided the same updates
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
 * {@link #writer(CursorContext)}  Writes} happen to the tree and are made durable and
 * safe on next call to {@link #checkpoint(CursorContext)}. Writes which happens after the last
 * {@link #checkpoint(CursorContext)} are not safe if there's a {@link #close()} or JVM crash in between, i.e:
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
public class GBPTree<KEY,VALUE> implements Closeable, Seeker.Factory<KEY,VALUE>
{
    private static final String INDEX_INTERNAL_TAG = "indexInternal";

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
            public void cleanupFinished( long numberOfPagesVisited, long numberOfTreeNodes, long numberOfCleanedCrashPointers, long durationMillis )
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

            @Override
            public void treeGrowth()
            {   // no-op
            }

            @Override
            public void treeShrink()
            {   // no-op
            }
        }

        class Delegate implements Monitor
        {
            private final Monitor delegate;

            public Delegate( Monitor delegate )
            {
                this.delegate = delegate;
            }

            @Override
            public void checkpointCompleted()
            {
                delegate.checkpointCompleted();
            }

            @Override
            public void noStoreFile()
            {
                delegate.noStoreFile();
            }

            @Override
            public void cleanupRegistered()
            {
                delegate.cleanupRegistered();
            }

            @Override
            public void cleanupStarted()
            {
                delegate.cleanupStarted();
            }

            @Override
            public void cleanupFinished( long numberOfPagesVisited, long numberOfTreeNodes, long numberOfCleanedCrashPointers, long durationMillis )
            {
                delegate.cleanupFinished( numberOfPagesVisited, numberOfTreeNodes, numberOfCleanedCrashPointers, durationMillis );
            }

            @Override
            public void cleanupClosed()
            {
                delegate.cleanupClosed();
            }

            @Override
            public void cleanupFailed( Throwable throwable )
            {
                delegate.cleanupFailed( throwable );
            }

            @Override
            public void startupState( boolean clean )
            {
                delegate.startupState( clean );
            }

            @Override
            public void treeGrowth()
            {
                delegate.treeGrowth();
            }

            @Override
            public void treeShrink()
            {
                delegate.treeShrink();
            }
        }

        /**
         * Called when a {@link GBPTree#checkpoint(CursorContext)} has been completed, but right before
         * {@link GBPTree#writer(CursorContext)} writers} are re-enabled.
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
         * @param numberOfPagesVisited number of pages visited by the cleaner.
         * @param numberOfTreeNodes number of tree nodes visited by the cleaner.
         * @param numberOfCleanedCrashPointers number of cleaned crashed pointers.
         * @param durationMillis time spent cleaning.
         */
        void cleanupFinished( long numberOfPagesVisited, long numberOfTreeNodes, long numberOfCleanedCrashPointers, long durationMillis );

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

        /**
         * Report tree growth, meaning split in root.
         */
        void treeGrowth();

        /**
         * Report tree shrink, when root becomes empty.
         */
        void treeShrink();
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
     * {@link Path} to map in {@link PageCache} for storing this tree.
     */
    private final Path indexFile;

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
     * Tells whether or not there have been made changes (using {@link #writer(CursorContext)}) to this tree
     * since last call to {@link #checkpoint(CursorContext)}. This variable is set when calling {@link #writer(CursorContext)}
     * and cleared inside {@link #checkpoint(CursorContext)}.
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
    private final int pageSize;

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
     * <li>stable generation, generation which has survived the last {@link #checkpoint(CursorContext)}</li>
     * <li>unstable generation, current generation under evolution. This generation will be the
     * {@link Generation#stableGeneration(long)} after the next {@link #checkpoint(CursorContext)}</li>
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

    private final RootInitializer rootInitializer = cursor -> root.goTo( cursor );

    /**
     * Catchup for {@link SeekCursor} to become aware of new roots since it started.
     */
    private final Supplier<RootCatchup> rootCatchupSupplier = () -> new TripCountingRootCatchup( () -> root );

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
     * If this tree is read only, no changes will be made to it. No generation bumping, no checkpointing, no nothing.
     */
    private final DatabaseReadOnlyChecker readOnlyChecker;

    /**
     * Underlying page cache tracer. Should be used to create page cursors tracers
     * only for cases where work is performed by tree itself: construction or shutdown, otherwise tracers caller
     * should provide correct context related tracer that should be used
     */
    private final PageCacheTracer pageCacheTracer;

    /**
     * Array of {@link OpenOption} which is passed to calls to {@link PageCache#map(Path, int, String, ImmutableSet)}
     * at open/create. When initially creating the file an array consisting of {@link StandardOpenOption#CREATE}
     * concatenated with the contents of this array is passed into the map call.
     */
    private final ImmutableSet<OpenOption> openOptions;

    // Name of the database this tree belongs to.
    private final String databaseName;

    /**
     * Whether or not this tree has been closed. Accessed and changed solely in
     * {@link #close()} to be able to close tree multiple times gracefully.
     */
    @SuppressWarnings( "UnusedAssignment" )
    private boolean closed = true;

    /**
     * True if tree is clean, false if dirty
     */
    private boolean clean;

    /**
     * True if initial tree state was dirty
     */
    private final boolean dirtyOnStartup;

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
     * {@link #createCleanupJob(RecoveryCleanupWorkCollector, boolean, String)} and clean crashed pointers as part of constructor. Tree is only clean if
     * since last time it was opened it was {@link #close()}  closed} without any non-checkpointed changes present.
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
     * @param indexFile {@link Path} containing the actual index
     * @param layout {@link Layout} to use in the tree, this must match the existing layout
     * we're just opening the index
     * @param monitor {@link Monitor} for monitoring {@link GBPTree}.
     * @param headerReader reads header data, previously written using {@link #checkpoint( Consumer, CursorContext)}
     * or {@link #close()}
     * @param headerWriter writes header data if indexFile is created as a result of this call.
     * @param recoveryCleanupWorkCollector collects recovery cleanup jobs for execution after recovery.
     * @param readOnlyChecker database readonly mode checker.
     * @param databaseName name of the database this tree belongs to.
     * @param name name of the tree that will be used when describing work related to this tree.
     * @throws UncheckedIOException on page cache error
     * @throws MetadataMismatchException if meta information does not match constructor parameters or meta page is missing
     */
    public GBPTree( PageCache pageCache, Path indexFile, Layout<KEY,VALUE> layout, Monitor monitor, Header.Reader headerReader,
            Consumer<PageCursor> headerWriter, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, DatabaseReadOnlyChecker readOnlyChecker,
            PageCacheTracer pageCacheTracer, ImmutableSet<OpenOption> openOptions, String databaseName, String name ) throws MetadataMismatchException
    {
        this.indexFile = indexFile;
        this.monitor = monitor;
        this.readOnlyChecker = readOnlyChecker;
        this.pageCacheTracer = pageCacheTracer;
        this.openOptions = openOptions;
        this.databaseName = databaseName;
        this.generation = Generation.generation( MIN_GENERATION, MIN_GENERATION + 1 );
        long rootId = IdSpace.MIN_TREE_NODE_ID;
        setRoot( rootId, Generation.unstableGeneration( generation ) );
        this.layout = layout;

        try ( var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( INDEX_INTERNAL_TAG ) ) )
        {
            this.pagedFile = openOrCreate( pageCache, indexFile, cursorContext, databaseName, openOptions );
            this.pageSize = pagedFile.pageSize();
            closed = false;
            TreeNodeSelector.Factory format;
            if ( created )
            {
                format = TreeNodeSelector.selectByLayout( layout );
                writeMeta( layout, format, pagedFile, cursorContext );
            }
            else
            {
                Meta meta = readMeta( pagedFile, cursorContext );
                meta.verify( layout );
                format = TreeNodeSelector.selectByFormat( meta.getFormatIdentifier(), meta.getFormatVersion() );
            }
            this.freeList = new FreeListIdProvider( pagedFile, rootId );
            OffloadStoreImpl<KEY,VALUE> offloadStore = buildOffload( layout, freeList, pagedFile, pageSize );
            this.bTreeNode = format.create( pageSize, layout, offloadStore );
            this.writer = new SingleWriter( new InternalTreeLogic<>( freeList, bTreeNode, layout, monitor ) );

            // Create or load state
            if ( created )
            {
                initializeAfterCreation( headerWriter, cursorContext );
            }
            else
            {
                loadState( pagedFile, headerReader, cursorContext );
            }
            this.monitor.startupState( clean );

            // Prepare tree for action
            dirtyOnStartup = !clean;
            if ( !readOnlyChecker.isPermanentlyReadOnly() )
            {
                clean = false;
                bumpUnstableGeneration();
                forceState( cursorContext );
                cleaning = createCleanupJob( recoveryCleanupWorkCollector, dirtyOnStartup, name );
            }
            else
            {
                cleaning = CleanupJob.CLEAN;
            }
        }
        catch ( IOException e )
        {
            throw exitConstructor( new UncheckedIOException( e ) );
        }
        catch ( Throwable e )
        {
            throw exitConstructor( e );
        }
    }

    private RuntimeException exitConstructor( Throwable throwable )
    {
        try
        {
            close();
        }
        catch ( IOException e )
        {
            throwable = Exceptions.chain( new UncheckedIOException( e ), throwable );
        }

        appendTreeInformation( throwable );
        Exceptions.throwIfUnchecked( throwable );
        return new RuntimeException( throwable );
    }

    private void initializeAfterCreation( Consumer<PageCursor> headerWriter, CursorContext cursorContext ) throws IOException
    {
        // Initialize state
        try ( PageCursor cursor = pagedFile.io( 0 /*ignored*/, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext ) )
        {
            TreeStatePair.initializeStatePages( cursor );
        }

        // Initialize index root node to a leaf node.
        try ( PageCursor cursor = openRootCursor( PagedFile.PF_SHARED_WRITE_LOCK, cursorContext ) )
        {
            long stableGeneration = stableGeneration( generation );
            long unstableGeneration = unstableGeneration( generation );
            bTreeNode.initializeLeaf( cursor, stableGeneration, unstableGeneration );
            checkOutOfBounds( cursor );
        }

        // Initialize free-list
        freeList.initializeAfterCreation( cursorContext );
        changesSinceLastCheckpoint = true;

        // Checkpoint to make the created root node stable. Forcing tree state also piggy-backs on this.
        checkpoint( headerWriter, cursorContext );
        clean = true;
    }

    private PagedFile openOrCreate( PageCache pageCache, Path indexFile, CursorContext cursorContext, String databaseName,
            ImmutableSet<OpenOption> openOptions ) throws IOException, MetadataMismatchException
    {
        try
        {
            var pageCacheOptions = openOptions.newWithoutAll( asList( GBPTreeOpenOptions.values() ) );
            return openExistingIndexFile( pageCache, indexFile, cursorContext, databaseName, pageCacheOptions );
        }
        catch ( NoSuchFileException e )
        {
            if ( readOnlyChecker.isPermanentlyReadOnly() )
            {
                throw new TreeFileNotFoundException( "Can not create new tree file in read only mode.", new Exception( "Database is permanently read-only" ) );
            }
            return createNewIndexFile( pageCache, indexFile );
        }
    }

    private static PagedFile openExistingIndexFile( PageCache pageCache, Path indexFile, CursorContext cursorContext, String databaseName,
            ImmutableSet<OpenOption> openOptions ) throws IOException, MetadataMismatchException
    {
        PagedFile pagedFile = pageCache.map( indexFile, pageCache.pageSize(), databaseName, openOptions );
        // This index already exists, verify meta data aligns with expectations

        MutableBoolean pagedFileOpen = new MutableBoolean( true );
        boolean success = false;
        try
        {
            // We're only interested in the page size really
            Meta meta = readMeta( pagedFile, cursorContext );
            if ( meta.getPageSize() != pageCache.pageSize() )
            {
                throw new MetadataMismatchException( format(
                        "Tried to open the tree using page size %d, but the tree was original created with page size %d so cannot be opened.",
                        pageCache.pageSize(), meta.getPageSize() ) );
            }
            success = true;
            return pagedFile;
        }
        catch ( IllegalStateException e )
        {
            throw new MetadataMismatchException( "Index is not fully initialized since it's missing the meta page", e );
        }
        finally
        {
            if ( !success && pagedFileOpen.booleanValue() )
            {
                pagedFile.close();
            }
        }
    }

    private PagedFile createNewIndexFile( PageCache pageCache, Path indexFile ) throws IOException
    {
        // First time
        monitor.noStoreFile();
        // We need to create this index
        PagedFile pagedFile = pageCache.map( indexFile, pageCache.pageSize(), databaseName, openOptions.newWith( CREATE ) );
        created = true;
        return pagedFile;
    }

    private void loadState( PagedFile pagedFile, Header.Reader headerReader, CursorContext cursorContext ) throws IOException
    {
        Pair<TreeState,TreeState> states = loadStatePages( pagedFile, cursorContext );
        TreeState state = TreeStatePair.selectNewestValidState( states );
        try ( PageCursor cursor = pagedFile.io( state.pageId(), PF_SHARED_READ_LOCK, cursorContext ) )
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
     * @param indexFile {@link Path} containing the actual index
     * @param headerReader reads header data, previously written using {@link #checkpoint( Consumer, CursorContext)}
     * or {@link #close()}
     * @param databaseName name of the database index file belongs to.
     * @throws IOException On page cache error
     * @throws MetadataMismatchException if some meta page is missing (tree not fully initialized)
     */
    public static void readHeader( PageCache pageCache, Path indexFile, Header.Reader headerReader, String databaseName, CursorContext cursorContext )
            throws IOException, MetadataMismatchException
    {
        try ( PagedFile pagedFile = openExistingIndexFile( pageCache, indexFile, cursorContext, databaseName, immutable.empty() ) )
        {
            Pair<TreeState,TreeState> states = loadStatePages( pagedFile, cursorContext );
            TreeState state = TreeStatePair.selectNewestValidState( states );
            try ( PageCursor cursor = pagedFile.io( state.pageId(), PF_SHARED_READ_LOCK, cursorContext ) )
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

    private void writeState( PagedFile pagedFile, Header.Writer headerWriter, CursorContext cursorContext ) throws IOException
    {
        Pair<TreeState,TreeState> states = readStatePages( pagedFile, cursorContext );
        TreeState oldestState = TreeStatePair.selectOldestOrInvalid( states );
        long pageToOverwrite = oldestState.pageId();
        Root root = this.root;
        try ( PageCursor cursor = pagedFile.io( pageToOverwrite, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext ) )
        {
            PageCursorUtil.goTo( cursor, "state page", pageToOverwrite );
            TreeState.write( cursor, stableGeneration( generation ), unstableGeneration( generation ),
                    root.id(), root.generation(),
                    freeList.lastId(), freeList.writePageId(), freeList.readPageId(),
                    freeList.writePos(), freeList.readPos(), clean );

            writerHeader( pagedFile, headerWriter, other( states, oldestState ), cursor, cursorContext );

            checkOutOfBounds( cursor );
        }
    }

    private static void writerHeader( PagedFile pagedFile, Header.Writer headerWriter,
            TreeState otherState, PageCursor cursor, CursorContext cursorContext ) throws IOException
    {
        // Write/carry over header
        int headerOffset = cursor.getOffset();
        int headerDataOffset = getHeaderDataOffset( headerOffset );
        if ( otherState.isValid() || headerWriter != CARRY_OVER_PREVIOUS_HEADER )
        {
            try ( PageCursor previousCursor = pagedFile.io( otherState.pageId(), PF_SHARED_READ_LOCK, cursorContext ) )
            {
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
            }
            checkOutOfBounds( cursor );

            int length = cursor.getOffset() - headerDataOffset;
            cursor.putInt( headerOffset, length );
        }
    }

    @VisibleForTesting
    public static void overwriteHeader( PageCache pageCache, Path indexFile, Consumer<PageCursor> headerWriter, String databaseName,
            CursorContext cursorContext ) throws IOException
    {
        Header.Writer writer = replace( headerWriter );
        try ( PagedFile pagedFile = openExistingIndexFile( pageCache, indexFile, cursorContext, databaseName, immutable.empty() ) )
        {
            Pair<TreeState,TreeState> states = readStatePages( pagedFile, cursorContext );
            TreeState newestValidState = TreeStatePair.selectNewestValidState( states );
            long pageToOverwrite = newestValidState.pageId();
            try ( PageCursor cursor = pagedFile.io( pageToOverwrite, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext ) )
            {
                PageCursorUtil.goTo( cursor, "state page", pageToOverwrite );

                // Place cursor after state data
                TreeState.read( cursor );

                // Note offset to header
                int headerOffset = cursor.getOffset();
                int headerDataOffset = getHeaderDataOffset( headerOffset );

                // Reserve space to store length
                cursor.setOffset( headerDataOffset );
                // Write data
                writer.write( null, 0, cursor );
                // Write length
                int length = cursor.getOffset() - headerDataOffset;
                cursor.putInt( headerOffset, length );
                checkOutOfBounds( cursor );
            }
        }
    }

    private static int getHeaderDataOffset( int headerOffset )
    {
        // Int reserved to store length of header
        return headerOffset + Integer.BYTES;
    }

    private static TreeState other( Pair<TreeState,TreeState> states, TreeState state )
    {
        return states.getLeft() == state ? states.getRight() : states.getLeft();
    }

    /**
     * Basically {@link #readStatePages(PagedFile, CursorContext)} with some more checks, suitable for when first opening an index file,
     * not while running it and check pointing.
     *
     * @param pagedFile {@link PagedFile} to read the state pages from.
     * @param cursorContext underlying page cursor context
     * @return both read state pages.
     * @throws MetadataMismatchException if state pages are missing (file is smaller than that) or if they are both empty.
     * @throws IOException on {@link PageCursor} error.
     */
    private static Pair<TreeState,TreeState> loadStatePages( PagedFile pagedFile, CursorContext cursorContext ) throws MetadataMismatchException, IOException
    {
        try
        {
            Pair<TreeState,TreeState> states = readStatePages( pagedFile, cursorContext );
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

    private static Pair<TreeState,TreeState> readStatePages( PagedFile pagedFile, CursorContext cursorContext ) throws IOException
    {
        Pair<TreeState,TreeState> states;
        try ( PageCursor cursor = pagedFile.io( 0L /*ignored*/, PF_SHARED_READ_LOCK, cursorContext ) )
        {
            states = TreeStatePair.readStatePages(
                    cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B );
        }
        return states;
    }

    private static PageCursor openMetaPageCursor( PagedFile pagedFile, int pfFlags, CursorContext cursorContext ) throws IOException
    {
        PageCursor metaCursor = pagedFile.io( IdSpace.META_PAGE_ID, pfFlags, cursorContext );
        PageCursorUtil.goTo( metaCursor, "meta page", IdSpace.META_PAGE_ID );
        return metaCursor;
    }

    private static <KEY,VALUE> Meta readMeta( PagedFile pagedFile, CursorContext cursorContext )
            throws IOException
    {
        try ( PageCursor metaCursor = openMetaPageCursor( pagedFile, PF_SHARED_READ_LOCK, cursorContext ) )
        {
            return Meta.read( metaCursor );
        }
    }

    private void writeMeta( Layout<KEY,VALUE> layout, TreeNodeSelector.Factory format, PagedFile pagedFile, CursorContext cursorContext ) throws IOException
    {
        Meta meta = new Meta( format.formatIdentifier(), format.formatVersion(), pageSize, layout );
        try ( PageCursor metaCursor = openMetaPageCursor( pagedFile, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext ) )
        {
            meta.write( metaCursor );
        }
    }

    /**
     * Utility for {@link PagedFile#io(long, int, CursorContext) acquiring} a new {@link PageCursor},
     * placed at the current root id and which have had its {@link PageCursor#next()} called-
     *
     * @param pfFlags flags sent into {@link PagedFile#io(long, int, CursorContext)}.
     * @return {@link PageCursor} result from call to {@link PagedFile#io(long, int, CursorContext)} after it has been
     * placed at the current root and has had {@link PageCursor#next()} called.
     * @throws IOException on {@link PageCursor} error.
     */
    private PageCursor openRootCursor( int pfFlags, CursorContext cursorContext ) throws IOException
    {
        PageCursor cursor = pagedFile.io( 0L /*Ignored*/, pfFlags, cursorContext );
        root.goTo( cursor );
        return cursor;
    }

    @Override
    public Seeker<KEY,VALUE> allocateSeeker( CursorContext cursorContext ) throws IOException
    {
        return internalAllocateSeeker( cursorContext, SeekCursor.NO_MONITOR );
    }

    private SeekCursor<KEY,VALUE> internalAllocateSeeker( CursorContext cursorContext, SeekCursor.Monitor monitor ) throws IOException
    {
        PageCursor cursor = pagedFile.io( 0L /*ignored*/, PF_SHARED_READ_LOCK, cursorContext );
        return new SeekCursor<>( cursor, bTreeNode, layout, generationSupplier, rootInitializer, rootCatchupSupplier.get(), exceptionDecorator, monitor,
                cursorContext );
    }

    @Override
    public Seeker<KEY,VALUE> seek( Seeker<KEY,VALUE> seeker, KEY fromInclusive, KEY toExclusive ) throws IOException
    {
        return initializeSeeker( seeker, fromInclusive, toExclusive, DEFAULT_MAX_READ_AHEAD, LEAF_LEVEL );
    }

    private Seeker<KEY,VALUE> initializeSeeker( Seeker<KEY,VALUE> seeker, KEY fromInclusive, KEY toExclusive, int readAheadLength, int searchLevel )
            throws IOException
    {
        return ((SeekCursor<KEY,VALUE>) seeker).initialize( fromInclusive, toExclusive, readAheadLength, searchLevel );
    }

    /**
     * Partitions the provided key range into {@code numberOfPartitions} partitions and instantiates a {@link Seeker} for each.
     * Caller can seek through the partitions in parallel. Caller is responsible for closing the returned {@link Seeker seekers}.
     * <p>
     * See {@link #partitionedSeekInternal(Object, Object, int, CursorContext, Seeker.Factory)} for details on implementation.
     *
     * @param fromInclusive lower bound of the target range to seek (inclusive).
     * @param toExclusive higher bound of the target range to seek (exclusive).
     * @param desiredNumberOfPartitions number of partitions desired by the caller. If the tree is small a lower number of partitions may be returned.
     * The number of partitions will never be higher than the provided {@code desiredNumberOfPartitions}.
     * @param cursorContext underlying page cursor cursorContext for the thread doing the partitioning.
     * @return a {@link Collection} of {@link Seeker seekers}, each having their own distinct partition to seek. Collectively they
     * seek across the whole provided range.
     * @throws IOException on error reading from index.
     */
    public Collection<Seeker.WithContext<KEY,VALUE>> partitionedSeek( KEY fromInclusive, KEY toExclusive,
                                                                      int desiredNumberOfPartitions, CursorContext cursorContext )
            throws IOException
    {
        return partitionedSeekInternal( fromInclusive, toExclusive, desiredNumberOfPartitions, cursorContext, this );
    }

    /**
     * We want to create a given number of partitions of the range given by <code>fromInclusive</code> and <code>toExclusive</code>.
     * We want the number of entries in each partition to be as equal as possible. We let the number of leaves in each partition
     * be an estimate for the number of entries, assuming that one leaf will contain a comparable number of entries as another.
     * Each subtree on level X is divided by splitter keys on the same level or on any of the levels above. Example:
     * <pre>
     * Level 0:                  [10,                           50]
     * Level 1:     [3,    7]     ^          [13,      25]                [70,      90]
     * Level 2: [1,2] [3,6]^[8,9]   [10,11,12]  [14,20]  [25,43]   [50, 55]  [71,85]  [109,200]
     *                ===== =====   ==========
     * </pre>
     * All keys on level 0 and 1 are called splitter keys (or internal keys) because they split subtrees from each other.
     * In this tree [3,6] and [8,9] on level 2 is separated by splitter key 7 on level 1. But looking at [8,9] and [10,11,12]
     * on level 2 we see that they are separated by splitter key 10 from level 0. Similarly, each subtree on level 2 (where each
     * subtree is a single leaf node) is separated from the others by a splitter key in one of the levels above. Noting that,
     * we can begin to form a strategy for how to create our partitions.
     * <p>
     * We want to create our partitions as high up in the tree as possible, simply to terminate the partitioning step as soon as possible.
     * If we want to create three partitions in the tree above for the range [0,300) we can use the three subtrees seen from the root
     * and be done, we would the form the three key-ranges [0,10), [10,50) and [50,300) and that is our partitioning. Note that we don't
     * strictly rely on those key-ranges to correspond perfectly to separate sub-trees since concurrent updates might change the structure of
     * the tree while we are reading. We have simply used the structure of the tree, at the time of forming the partitions, as a way to
     * construct key-ranges that are estimated to contain a similar number of entries.
     * <p>
     * If we want a more fine grained partitioning we need to go to the lower parts of the tree.
     * This is what we do: We start at level 0 and collect all keys within our target range, let's say we find N keys [K1, K2,... KN].
     * In between each key and on both sides of the range is a subtree which means we now have a way to create N+1 partitions of
     * estimated equal size. The outer boundaries will be given by fromInclusive and toExclusive. If <code>N+1 < desiredNumberOfPartitions</code>
     * we need to go one level further down and include all of the keys within our range from that level as well. If we still don't
     * have enough splitter keys in our range we continue down the tree until we either have enough keys or we reach the leaf level.
     * If we reach the leaf level it means that each partition will be only a single leaf node and we do not partition any further.
     * <p>
     * If concurrent updates causes changes higher up in the tree while searching in lower levels, some splitter keys can be missed or
     * extra splitter keys may be included. This can lead to partitions being more unevenly sized but it will not affect correctness.
     *
     * @param fromInclusive lower bound of the target range to seek (inclusive).
     * @param toExclusive higher bound of the target range to seek (exclusive).
     * @param desiredNumberOfPartitions number of partitions desired by the caller. If the tree is small a lower number of partitions may be returned.
     * The number of partitions will never be higher than the provided {@code desiredNumberOfPartitions}.
     * @param cursorContext underlying page cursor cursorContext for the thread doing the partitioning.
     * @param seekerFactory {@link Seeker.Factory} factory method that create the seekers for each partition.
     * @return {@link Collection} of {@link Seeker.WithContext wrappers} around {@link Seeker seekers} placed on each partition.
     * The number of partitions is given by the size of the collection.
     * @throws IOException on error accessing the index.
     */
    private Collection<Seeker.WithContext<KEY,VALUE>> partitionedSeekInternal( KEY fromInclusive, KEY toExclusive, int desiredNumberOfPartitions,
                                                                               CursorContext cursorContext, Seeker.Factory<KEY,VALUE> seekerFactory )
            throws IOException
    {
        Preconditions.checkArgument( layout.compare( fromInclusive, toExclusive ) <= 0, "Partitioned seek only supports forward seeking for the time being" );

        // Read enough splitter keys from root and downwards to create enough partitions.
        Set<KEY> splitterKeysInRange = new TreeSet<>( layout );
        int numberOfSubtrees;
        int searchLevel = 0;
        do
        {
            SeekDepthMonitor depthMonitor = new SeekDepthMonitor();
            KEY localFrom = layout.copyKey( fromInclusive, layout.newKey() );
            KEY localTo = layout.copyKey( toExclusive, layout.newKey() );
            try ( Seeker<KEY,VALUE> seek = initializeSeeker( internalAllocateSeeker( cursorContext, depthMonitor ), localFrom, localTo, DEFAULT_MAX_READ_AHEAD,
                    searchLevel ) )
            {
                if ( depthMonitor.reachedLeafLevel )
                {
                    // Don't partition any further if we've reached leaf level.
                    break;
                }
                while ( seek.next() )
                {
                    splitterKeysInRange.add( layout.copyKey( seek.key(), layout.newKey() ) );
                }
            }
            searchLevel++;
            numberOfSubtrees = splitterKeysInRange.size() + 1;
        }
        while ( numberOfSubtrees < desiredNumberOfPartitions );

        // From the set of splitter keys, create partitions
        KeyPartitioning<KEY> partitioning = new KeyPartitioning<>( layout );
        return partitioning.partition( splitterKeysInRange, fromInclusive, toExclusive, desiredNumberOfPartitions ).stream()
                           .map( partition -> (Seeker.WithContext<KEY,VALUE>) context ->
                                   seekerFactory.seek( partition.getLeft(), partition.getRight(), context ) )
                           .collect( Collectors.toUnmodifiableList() );
    }

    /**
     * Calculates an estimate of number of keys in this tree in O(log(n)) time. The number is only an estimate and may make its decision on a
     * concurrently changing tree, but should usually be correct within a couple of percents margin.
     * @param cursorContext underlying page cursor context
     *
     * @return an estimate of number of keys in the tree.
     */
    public long estimateNumberOfEntriesInTree( CursorContext cursorContext ) throws IOException
    {
        KEY low = layout.newKey();
        layout.initializeAsLowest( low );
        KEY high = layout.newKey();
        layout.initializeAsHighest( high );
        int sampleSize = 100;
        SizeEstimationMonitor monitor = new SizeEstimationMonitor();
        do
        {
            monitor.clear();
            Seeker.Factory<KEY,VALUE> monitoredSeeks = new Seeker.Factory<>()
            {
                @Override
                public Seeker<KEY,VALUE> allocateSeeker( CursorContext cursorContext ) throws IOException
                {
                    return internalAllocateSeeker( cursorContext, monitor );
                }

                @Override
                public Seeker<KEY,VALUE> seek( Seeker<KEY,VALUE> seeker, KEY fromInclusive, KEY toExclusive ) throws IOException
                {
                    return initializeSeeker( seeker, fromInclusive, toExclusive, 1, LEAF_LEVEL );
                }
            };
            Collection<Seeker.WithContext<KEY,VALUE>> seekersWithContext = partitionedSeekInternal( low, high, sampleSize, cursorContext, monitoredSeeks );
            for ( Seeker.WithContext<KEY,VALUE> seeker : seekersWithContext )
            {
                // Simply make sure the first one is found so that the supplied monitor have been notified about the path down to it
                try ( Seeker<KEY,VALUE> partition = seeker.with( cursorContext ) )
                {
                    partition.next();
                }
            }
        }
        while ( !monitor.isConsistent() );
        return monitor.estimateNumberOfKeys();
    }

    /**
     * Checkpoints and flushes any pending changes to storage. After a successful call to this method
     * the data is durable and safe. {@link #writer(CursorContext)} Changes} made after this call and until crashing or
     * otherwise non-clean shutdown (by omitting calling checkpoint before {@link #close()}) will need to be replayed
     * next time this tree is opened.
     * <p>
     * Header writer is expected to leave consumed {@link PageCursor} at end of written header for calculation of
     * header size.
     *
     * @param headerWriter hook for writing header data, must leave cursor at end of written header.
     * @param cursorContext underlying page cursor context
     * @throws UncheckedIOException on error flushing to storage.
     */
    public void checkpoint( Consumer<PageCursor> headerWriter, CursorContext cursorContext )
    {
        try
        {
            checkpoint( replace( headerWriter ), cursorContext );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    /**
     * Performs a {@link #checkpoint( Consumer, CursorContext)}  check point}, keeping any header information
     * written in previous check point.
     *
     * @param cursorContext underlying page cursor context
     * @throws UncheckedIOException on error flushing to storage.
     * @see #checkpoint( Header.Writer, CursorContext)
     */
    public void checkpoint( CursorContext cursorContext )
    {
        try
        {
            checkpoint( CARRY_OVER_PREVIOUS_HEADER, cursorContext );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void checkpoint( Header.Writer headerWriter, CursorContext cursorContext ) throws IOException
    {
        if ( readOnlyChecker.isPermanentlyReadOnly() )
        {
            return;
        }

        // Flush dirty pages of the tree, do this before acquiring the lock so that writers won't be
        // blocked while we do this
        pagedFile.flushAndForce();

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
            writeState( pagedFile, headerWriter, cursorContext );

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

    private void assertNotReadOnly( String operationDescription )
    {
        if ( readOnlyChecker.isReadOnly() )
        {
            throw new UnsupportedOperationException( "GBPTree currently is in read only mode and can not finish operation: " + operationDescription );
        }
    }

    /**
     * Closes this tree and its associated resources.
     * <p>
     * NOTE: No {@link #checkpoint(CursorContext)} checkpoint} is performed.
     * @throws IOException on error closing resources.
     */
    @Override
    public void close() throws IOException
    {
        try ( var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( INDEX_INTERNAL_TAG ) ) )
        {
            if ( openOptions.contains( NO_FLUSH_ON_CLOSE ) || readOnlyChecker.isPermanentlyReadOnly() )
            {
                // Close without forcing state
                doClose();
                return;
            }
            lock.writerLock();
            try
            {
                if ( closed )
                {
                    return;
                }

                maybeForceCleanState( cursorContext );
                doClose();
            }
            catch ( IOException ioe )
            {
                try
                {
                    if ( !pagedFile.isDeleteOnClose() )
                    {
                        pagedFile.flushAndForce();
                    }
                    maybeForceCleanState( cursorContext );
                    doClose();
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
    }

    public void setDeleteOnClose( boolean deleteOnClose )
    {
        pagedFile.setDeleteOnClose( deleteOnClose );
    }

    private void maybeForceCleanState( CursorContext cursorContext ) throws IOException
    {
        if ( cleaning != null && !changesSinceLastCheckpoint && !cleaning.needed() )
        {
            clean = true;
            if ( !pagedFile.isDeleteOnClose() )
            {
                forceState( cursorContext );
            }
        }
    }

    private void doClose()
    {
        if ( pagedFile != null )
        {
            // Will be null if exception while mapping file
            pagedFile.close();
        }
        closed = true;
    }

    /**
     * Use default value for ratioToKeepInLeftOnSplit
     * @param cursorContext underlying page cursor context
     * @see GBPTree#writer(double, CursorContext)
     */
    public Writer<KEY,VALUE> writer( CursorContext cursorContext ) throws IOException
    {
        return writer( InternalTreeLogic.DEFAULT_SPLIT_RATIO, cursorContext );
    }

    /**
     * Returns a {@link Writer} able to modify the index, i.e. insert and remove keys/values.
     * After usage the returned writer must be closed, typically by using try-with-resource clause.
     *
     * @param ratioToKeepInLeftOnSplit Decide how much to keep in left node on split, 0=keep nothing, 0.5=split 50-50, 1=keep everything.
     * @param cursorContext underlying page cursor context
     * @return the single {@link Writer} for this index. The returned writer must be
     * {@link Writer#close() closed} before another caller can acquire this writer.
     * @throws IOException on error accessing the index.
     * @throws IllegalStateException for calls made between a successful call to this method and closing the
     * returned writer.
     */
    public Writer<KEY,VALUE> writer( double ratioToKeepInLeftOnSplit, CursorContext cursorContext ) throws IOException
    {
        assertNotReadOnly( "Open tree writer." );
        return unsafeWriter( ratioToKeepInLeftOnSplit, cursorContext );
    }

    public Writer<KEY,VALUE> unsafeWriter(  CursorContext cursorContext ) throws IOException
    {
        return unsafeWriter( InternalTreeLogic.DEFAULT_SPLIT_RATIO, cursorContext );
    }

    public Writer<KEY,VALUE> unsafeWriter( double ratioToKeepInLeftOnSplit, CursorContext cursorContext ) throws IOException
    {
        writer.initialize( ratioToKeepInLeftOnSplit, cursorContext );
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
     * created in {@link #createCleanupJob(RecoveryCleanupWorkCollector, boolean, String)}.
     */
    private void bumpUnstableGeneration()
    {
        generation = generation( stableGeneration( generation ), unstableGeneration( generation ) + 1 );
    }

    private void forceState( CursorContext cursorContext ) throws IOException
    {
        if ( changesSinceLastCheckpoint )
        {
            throw new IllegalStateException( "It seems that this method has been called in the wrong state. " +
                    "It's expected that this is called after opening this tree, but before any changes " +
                    "have been made" );
        }

        writeState( pagedFile, CARRY_OVER_PREVIOUS_HEADER, cursorContext );
        pagedFile.flushAndForce();
    }

    /**
     * Called on start if tree was not clean.
     */
    private CleanupJob createCleanupJob( RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean needsCleaning, String treeName )
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
                            stableGeneration, unstableGeneration, monitor, pageCacheTracer, treeName );
            GBPTreeCleanupJob cleanupJob = new GBPTreeCleanupJob( crashGenerationCleaner, lock, monitor, indexFile );
            recoveryCleanupWorkCollector.add( cleanupJob );
            return cleanupJob;
        }
    }

    @VisibleForTesting
    public <VISITOR extends GBPTreeVisitor<KEY,VALUE>> VISITOR visit( VISITOR visitor, CursorContext cursorContext ) throws IOException
    {
        try ( PageCursor cursor = openRootCursor( PF_SHARED_READ_LOCK, cursorContext ) )
        {
            new GBPTreeStructure<>( bTreeNode, layout, stableGeneration( generation ), unstableGeneration( generation ) )
                    .visitTree( cursor, writer.cursor, visitor, cursorContext );
            freeList.visitFreelist( visitor, cursorContext );
        }
        return visitor;
    }

    @SuppressWarnings( "unused" )
    public void printTree( CursorContext cursorContext ) throws IOException
    {
        printTree( PrintConfig.defaults(), cursorContext );
    }

    // Utility method
    /**
     * Prints the contents of the tree to System.out.
     *
     * @param printConfig {@link PrintConfig} containing configurations for this printing.
     * @throws IOException on I/O error.
     */
    @SuppressWarnings( "SameParameterValue" )
    void printTree( PrintConfig printConfig, CursorContext cursorContext ) throws IOException
    {
        PrintingGBPTreeVisitor<KEY,VALUE> printingVisitor = new PrintingGBPTreeVisitor<>( printConfig );
        visit( printingVisitor, cursorContext );
    }

    // Utility method
    public void printState( CursorContext cursorContext ) throws IOException
    {
        try ( PageCursor cursor = openRootCursor( PF_SHARED_READ_LOCK, cursorContext ) )
        {
            PrintingGBPTreeVisitor<KEY,VALUE> printingVisitor = new PrintingGBPTreeVisitor<>( PrintConfig.defaults().printState() );
            GBPTreeStructure.visitTreeState( cursor, printingVisitor );
        }
    }

    // Utility method
    /**
     * Print node with given id to System.out, if node with id exists.
     * @param id the page id of node to print
     */
    void printNode( long id, CursorContext cursorContext ) throws IOException
    {
        if ( id <= freeList.lastId() )
        {
            // Use write lock to avoid adversary interference
            try ( PageCursor cursor = pagedFile.io( id, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext ) )
            {
                cursor.next();
                byte nodeType = TreeNode.nodeType( cursor );
                if ( nodeType == TreeNode.NODE_TYPE_TREE_NODE )
                {
                    bTreeNode.printNode( cursor, false, true, stableGeneration( generation ), unstableGeneration( generation ), cursorContext );
                }
            }
        }
    }

    public boolean consistencyCheck( CursorContext cursorContext ) throws IOException
    {
        return consistencyCheck( true, cursorContext );
    }

    public boolean consistencyCheck( boolean reportDirty, CursorContext cursorContext ) throws IOException
    {
        ThrowingConsistencyCheckVisitor<KEY> reporter = new ThrowingConsistencyCheckVisitor<>();
        return consistencyCheck( reporter, reportDirty, cursorContext );
    }

    public boolean consistencyCheck( GBPTreeConsistencyCheckVisitor<KEY> visitor, CursorContext cursorContext ) throws IOException
    {
        return consistencyCheck( visitor, true, cursorContext );
    }

    // Utility method
    public boolean consistencyCheck( GBPTreeConsistencyCheckVisitor<KEY> visitor, boolean reportDirty, CursorContext cursorContext ) throws IOException
    {
        CleanTrackingConsistencyCheckVisitor<KEY> cleanTrackingVisitor = new CleanTrackingConsistencyCheckVisitor<>( visitor );
        try ( PageCursor cursor = pagedFile.io( 0L /*ignored*/, PF_SHARED_READ_LOCK, cursorContext ) )
        {
            long unstableGeneration = unstableGeneration( generation );
            GBPTreeConsistencyChecker<KEY> consistencyChecker = new GBPTreeConsistencyChecker<>( bTreeNode, layout, freeList,
                    stableGeneration( generation ), unstableGeneration, reportDirty );

            if ( dirtyOnStartup && reportDirty )
            {
                cleanTrackingVisitor.dirtyOnStartup( indexFile );
            }
            consistencyChecker.check( indexFile, cursor, root, cleanTrackingVisitor, cursorContext );
        }
        catch ( TreeInconsistencyException | MetadataMismatchException | CursorException e )
        {
            cleanTrackingVisitor.exception( e );
        }
        return cleanTrackingVisitor.isConsistent();
    }

    @VisibleForTesting
    public void unsafe( GBPTreeUnsafe<KEY,VALUE> unsafe, CursorContext cursorContext ) throws IOException
    {
        TreeState state;
        try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext ) )
        {
            // todo find better way of getting TreeState?
            Pair<TreeState,TreeState> states = TreeStatePair.readStatePages( cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B );
            state = TreeStatePair.selectNewestValidState( states );
        }
        unsafe.access( pagedFile, layout, bTreeNode, state );
    }

    @Override
    public String toString()
    {
        long generation = this.generation;
        return format( "GB+Tree[file:%s, layout:%s, generation:%d/%d]",
                indexFile.toAbsolutePath(), layout,
                stableGeneration( generation ), unstableGeneration( generation ) );
    }

    private <E extends Throwable> void appendTreeInformation( E e )
    {
        Exceptions.withMessage( e, e.getMessage() + " | " + toString() );
    }

    private static class SeekDepthMonitor extends SeekCursor.MonitorAdaptor
    {
        private boolean reachedLeafLevel;

        @Override
        public void leafNode( int depth, int keyCount )
        {
            reachedLeafLevel = true;
        }
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
        private CursorContext cursorContext;

        // Writer can't live past a checkpoint because of the mutex with checkpoint,
        // therefore safe to locally cache these generation fields from the volatile generation in the tree
        private long stableGeneration;
        private long unstableGeneration;
        private double ratioToKeepInLeftOnSplit;

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
         * @param ratioToKeepInLeftOnSplit Decide how much to keep in left node on split, 0=keep nothing, 0.5=split 50-50, 1=keep everything.
         * @param cursorContext underlying page cursor context
         */
        void initialize( double ratioToKeepInLeftOnSplit, CursorContext cursorContext ) throws IOException
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
                // Block here until cleaning has completed, if cleaning was required
                lock.writerAndCleanerLock();
                assertRecoveryCleanSuccessful();
                cursor = openRootCursor( PagedFile.PF_SHARED_WRITE_LOCK, cursorContext );
                this.cursorContext = cursorContext;
                stableGeneration = stableGeneration( generation );
                unstableGeneration = unstableGeneration( generation );
                this.ratioToKeepInLeftOnSplit = ratioToKeepInLeftOnSplit;
                assert assertNoSuccessor( cursor, stableGeneration, unstableGeneration );
                treeLogic.initialize( cursor, ratioToKeepInLeftOnSplit );
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
        public void put( KEY key, VALUE value )
        {
            merge( key, value, ValueMergers.overwrite() );
        }

        @Override
        public void merge( KEY key, VALUE value, ValueMerger<KEY,VALUE> valueMerger )
        {
            internalMerge( key, value, valueMerger, true );
        }

        @Override
        public void mergeIfExists( KEY key, VALUE value, ValueMerger<KEY,VALUE> valueMerger )
        {
            internalMerge( key, value, valueMerger, false );
        }

        private void internalMerge( KEY key, VALUE value, ValueMerger<KEY,VALUE> valueMerger, boolean createIfNotExists )
        {
            try
            {
                treeLogic.insert( cursor, structurePropagation, key, value, valueMerger, createIfNotExists,
                        stableGeneration, unstableGeneration, cursorContext );

                handleStructureChanges( cursorContext );
            }
            catch ( IOException e )
            {
                appendTreeInformation( e );
                throw new UncheckedIOException( e );
            }
            catch ( Throwable t )
            {
                appendTreeInformation( t );
                throw t;
            }

            checkOutOfBounds( cursor );
        }

        private void setRoot( long rootPointer )
        {
            long rootId = GenerationSafePointerPair.pointer( rootPointer );
            GBPTree.this.setRoot( rootId, unstableGeneration );
            treeLogic.initialize( cursor, ratioToKeepInLeftOnSplit );
        }

        @Override
        public VALUE remove( KEY key )
        {
            VALUE result;
            try
            {
                result = treeLogic.remove( cursor, structurePropagation, key, layout.newValue(),
                        stableGeneration, unstableGeneration, cursorContext );

                handleStructureChanges( cursorContext );
            }
            catch ( IOException e )
            {
                appendTreeInformation( e );
                throw new UncheckedIOException( e );
            }
            catch ( Throwable e )
            {
                appendTreeInformation( e );
                throw e;
            }

            checkOutOfBounds( cursor );
            return result;
        }

        private void handleStructureChanges( CursorContext cursorContext ) throws IOException
        {
            if ( structurePropagation.hasRightKeyInsert )
            {
                // New root
                long newRootId = freeList.acquireNewId( stableGeneration, unstableGeneration, cursorContext );
                PageCursorUtil.goTo( cursor, "new root", newRootId );

                bTreeNode.initializeInternal( cursor, stableGeneration, unstableGeneration );
                bTreeNode.setChildAt( cursor, structurePropagation.midChild, 0,
                        stableGeneration, unstableGeneration );
                bTreeNode.insertKeyAndRightChildAt( cursor, structurePropagation.rightKey, structurePropagation.rightChild, 0, 0,
                        stableGeneration, unstableGeneration, cursorContext );
                TreeNode.setKeyCount( cursor, 1 );
                setRoot( newRootId );
                monitor.treeGrowth();
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
            lock.writerAndCleanerUnlock();
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

    /**
     * Total size limit for key and value.
     * This limit includes storage overhead that is specific to key implementation for example entity id or meta data about type.
     * @return Total size limit for key and value or {@link TreeNode#NO_KEY_VALUE_SIZE_CAP} if no such value exists.
     */
    public int keyValueSizeCap()
    {
        return bTreeNode.keyValueSizeCap();
    }

    int inlineKeyValueSizeCap()
    {
        return bTreeNode.inlineKeyValueSizeCap();
    }

    private static <KEY, VALUE> OffloadStoreImpl<KEY,VALUE> buildOffload( Layout<KEY,VALUE> layout, IdProvider idProvider, PagedFile pagedFile, int pageSize )
    {
        OffloadIdValidator idValidator = id -> id >= IdSpace.MIN_TREE_NODE_ID && id <= pagedFile.getLastPageId();
        return new OffloadStoreImpl<>( layout, idProvider, pagedFile::io, idValidator, pageSize );
    }
}
