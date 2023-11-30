/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.Arrays.asList;
import static org.neo4j.index.internal.gbptree.CursorCreator.bind;
import static org.neo4j.index.internal.gbptree.GBPTreeOpenOptions.NO_FLUSH_ON_CLOSE;
import static org.neo4j.index.internal.gbptree.Generation.generation;
import static org.neo4j.index.internal.gbptree.Generation.stableGeneration;
import static org.neo4j.index.internal.gbptree.Generation.unstableGeneration;
import static org.neo4j.index.internal.gbptree.GenerationSafePointer.FIRST_STABLE_GENERATION;
import static org.neo4j.index.internal.gbptree.GenerationSafePointer.FIRST_UNSTABLE_GENERATION;
import static org.neo4j.index.internal.gbptree.Header.CARRY_OVER_PREVIOUS_HEADER;
import static org.neo4j.index.internal.gbptree.Header.replace;
import static org.neo4j.index.internal.gbptree.PointerChecking.checkOutOfBounds;
import static org.neo4j.io.pagecache.PageCacheOpenOptions.MULTI_VERSIONED;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.common.DependencyResolver;
import org.neo4j.function.ThrowingAction;
import org.neo4j.index.internal.gbptree.GBPTreeConsistencyChecker.ConsistencyCheckState;
import org.neo4j.index.internal.gbptree.Header.Reader;
import org.neo4j.index.internal.gbptree.RootLayer.TreeRootsVisitor;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageCursorUtil;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.util.VisibleForTesting;

/**
 * A generation-aware B+tree (GB+Tree) implementation directly atop a {@link PageCache} with no caching in between.
 * Additionally internal and leaf nodes on same level are linked both left and right (sibling pointers),
 * this to provide correct reading when concurrently writing to the tree.
 * <p>
 * Generation is incremented on {@link #checkpoint(FileFlushEvent, CursorContext)} check-pointing}.
 * Generation awareness allows for recovery from last {@link #checkpoint(FileFlushEvent, CursorContext)}, provided the same updates
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
 * {@link DataTree#writer(int, CursorContext) Writes} happen to the tree and are made durable and
 * safe on next call to {@link #checkpoint(FileFlushEvent, CursorContext)}. Writes which happens after the last
 * {@link #checkpoint(FileFlushEvent, CursorContext)} are not safe if there's a {@link #close()} or JVM crash in between, i.e:
 *
 * <pre>
 * w: write
 * c: checkpoint
 * x: crash or {@link #close()}
 *
 * TIME |--w--w----w--c--ww--w-c-w--w-ww--w--w---x------|
 *         ^------ safe -----^   ^- unsafe --^
 * </pre>
 *
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
 * <p>
 * Main benefits of using a {@link MultiRootGBPTree} includes:
 * <ul>
 *     <li>Parallel writers only locks its own data tree</li>
 *     <li>Common bits of data that all keys in a {@link DataTree} contains can be omitted and instead only exist in the root key</li>
 *     <li>Since the root layer is cached to some extent, access in each {@link DataTree} is generally not as deep, i.e. less page accesses
 *     and less key search.</li>
 * </ul>
 *
 * @param <KEY> type of keys
 * @param <VALUE> type of values
 */
public class MultiRootGBPTree<ROOT_KEY, KEY, VALUE> implements Closeable {
    private static final String INDEX_INTERNAL_TAG = "indexInternal";

    /**
     * For monitoring {@link GBPTree}.
     */
    public interface Monitor {
        /**
         * Adapter for {@link Monitor}.
         */
        class Adaptor implements Monitor {
            @Override
            public void checkpointStarted() {}

            @Override
            public void checkpointCompleted() {}

            @Override
            public void noStoreFile() {}

            @Override
            public void needRecreation() {}

            @Override
            public void cleanupRegistered() {}

            @Override
            public void cleanupStarted() {}

            @Override
            public void cleanupFinished(
                    long numberOfPagesVisited,
                    long numberOfTreeNodes,
                    long numberOfCleanedCrashPointers,
                    long durationMillis) {}

            @Override
            public void cleanupClosed() {}

            @Override
            public void cleanupFailed(Throwable throwable) {}

            @Override
            public void startupState(boolean clean) {}

            @Override
            public void treeGrowth() {}

            @Override
            public void treeShrink() {}
        }

        class Delegate implements Monitor {
            private final Monitor delegate;

            public Delegate(Monitor delegate) {
                this.delegate = delegate;
            }

            @Override
            public void checkpointStarted() {
                delegate.checkpointStarted();
            }

            @Override
            public void checkpointCompleted() {
                delegate.checkpointCompleted();
            }

            @Override
            public void noStoreFile() {
                delegate.noStoreFile();
            }

            @Override
            public void needRecreation() {
                delegate.needRecreation();
            }

            @Override
            public void cleanupRegistered() {
                delegate.cleanupRegistered();
            }

            @Override
            public void cleanupStarted() {
                delegate.cleanupStarted();
            }

            @Override
            public void cleanupFinished(
                    long numberOfPagesVisited,
                    long numberOfTreeNodes,
                    long numberOfCleanedCrashPointers,
                    long durationMillis) {
                delegate.cleanupFinished(
                        numberOfPagesVisited, numberOfTreeNodes, numberOfCleanedCrashPointers, durationMillis);
            }

            @Override
            public void cleanupClosed() {
                delegate.cleanupClosed();
            }

            @Override
            public void cleanupFailed(Throwable throwable) {
                delegate.cleanupFailed(throwable);
            }

            @Override
            public void startupState(boolean clean) {
                delegate.startupState(clean);
            }

            @Override
            public void treeGrowth() {
                delegate.treeGrowth();
            }

            @Override
            public void treeShrink() {
                delegate.treeShrink();
            }
        }

        /**
         * Called when a {@link GBPTree#checkpoint(FileFlushEvent, CursorContext)} has started, right after
         * current writers have been drained. Future writers after this point onwards and until the next call
         * to {@link #checkpointCompleted()} will do eager flushing of their changes.
         */
        void checkpointStarted();

        /**
         * Called when a {@link GBPTree#checkpoint(FileFlushEvent, CursorContext)} has been completed and generation
         * has been bumped, but right before {@link GBPTree#writer(CursorContext)} writers are re-enabled.
         * Writers from this point onwards and until the next call to {@link #checkpointStarted()} will not do
         * eager flushing of their changes.
         */
        void checkpointCompleted();

        /**
         * Called when the tree was started on no existing store file and so will be created.
         */
        void noStoreFile();

        /**
         * Called when the tree was started in a state where file exists, but it needs to be recreated.
         */
        void needRecreation();

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
        void cleanupFinished(
                long numberOfPagesVisited,
                long numberOfTreeNodes,
                long numberOfCleanedCrashPointers,
                long durationMillis);

        /**
         * Called when cleanup job is closed and lock is released
         */
        void cleanupClosed();

        /**
         * Called when cleanup job catches a throwable
         * @param throwable cause of failure
         */
        void cleanupFailed(Throwable throwable);

        /**
         * Report tree state on startup.
         *
         * @param clean true if tree was clean on startup.
         */
        void startupState(boolean clean);

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
    public static final Header.Reader NO_HEADER_READER = headerData -> {};

    /**
     * No-op header writer.
     */
    public static final Consumer<PageCursor> NO_HEADER_WRITER = pc -> {};

    /**
     * Paged file in a {@link PageCache} providing the means of storage.
     */
    protected final PagedFile pagedFile;

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
    protected final Layout<KEY, VALUE> layout;

    /**
     * A free-list of released ids. Acquiring new ids involves first trying out the free-list and then,
     * as a fall-back allocate a new id at the end of the store.
     */
    protected final FreeListIdProvider freeList;

    /**
     * Tells whether there have been made changes (using {@link DataTree#writer(int, CursorContext)}) to this tree
     * since last call to {@link #checkpoint(FileFlushEvent, CursorContext)}. This variable is set when calling {@link DataTree#writer(int, CursorContext)}
     * and cleared inside {@link #checkpoint(FileFlushEvent, CursorContext)}.
     */
    private final AtomicBoolean changesSinceLastCheckpoint = new AtomicBoolean();

    /**
     * These locks together controls access to cleaning, writing and checkpointing the tree.
     * <ul>
     *     <li>Writing needs cleaner to have completed and shared checkpoint lock + writer lock</li>
     *     <li>Checkpointing needs cleaner to have completed and exclusive checkpoint lock + writer lock</li>
     * </ul>
     */
    private volatile CountDownLatch cleanerLock;

    private final ReadWriteLock checkpointLock = new ReentrantReadWriteLock();
    private final ReadWriteLock writerLock = new ReentrantReadWriteLock();

    /**
     * Page size, i.e. tree node size, of the tree nodes in this tree. The page size is determined on
     * tree creation, stored in meta page and read when opening tree later.
     */
    protected final int payloadSize;

    /**
     * Generation of the tree. This variable contains both stable and unstable generation and is
     * represented as one long to get atomic updates of both stable and unstable generation for readers.
     * Both stable and unstable generation are unsigned ints, i.e. 32 bits each.
     *
     * <ul>
     * <li>stable generation, generation which has survived the last {@link #checkpoint(FileFlushEvent, CursorContext)}</li>
     * <li>unstable generation, current generation under evolution. This generation will be the
     * {@link Generation#stableGeneration(long)} after the next {@link #checkpoint(FileFlushEvent, CursorContext)}</li>
     * </ul>
     */
    private volatile long generation;

    /**
     * Supplier of generation to readers. This supplier will actually very rarely be used, because normally
     * a {@link SeekCursor} is bootstrapped from {@link #generation}. The only time this supplier will be
     * used is so that a long-running {@link SeekCursor} can keep up with a generation change after
     * a checkpoint, if the cursor lives that long.
     */
    protected final LongSupplier generationSupplier = () -> generation;

    /**
     * Called on certain events.
     */
    private final Monitor monitor;

    /**
     * If this tree is read only, no changes will be made to it. No generation bumping, no checkpointing, no nothing.
     */
    private final boolean readOnly;

    /**
     * Underlying cursor context factory. Should be used to create page cursors tracers
     * only for cases where work is performed by tree itself: construction or shutdown, otherwise tracers caller
     * should provide correct context related tracer that should be used
     */
    private final CursorContextFactory contextFactory;

    /**
     * Underlying page cache tracer
     */
    private final PageCacheTracer pageCacheTracer;

    /**
     * Array of {@link OpenOption} which is passed to calls to {@link PageCache#map(Path, int, String, ImmutableSet)}
     * at open/create. When initially creating the file an array consisting of {@link StandardOpenOption#CREATE}
     * concatenated with the contents of this array is passed into the map call.
     */
    private final ImmutableSet<OpenOption> openOptions;

    /**
     * Whether this tree has been closed. Accessed and changed solely in
     * {@link #close()} to be able to close tree multiple times gracefully.
     */
    @SuppressWarnings("UnusedAssignment")
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

    protected final RootLayer<ROOT_KEY, KEY, VALUE> rootLayer;

    protected final RootLayerSupport rootLayerSupport;

    /**
     * Flag which is set in part of checkpoint where, on initializing a writer, the writer must make sure
     * to use write cursors that eagerly flush changes. Basically the writer must ensure that all pages that
     * it has touched has been flushed before writer is closed.
     */
    private volatile boolean writersMustEagerlyFlush;

    private final StructureWriteLog structureWriteLog;

    /**
     * Opens an index {@code indexFile} in the {@code pageCache}, creating and initializing it if it doesn't exist. If the index doesn't exist it will be
     * created and the {@link Layout} and {@code pageSize} will be written in index header. If the index exists it will be opened and the {@link Layout} will be
     * matched with the information in the header. At the very least {@link Layout#identifier()} will be matched.
     * <p>
     * On start, tree can be in a clean or dirty state. If dirty, it will {@link #createCleanupJob(RecoveryCleanupWorkCollector, boolean)} and clean crashed
     * pointers as part of constructor. Tree is only clean if since last time it was opened it was {@link #close()}  closed} without any non-checkpointed
     * changes present. Correct usage pattern of the GBPTree is:
     *
     * <pre>
     *     try ( GBPTree tree = new GBPTree(...) )
     *     {
     *         // Use the tree
     *         tree.checkpoint( ... );
     *     }
     * </pre>
     * <p>
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
     * @param pageCache                    {@link PageCache} to use to map index file
     * @param fileSystem                   {@link FileSystemAbstraction} which the index file is mapped in
     * @param indexFile                    {@link Path} containing the actual index
     * @param layout                       {@link Layout} to use in the tree, this must match the existing layout
     *                                     we're just opening the index
     * @param monitor                      {@link Monitor} for monitoring {@link GBPTree}.
     * @param headerReader                 reads header data, previously written using {@link #checkpoint(Consumer, FileFlushEvent, CursorContext)}
     *                                     or {@link #close()}
     * @param recoveryCleanupWorkCollector collects recovery cleanup jobs for execution after recovery.
     * @param readOnly whether this tree should be opened in read-only mode. If {@code true} then no generation
     * bump is done on open and writes/checkpoints are prevented.
     * @param databaseName                 name of the database this tree belongs to.
     * @param name                         name of the tree that will be used when describing work related to this tree.
     * @throws UncheckedIOException      on page cache error
     * @throws MetadataMismatchException if meta information does not match constructor parameters or meta page is missing
     */
    public MultiRootGBPTree(
            PageCache pageCache,
            FileSystemAbstraction fileSystem,
            Path indexFile,
            Layout<KEY, VALUE> layout,
            Monitor monitor,
            Header.Reader headerReader,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            boolean readOnly,
            ImmutableSet<OpenOption> engineOpenOptions,
            String databaseName,
            String name,
            CursorContextFactory contextFactory,
            RootLayerConfiguration<ROOT_KEY> rootLayerConfiguration,
            PageCacheTracer pageCacheTracer,
            DependencyResolver dependencyResolver,
            TreeNodeLayoutFactory treeNodeLayoutFactory,
            StructureWriteLog structureWriteLog)
            throws MetadataMismatchException {
        this.indexFile = indexFile;
        this.monitor = monitor;
        this.readOnly = readOnly;
        this.contextFactory = contextFactory;
        this.openOptions = treeOpenOptions(engineOpenOptions);
        this.pageCacheTracer = pageCacheTracer;
        this.generation = Generation.generation(FIRST_STABLE_GENERATION, FIRST_UNSTABLE_GENERATION);
        this.layout = layout;
        this.structureWriteLog = structureWriteLog;

        try (var cursorContext = contextFactory.create(INDEX_INTERNAL_TAG)) {
            var openResult = openOrCreate(fileSystem, pageCache, indexFile, databaseName, openOptions);
            boolean created = openResult.created;
            this.pagedFile = openResult.pagedFile;
            closed = false;

            if (!created) {
                // Verify stored payload size before we verify state pages
                // since the offset for those pages is based on page size
                verifyPayloadSize(pagedFile, cursorContext);
                created = needRecreation(pagedFile, cursorContext, monitor, readOnly);
            }

            this.payloadSize = pagedFile.payloadSize();
            this.freeList = new FreeListIdProvider(pagedFile.payloadSize());
            TreeNodeLatchService latchService = new TreeNodeLatchService();
            var treeNodeSelector = treeNodeLayoutFactory.createSelector(engineOpenOptions);
            this.rootLayerSupport = new RootLayerSupport(
                    pagedFile,
                    generationSupplier,
                    this::appendTreeInformation,
                    latchService,
                    freeList,
                    monitor,
                    this::awaitCleaner,
                    checkpointLock,
                    writerLock,
                    changesSinceLastCheckpoint,
                    name,
                    readOnly,
                    () -> writersMustEagerlyFlush,
                    structureWriteLog);
            this.rootLayer = rootLayerConfiguration.buildRootLayer(
                    rootLayerSupport, layout, treeNodeSelector, dependencyResolver);

            // Create or load state
            if (created) {
                initializeAfterCreation(cursorContext);
                dirtyOnStartup = false;
                cleaning = CleanupJob.CLEAN;
            } else {
                initialize(pagedFile, headerReader, cursorContext);
                dirtyOnStartup = !clean;
                if (!readOnly) {
                    clean = false;
                    bumpUnstableGeneration();
                    try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                        forceState(flushEvent, cursorContext);
                    }
                    cleaning = createCleanupJob(recoveryCleanupWorkCollector, dirtyOnStartup);
                } else {
                    cleaning = CleanupJob.CLEAN;
                }
            }
            this.monitor.startupState(!dirtyOnStartup);
        } catch (IOException e) {
            throw exitConstructor(new UncheckedIOException(e));
        } catch (Throwable e) {
            throw exitConstructor(e);
        }
    }

    private RuntimeException exitConstructor(Throwable throwable) {
        try {
            close();
        } catch (IOException e) {
            throwable = Exceptions.chain(new UncheckedIOException(e), throwable);
        }

        appendTreeInformation(throwable);
        Exceptions.throwIfUnchecked(throwable);
        return new RuntimeException(throwable);
    }

    private void initializeAfterCreation(CursorContext cursorContext) throws IOException {
        var firstRoot = new Root(IdSpace.MIN_TREE_NODE_ID, unstableGeneration(generation));
        rootLayer.initializeAfterCreation(firstRoot, cursorContext);
        freeList.initializeAfterCreation(
                bind(pagedFile, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext), IdSpace.MIN_FREELIST_NODE_ID);
        structureWriteLog.createRoot(unstableGeneration(generation), firstRoot.id());
    }

    /**
     * Map index file using provided pageCache. Create it if it doesn't exist.
     * @return {@link OpenResult} containing {@link PagedFile} and created file that indicate if file was created or not..
     * @throws IOException if something goes wrong in {@link PageCache}
     * @throws TreeFileNotFoundException if file didn't exist and we are in read only mode.
     */
    private OpenResult openOrCreate(
            FileSystemAbstraction fs,
            PageCache pageCache,
            Path indexFile,
            String databaseName,
            ImmutableSet<OpenOption> openOptions)
            throws IOException, TreeFileNotFoundException {
        openOptions = openOptions.newWithoutAll(asList(GBPTreeOpenOptions.values()));
        if (!fs.fileExists(indexFile)) {
            if (readOnly) {
                throw new TreeFileNotFoundException(
                        "Can not create new tree file '" + indexFile + "' in read only mode.");
            }
            monitor.noStoreFile();
            openOptions = openOptions.newWith(CREATE);
            return new OpenResult(pageCache.map(indexFile, pageCache.pageSize(), databaseName, openOptions), true);
        }
        return new OpenResult(pageCache.map(indexFile, pageCache.pageSize(), databaseName, openOptions), false);
    }

    /**
     * The tree needs to be recreated from scratch if it hasn't seen a first successful checkpoint.
     * This is the case if and only if both state pages are empty.
     *
     * If the tree needs to be recreated the minimal tree pages are zapped.
     */
    private static boolean needRecreation(
            PagedFile pagedFile, CursorContext cursorContext, Monitor monitor, boolean readOnly) throws IOException {
        try (var cursor = pagedFile.io(
                IdSpace.META_PAGE_ID, readOnly ? PF_SHARED_READ_LOCK : PF_SHARED_WRITE_LOCK, cursorContext)) {
            var bytes = new byte[pagedFile.payloadSize()];
            var needRecreation = pageIsEmpty(cursor, bytes, IdSpace.STATE_PAGE_A)
                    && pageIsEmpty(cursor, bytes, IdSpace.STATE_PAGE_B);
            if (needRecreation) {
                if (readOnly) {
                    throw new TreeFileNotFoundException(
                            "Can not re-create tree file '" + pagedFile.path() + "' in read only mode.");
                }
                zapPage(cursor, IdSpace.META_PAGE_ID);
                zapPage(cursor, IdSpace.STATE_PAGE_A);
                zapPage(cursor, IdSpace.STATE_PAGE_B);
                zapPage(cursor, IdSpace.MIN_TREE_NODE_ID);
                zapPage(cursor, IdSpace.MIN_FREELIST_NODE_ID);
                monitor.needRecreation();
            }
            return needRecreation;
        }
    }

    private static boolean pageIsEmpty(PageCursor cursor, byte[] bytes, long pageId) throws IOException {
        if (!cursor.next(pageId)) {
            return true;
        }
        do {
            Arrays.fill(bytes, (byte) 0);
            cursor.getBytes(bytes);
        } while (cursor.shouldRetry());
        return allZeroes(bytes);
    }

    private static void zapPage(PageCursor cursor, long pageId) throws IOException {
        cursor.next(pageId);
        cursor.zapPage();
    }

    private static boolean allZeroes(final byte[] array) {
        for (byte b : array) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }

    private static PagedFile openExistingIndexFile(
            PageCache pageCache,
            Path indexFile,
            CursorContext cursorContext,
            String databaseName,
            ImmutableSet<OpenOption> openOptions)
            throws IOException, MetadataMismatchException {
        PagedFile pagedFile =
                pageCache.map(indexFile, pageCache.pageSize(), databaseName, treeOpenOptions(openOptions));
        // This index already exists, verify meta data aligns with expectations

        MutableBoolean pagedFileOpen = new MutableBoolean(true);
        boolean success = false;
        try {
            // We're only interested in the page size really
            verifyPayloadSize(pagedFile, cursorContext);
            success = true;
            return pagedFile;
        } catch (IllegalStateException e) {
            throw new MetadataMismatchException("Index is not fully initialized since it's missing the meta page", e);
        } finally {
            if (!success && pagedFileOpen.booleanValue()) {
                pagedFile.close();
            }
        }
    }

    private void initialize(PagedFile pagedFile, Header.Reader headerReader, CursorContext cursorContext)
            throws IOException {
        var openOptions = this.openOptions;
        TreeState state = readHeaderFromPagedFiled(pagedFile, headerReader, cursorContext, openOptions);
        generation = Generation.generation(state.stableGeneration(), state.unstableGeneration());
        var root = new Root(state.rootId(), state.rootGeneration());
        rootLayer.initialize(root, cursorContext);

        long lastId = state.lastId();
        long freeListWritePageId = state.freeListWritePageId();
        long freeListReadPageId = state.freeListReadPageId();
        int freeListWritePos = state.freeListWritePos();
        int freeListReadPos = state.freeListReadPos();
        freeList.initialize(lastId, freeListWritePageId, freeListReadPageId, freeListWritePos, freeListReadPos);
        clean = state.isClean();
    }

    public static <T extends Header.Reader> Optional<T> readHeader(
            FileSystemAbstraction fileSystem, Path indexFile, T headerReader, ImmutableSet<OpenOption> openOptions) {
        if (fileSystem.fileExists(indexFile)) {
            try (StoreChannel channel = fileSystem.read(indexFile);
                    var scopedBuffer =
                            new NativeScopedBuffer(512, getEndianness(openOptions), EmptyMemoryTracker.INSTANCE)) {
                ByteBuffer buffer = scopedBuffer.getBuffer();

                // Read metadata to determine page size
                Meta meta = getMeta(buffer, channel);
                int pageSize = meta.getPayloadSize();

                // Read both states
                TreeState stateA = getTreeState(buffer, channel, pageSize, IdSpace.STATE_PAGE_A);
                TreeState stateB = getTreeState(buffer, channel, pageSize, IdSpace.STATE_PAGE_B);

                // Determine which one is stable
                TreeState state = TreeStatePair.selectNewestValidState(Pair.of(stateA, stateB));

                readEmbeddedHeader(headerReader, channel, buffer, pageSize, state);
                return Optional.of(headerReader);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return Optional.empty();
    }

    private static void readEmbeddedHeader(
            Header.Reader headerReader, StoreChannel channel, ByteBuffer buffer, int pageSize, TreeState state)
            throws IOException {
        buffer.clear().limit(Integer.BYTES);
        long headerPosition = state.pageId() * pageSize + TreeState.SIZE;
        channel.position(headerPosition);
        channel.readAll(buffer);
        int headerSize = buffer.flip().getInt();
        buffer.clear().limit(headerSize);
        channel.readAll(buffer);
        buffer.flip();
        headerReader.read(buffer);
    }

    private static ByteOrder getEndianness(ImmutableSet<OpenOption> openOptions) {
        return openOptions.contains(PageCacheOpenOptions.BIG_ENDIAN) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    private static Meta getMeta(ByteBuffer buffer, StoreChannel channel) throws IOException {
        buffer.clear().limit(Meta.META_SIZE);
        channel.position(IdSpace.META_PAGE_ID);
        channel.readAll(buffer);
        buffer.flip();
        return Meta.read(buffer);
    }

    private static TreeState getTreeState(ByteBuffer buffer, StoreChannel read, int pageSize, long statePage)
            throws IOException {
        buffer.clear().limit(TreeState.SIZE);
        read.position(pageSize * statePage);
        read.readAll(buffer);
        buffer.flip();
        return TreeState.read(statePage, buffer);
    }

    /**
     * Use when you are only interested in reading the header of existing index file without opening the index for writes.
     * Useful when reading header and the demands on matching layout can be relaxed a bit.
     *
     * @param pageCache {@link PageCache} to use to map index file
     * @param indexFile {@link Path} containing the actual index
     * @param headerReader reads header data, previously written using {@link #checkpoint( Consumer, FileFlushEvent, CursorContext)}
     * or {@link #close()}
     * @param databaseName name of the database index file belongs to.
     * @throws IOException On page cache error
     * @throws MetadataMismatchException if some meta page is missing (tree not fully initialized)
     */
    public static void readHeader(
            PageCache pageCache,
            Path indexFile,
            Header.Reader headerReader,
            String databaseName,
            CursorContext cursorContext,
            ImmutableSet<OpenOption> openOptions)
            throws IOException, MetadataMismatchException {
        try (PagedFile pagedFile =
                openExistingIndexFile(pageCache, indexFile, cursorContext, databaseName, openOptions)) {
            readHeaderFromPagedFiled(pagedFile, headerReader, cursorContext, openOptions);
        } catch (Throwable t) {
            // Decorate outgoing exceptions with basic tree information. This is similar to how the constructor
            // appends its information, but the constructor has read more information at that point so this one
            // is a bit more sparse on information.
            t.addSuppressed(new Exception(format("GBPTree[file:%s]", indexFile)));
            throw t;
        }
    }

    private static TreeState readHeaderFromPagedFiled(
            PagedFile pagedFile, Reader headerReader, CursorContext cursorContext, ImmutableSet<OpenOption> openOptions)
            throws IOException {
        Pair<TreeState, TreeState> states = loadStatePages(pagedFile, cursorContext);
        TreeState state = TreeStatePair.selectNewestValidState(states);
        try (PageCursor cursor = pagedFile.io(state.pageId(), PF_SHARED_READ_LOCK, cursorContext)) {
            PageCursorUtil.goTo(cursor, "header data", state.pageId());
            doReadHeader(headerReader, cursor, getEndianness(openOptions));
        }
        return state;
    }

    private static void doReadHeader(Header.Reader headerReader, PageCursor cursor, ByteOrder order)
            throws IOException {
        int headerDataLength;
        do {
            TreeState.read(cursor);
            headerDataLength = cursor.getInt();
        } while (cursor.shouldRetry());

        int headerDataOffset = cursor.getOffset();
        byte[] headerDataBytes = new byte[headerDataLength];
        do {
            cursor.setOffset(headerDataOffset);
            cursor.getBytes(headerDataBytes);
        } while (cursor.shouldRetry());

        headerReader.read(ByteBuffer.wrap(headerDataBytes).order(order));
    }

    private void writeState(PagedFile pagedFile, Header.Writer headerWriter, CursorContext cursorContext)
            throws IOException {
        Pair<TreeState, TreeState> states = readStatePages(pagedFile, cursorContext);
        TreeState oldestState = TreeStatePair.selectOldestOrInvalid(states);
        long pageToOverwrite = oldestState.pageId();
        Root root = rootLayer.getRoot(cursorContext);
        try (PageCursor cursor = pagedFile.io(pageToOverwrite, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext)) {
            PageCursorUtil.goTo(cursor, "state page", pageToOverwrite);
            FreeListIdProvider.FreelistMetaData freelistMetaData = freeList.metaData();
            TreeState.write(
                    cursor,
                    stableGeneration(generation),
                    unstableGeneration(generation),
                    root.id(),
                    root.generation(),
                    freelistMetaData.lastId(),
                    freelistMetaData.writePageId(),
                    freelistMetaData.readPageId(),
                    freelistMetaData.writePos(),
                    freelistMetaData.readPos(),
                    clean);

            writerHeader(pagedFile, headerWriter, other(states, oldestState), cursor, cursorContext);

            checkOutOfBounds(cursor);
        }
    }

    private static void writerHeader(
            PagedFile pagedFile,
            Header.Writer headerWriter,
            TreeState otherState,
            PageCursor cursor,
            CursorContext cursorContext)
            throws IOException {
        // Write/carry over header
        int headerOffset = cursor.getOffset();
        int headerDataOffset = getHeaderDataOffset(headerOffset);
        if (otherState.isValid() || headerWriter != CARRY_OVER_PREVIOUS_HEADER) {
            try (PageCursor previousCursor = pagedFile.io(otherState.pageId(), PF_SHARED_READ_LOCK, cursorContext)) {
                PageCursorUtil.goTo(previousCursor, "previous state page", otherState.pageId());
                checkOutOfBounds(cursor);
                do {
                    // Clear any out-of-bounds from prior attempts
                    cursor.checkAndClearBoundsFlag();
                    // Place the previous state cursor after state data
                    TreeState.read(previousCursor);
                    // Read length of previous header
                    int previousLength = previousCursor.getInt();
                    // Reserve space to store length
                    cursor.setOffset(headerDataOffset);
                    // Write
                    headerWriter.write(previousCursor, previousLength, cursor);
                } while (previousCursor.shouldRetry());
                checkOutOfBounds(previousCursor);
            }
            checkOutOfBounds(cursor);

            int length = cursor.getOffset() - headerDataOffset;
            cursor.putInt(headerOffset, length);
        }
    }

    @VisibleForTesting
    public static void overwriteHeader(
            PageCache pageCache,
            Path indexFile,
            Consumer<PageCursor> headerWriter,
            String databaseName,
            CursorContext cursorContext,
            ImmutableSet<OpenOption> openOptions)
            throws IOException {
        Header.Writer writer = replace(headerWriter);
        try (PagedFile pagedFile =
                openExistingIndexFile(pageCache, indexFile, cursorContext, databaseName, openOptions)) {
            Pair<TreeState, TreeState> states = readStatePages(pagedFile, cursorContext);
            TreeState newestValidState = TreeStatePair.selectNewestValidState(states);
            long pageToOverwrite = newestValidState.pageId();
            try (PageCursor cursor = pagedFile.io(pageToOverwrite, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext)) {
                PageCursorUtil.goTo(cursor, "state page", pageToOverwrite);

                // Place cursor after state data
                cursor.setOffset(TreeState.SIZE);

                // Note offset to header
                int headerOffset = cursor.getOffset();
                int headerDataOffset = getHeaderDataOffset(headerOffset);

                // Reserve space to store length
                cursor.setOffset(headerDataOffset);
                // Write data
                writer.write(null, 0, cursor);
                // Write length
                int length = cursor.getOffset() - headerDataOffset;
                cursor.putInt(headerOffset, length);
                checkOutOfBounds(cursor);
            }
        }
    }

    private static int getHeaderDataOffset(int headerOffset) {
        // Int reserved to store length of header
        return headerOffset + Integer.BYTES;
    }

    private static TreeState other(Pair<TreeState, TreeState> states, TreeState state) {
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
    private static Pair<TreeState, TreeState> loadStatePages(PagedFile pagedFile, CursorContext cursorContext)
            throws MetadataMismatchException, IOException {
        try {
            Pair<TreeState, TreeState> states = readStatePages(pagedFile, cursorContext);
            if (states.getLeft().isEmpty() && states.getRight().isEmpty()) {
                throw new MetadataMismatchException("Index is not fully initialized since its state pages are empty");
            }
            return states;
        } catch (IllegalStateException e) {
            throw new MetadataMismatchException("Index is not fully initialized since it's missing state pages", e);
        }
    }

    private static Pair<TreeState, TreeState> readStatePages(PagedFile pagedFile, CursorContext cursorContext)
            throws IOException {
        Pair<TreeState, TreeState> states;
        try (PageCursor cursor = pagedFile.io(0L /*ignored*/, PF_SHARED_READ_LOCK, cursorContext)) {
            states = TreeStatePair.readStatePages(cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B);
        }
        return states;
    }

    public void create(ROOT_KEY dataRootKey, CursorContext cursorContext) throws IOException {
        rootLayer.create(dataRootKey, cursorContext);
    }

    public void delete(ROOT_KEY dataRootKey, CursorContext cursorContext) throws IOException {
        rootLayer.delete(dataRootKey, cursorContext);
    }

    public DataTree<KEY, VALUE> access(ROOT_KEY dataRootKey) {
        return rootLayer.access(dataRootKey);
    }

    /**
     * Checkpoints and flushes any pending changes to storage. After a successful call to this method
     * the data is durable and safe. {@link DataTree#writer(int, CursorContext)} Changes} made after this call and until crashing or
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
    public void checkpoint(Consumer<PageCursor> headerWriter, FileFlushEvent flushEvent, CursorContext cursorContext) {
        try {
            checkpoint(replace(headerWriter), flushEvent, cursorContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Performs a {@link #checkpoint( Consumer, FileFlushEvent, CursorContext)}  check point}, keeping any header information
     * written in previous check point.
     *
     * @param cursorContext underlying page cursor context
     * @throws UncheckedIOException on error flushing to storage.
     * @see #checkpoint( Header.Writer, FileFlushEvent, CursorContext)
     */
    public void checkpoint(FileFlushEvent flushEvent, CursorContext cursorContext) {
        try {
            checkpoint(CARRY_OVER_PREVIOUS_HEADER, flushEvent, cursorContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Locking and interaction with writers during checkpoint:
     *
     * <pre>
     * |-WW--W-WWWW│-│WFWWFFFWFWWFWWWF│-│WWW-W--WW-WWW--| time
     *             └┬┴────────┬───────┴┬┘
     *              │         │        └ #3 block and drain writers, bump generation, force, unblock writers
     *              │         └ #2 flush all file pages cooperatively with writers
     *              └ #1 block and drain writers (and block new ones), set writers-must-flush flag, unblock writers
     * </pre>
     *
     * The two sections where writers are blocked are both very short:
     * <ul>
     *     <li>#1 flips a boolean</li>
     *     <li>#3 writes and flushes a maximum of 3-or-so pages (one state page and potentially two freelist pages</li>
     * </ul>
     * During #2 (when the file is flushed) writers are unblocked and will eagerly flush their changes as they write.
     */
    private synchronized void checkpoint(
            Header.Writer headerWriter, FileFlushEvent flushEvent, CursorContext cursorContext) throws IOException {
        if (readOnly) {
            return;
        }

        awaitCleaner();
        try {
            // Drain writers and flip state so that writers after this point on will do co-operative flushing
            // together with the file-global flush. This way writers can still progress through the flush,
            // w/o blocking on its completion.
            withCheckpointAndWriterLock(() -> writersMustEagerlyFlush = true);

            // Do the file-global flush together with the flushing by any potential concurrent writer
            monitor.checkpointStarted();
            pagedFile.flushAndForce(flushEvent);

            // Drain writers, bump generation and do the final forcing of the file.
            // New writers after this section don't need to eagerly flush anymore.
            withCheckpointAndWriterLock(() -> {
                // Eager flush is optimistic and may fail. There is a chance a few pages are still dirty at this point.
                writersMustEagerlyFlush = false;
                long generation = this.generation;
                long stableGeneration = stableGeneration(generation);
                long unstableGeneration = unstableGeneration(generation);
                freeList.flush(
                        stableGeneration, unstableGeneration, bind(pagedFile, PF_SHARED_WRITE_LOCK, cursorContext));

                // Force any potential pages flushed from writers after completion of the above flushAndForce
                // so that there's no chance that the state page change below can make it to disk before
                // the state page. Only force is needed, but there's no method only doing force, although
                // the flush part is really fast if there are no dirty pages.
                structureWriteLog.checkpoint(stableGeneration, unstableGeneration, unstableGeneration + 1);
                pagedFile.flushAndForce(flushEvent);

                // Increment generation, i.e. stable becomes current unstable and unstable increments by one
                // and write the tree state (rootId, lastId, generation a.s.o.) to state page.
                this.generation = Generation.generation(unstableGeneration, unstableGeneration + 1);
                writeState(pagedFile, headerWriter, cursorContext);
                pagedFile.flushAndForce(flushEvent);

                monitor.checkpointCompleted();
                changesSinceLastCheckpoint.set(false);
            });
        } finally {
            // Safeguard, let's never leave this method with eager flushing for writers enabled.
            writersMustEagerlyFlush = false;
        }
    }

    /**
     * Closes this tree and its associated resources.
     * <p>
     * NOTE: No {@link #checkpoint(FileFlushEvent flushEvent, CursorContext)} checkpoint} is performed.
     * @throws IOException on error closing resources.
     */
    @Override
    public synchronized void close() throws IOException {
        try (var cursorContext = contextFactory.create(INDEX_INTERNAL_TAG)) {
            if (openOptions.contains(NO_FLUSH_ON_CLOSE) || readOnly) {
                // Close without forcing state
                doClose();
                return;
            }
            withCheckpointAndWriterLock(() -> {
                try {
                    if (closed) {
                        return;
                    }
                    try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                        maybeForceCleanState(flushEvent, cursorContext);
                    }
                    doClose();
                } catch (IOException ioe) {
                    try {
                        if (!pagedFile.isDeleteOnClose()) {
                            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                                pagedFile.flushAndForce(flushEvent);
                            }
                        }
                        try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                            maybeForceCleanState(flushEvent, cursorContext);
                        }
                        doClose();
                    } catch (IOException e) {
                        ioe.addSuppressed(e);
                        throw ioe;
                    }
                }
            });
        }
    }

    private void withCheckpointAndWriterLock(ThrowingAction<IOException> task) throws IOException {
        checkpointLock.writeLock().lock();
        writerLock.writeLock().lock();
        try {
            task.apply();
        } finally {
            writerLock.writeLock().unlock();
            checkpointLock.writeLock().unlock();
        }
    }

    protected void awaitCleaner() throws IOException {
        if (cleanerLock != null) {
            try {
                cleanerLock.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Got interrupted while awaiting the cleaner lock, cannot continue execution beyond this point");
            }
        }
        if (cleaning != null && cleaning.hasFailed()) {
            throw new IOException("Pointer cleaning during recovery failed", cleaning.getCause());
        }
    }

    public void setDeleteOnClose(boolean deleteOnClose) {
        pagedFile.setDeleteOnClose(deleteOnClose);
    }

    private void maybeForceCleanState(FileFlushEvent flushEvent, CursorContext cursorContext) throws IOException {
        if (cleaning != null && !changesSinceLastCheckpoint.get() && !cleaning.needed()) {
            clean = true;
            if (!pagedFile.isDeleteOnClose()) {
                forceState(flushEvent, cursorContext);
            }
        }
    }

    private void doClose() {
        try (pagedFile;
                structureWriteLog) {}
        closed = true;
    }

    /**
     * Bump unstable generation, increasing the gap between stable and unstable generation. All pointers and tree nodes
     * with generation in this gap are considered to be 'crashed' and will be cleaned up by {@link CleanupJob}
     * created in {@link #createCleanupJob(RecoveryCleanupWorkCollector, boolean)}.
     */
    private void bumpUnstableGeneration() {
        generation = generation(stableGeneration(generation), unstableGeneration(generation) + 1);
    }

    private void forceState(FileFlushEvent flushEvent, CursorContext cursorContext) throws IOException {
        if (changesSinceLastCheckpoint.get()) {
            throw new IllegalStateException("It seems that this method has been called in the wrong state. "
                    + "It's expected that this is called after opening this tree, but before any changes "
                    + "have been made");
        }

        writeState(pagedFile, CARRY_OVER_PREVIOUS_HEADER, cursorContext);
        pagedFile.flushAndForce(flushEvent);
    }

    /**
     * Called on start if tree was not clean.
     */
    private CleanupJob createCleanupJob(
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean needsCleaning) {
        if (!needsCleaning) {
            return CleanupJob.CLEAN;
        } else {
            cleanerLock = new CountDownLatch(1);
            monitor.cleanupRegistered();

            CrashGenerationCleaner crashGenerationCleaner = rootLayer.createCrashGenerationCleaner(contextFactory);
            GBPTreeCleanupJob cleanupJob =
                    new GBPTreeCleanupJob(crashGenerationCleaner, cleanerLock, monitor, indexFile);
            recoveryCleanupWorkCollector.add(cleanupJob);
            return cleanupJob;
        }
    }

    public boolean consistencyCheck(
            ReporterFactory reporterFactory,
            CursorContextFactory contextFactory,
            int numThreads,
            ProgressMonitorFactory progressMonitorFactory) {
        return consistencyCheck(
                reporterFactory.getClass(GBPTreeConsistencyCheckVisitor.class),
                true,
                contextFactory,
                numThreads,
                progressMonitorFactory);
    }

    // Utility method
    public boolean consistencyCheck(
            GBPTreeConsistencyCheckVisitor visitor,
            boolean reportDirty,
            CursorContextFactory contextFactory,
            int numThreads,
            ProgressMonitorFactory progressMonitorFactory) {
        CleanTrackingConsistencyCheckVisitor cleanTrackingVisitor = new CleanTrackingConsistencyCheckVisitor(visitor);
        try (var context = contextFactory.create("consistencyCheck");
                var state = new ConsistencyCheckState(
                        indexFile,
                        freeList,
                        visitor,
                        bind(pagedFile, PF_SHARED_READ_LOCK, context),
                        numThreads,
                        progressMonitorFactory)) {
            if (dirtyOnStartup && reportDirty) {
                cleanTrackingVisitor.dirtyOnStartup(indexFile);
            }
            rootLayer.consistencyCheck(state, cleanTrackingVisitor, reportDirty, contextFactory, numThreads);
        } catch (TreeInconsistencyException | MetadataMismatchException | CursorException e) {
            cleanTrackingVisitor.exception(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return cleanTrackingVisitor.isConsistent();
    }

    @VisibleForTesting
    public <K, V> void unsafe(GBPTreeUnsafe<K, V> unsafe, CursorContext cursorContext) throws IOException {
        unsafe(unsafe, true, cursorContext);
    }

    @VisibleForTesting
    public <K, V> void unsafe(GBPTreeUnsafe<K, V> unsafe, boolean dataTree, CursorContext cursorContext)
            throws IOException {
        rootLayer.unsafe(unsafe, dataTree, cursorContext);
    }

    @VisibleForTesting
    public int leafMaxKeyCount() {
        return rootLayer.leafNodeMaxKeys();
    }

    @Override
    public String toString() {
        long generation = this.generation;
        return format(
                "GB+Tree[file:%s, layout:%s, generation:%d/%d]",
                indexFile.toAbsolutePath(), layout, stableGeneration(generation), unstableGeneration(generation));
    }

    private <E extends Throwable> void appendTreeInformation(E e) {
        e.addSuppressed(new Exception(e.getMessage() + " | " + this));
    }

    /**
     * Total size limit for key and value.
     * This limit includes storage overhead that is specific to key implementation for example entity id or meta data about type.
     * @return Total size limit for key and value or {@link TreeNodeUtil#NO_KEY_VALUE_SIZE_CAP} if no such value exists.
     */
    public int keyValueSizeCap() {
        return rootLayer.keyValueSizeCap();
    }

    @VisibleForTesting
    int inlineKeyValueSizeCap() {
        return rootLayer.inlineKeyValueSizeCap();
    }

    /**
     * @return size of the file backing this {@link GBPTree}, in bytes.
     */
    public long sizeInBytes() {
        try {
            return pagedFile.fileSize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void visitAllRoots(CursorContext cursorContext, TreeRootsVisitor<ROOT_KEY> visitor) throws IOException {
        rootLayer.visitAllDataTreeRoots(cursorContext, visitor);
    }

    @VisibleForTesting
    public <VISITOR extends GBPTreeVisitor<ROOT_KEY, KEY, VALUE>> VISITOR visit(
            VISITOR visitor, CursorContext cursorContext) throws IOException {
        rootLayer.visit(visitor, cursorContext);
        return visitor;
    }

    @SuppressWarnings("unused")
    public void printTree(CursorContext cursorContext) throws IOException {
        printTree(PrintConfig.defaults(), cursorContext);
    }

    // Utility method
    /**
     * Prints the contents of the tree to System.out.
     *
     * @param printConfig {@link PrintConfig} containing configurations for this printing.
     * @throws IOException on I/O error.
     */
    @SuppressWarnings("SameParameterValue")
    public void printTree(PrintConfig printConfig, CursorContext cursorContext) throws IOException {
        visit(new PrintingGBPTreeVisitor<>(printConfig), cursorContext);
    }

    // Utility method
    public void printState(CursorContext cursorContext) throws IOException {
        try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
            GBPTreeStructure.visitTreeState(
                    cursor, new PrintingGBPTreeVisitor<>(PrintConfig.defaults().printState()));
        }
    }

    // Utility method
    /**
     * Print node with given id to System.out, if node with id exists.
     * @param id the page id of node to print
     */
    void printNode(long id, CursorContext cursorContext) throws IOException {
        if (id <= freeList.lastId()) {
            try (PageCursor cursor = pagedFile.io(id, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext)) {
                cursor.next();
                byte nodeType = TreeNodeUtil.nodeType(cursor);
                if (nodeType == TreeNodeUtil.NODE_TYPE_TREE_NODE) {
                    rootLayer.printNode(cursor, cursorContext);
                }
            }
        }
    }

    private static ImmutableSet<OpenOption> treeOpenOptions(ImmutableSet<OpenOption> openOptions) {
        return openOptions.newWithout(MULTI_VERSIONED);
    }

    protected static <KEY, VALUE> OffloadStoreImpl<KEY, VALUE> buildOffload(
            Layout<KEY, VALUE> layout, IdProvider idProvider, PagedFile pagedFile, int pageSize) {
        OffloadIdValidator idValidator = id -> id >= IdSpace.MIN_TREE_NODE_ID && id <= pagedFile.getLastPageId();
        return new OffloadStoreImpl<>(layout, idProvider, pagedFile::io, idValidator, pageSize);
    }

    private static void verifyPayloadSize(PagedFile pagedFile, CursorContext cursorContext) throws IOException {
        if (pagedFile.getLastPageId() >= IdSpace.META_PAGE_ID) {
            var metaPayloadSize =
                    RootLayerSupport.readMeta(pagedFile, cursorContext).getPayloadSize();
            if (metaPayloadSize != 0 && metaPayloadSize != pagedFile.payloadSize()) {
                throw new MetadataMismatchException(format(
                        "Tried to open the tree using page payload size %d, but the tree was original created with page payload size %d so cannot be opened.",
                        pagedFile.payloadSize(), metaPayloadSize));
            }
        }
    }

    private record OpenResult(PagedFile pagedFile, boolean created) {}
}
