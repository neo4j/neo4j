/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.id.indexed;

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeConsistencyCheckVisitor;
import org.neo4j.index.internal.gbptree.GBPTreeVisitor;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.internal.id.FreeIds;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

/**
 * At the heart of this free-list sits a {@link GBPTree}, containing all deleted and freed ids. The tree is used as a bit-set and since it's
 * sorted then it can be later extended to allocate multiple consecutive ids. Another design feature of this free-list is that it's crash-safe,
 * that is if the {@link #marker(PageCursorTracer)} is only used for applying committed data.
 */
public class IndexedIdGenerator implements IdGenerator
{
    public interface Monitor extends AutoCloseable
    {
        void opened( long highestWrittenId, long highId );

        @Override
        void close();

        void allocatedFromHigh( long allocatedId );

        void allocatedFromReused( long allocatedId );

        void cached( long cachedId );

        void markedAsUsed( long markedId );

        void markedAsDeleted( long markedId );

        void markedAsFree( long markedId );

        void markedAsReserved( long markedId );

        void markedAsUnreserved( long markedId );

        void markedAsDeletedAndFree( long markedId );

        void markSessionDone();

        void normalized( long idRange );

        void bridged( long bridgedId );

        void checkpoint( long highestWrittenId, long highId );

        void clearingCache();

        void clearedCache();

        class Adapter implements Monitor
        {
            @Override
            public void opened( long highestWrittenId, long highId )
            {
            }

            @Override
            public void allocatedFromHigh( long allocatedId )
            {
            }

            @Override
            public void allocatedFromReused( long allocatedId )
            {
            }

            @Override
            public void cached( long cachedId )
            {
            }

            @Override
            public void markedAsUsed( long markedId )
            {
            }

            @Override
            public void markedAsDeleted( long markedId )
            {
            }

            @Override
            public void markedAsFree( long markedId )
            {
            }

            @Override
            public void markedAsReserved( long markedId )
            {
            }

            @Override
            public void markedAsUnreserved( long markedId )
            {
            }

            @Override
            public void markedAsDeletedAndFree( long markedId )
            {
            }

            @Override
            public void markSessionDone()
            {
            }

            @Override
            public void normalized( long idRange )
            {
            }

            @Override
            public void bridged( long bridgedId )
            {
            }

            @Override
            public void checkpoint( long highestWrittenId, long highId )
            {
            }

            @Override
            public void clearingCache()
            {
            }

            @Override
            public void clearedCache()
            {
            }

            @Override
            public void close()
            {
            }
        }
    }

    public static final Monitor NO_MONITOR = new Monitor.Adapter();

    /**
     * Represents the absence of an id in the id cache.
     */
    static final long NO_ID = -1;

    /**
     * Number of ids per entry in the GBPTree.
     */
    static final int IDS_PER_ENTRY = 128;

    /**
     * Used for id generators that generally has low activity.
     * 2^8 == 256 and one ID takes up 8B, which results in a memory usage of 256 * 8 = ~2k memory
     */
    private static final int SMALL_CACHE_CAPACITY = 1 << 8;

    /**
     * Used for id generators that generally has high activity.
     * 2^14 == 16384 and one ID takes up 8B, which results in a memory usage of 16384 * 8 = ~131k memory
     */
    private static final int LARGE_CACHE_CAPACITY = 1 << 14;

    /**
     * First generation the tree entries will start at. Generation will be incremented each time an IndexedIdGenerator is opened,
     * i.e. not for every checkpoint. Generation is used to do lazy normalization of id states, so that DELETED ids from a previous generation
     * looks like FREE in the current session. Updates to tree items (except for recovery) will reset the generation that of the current session.
     */
    private static final long STARTING_GENERATION = 1;

    /**
     * {@link GBPTree} for storing and accessing the id states.
     */
    private final GBPTree<IdRangeKey,IdRange> tree;

    /**
     * Cache of free ids to be handed out from {@link #nextId(PageCursorTracer)}. Populated by {@link FreeIdScanner}.
     */
    private final ConcurrentLongQueue cache;

    /**
     * {@link IdType} that this id generator covers.
     */
    private final IdType idType;

    /**
     * Number of ids per {@link IdRange} in the {@link GBPTree}.
     */
    private final int idsPerEntry;

    /**
     * Cache low-watermark when to trigger {@link FreeIdScanner} for refill.
     */
    private final int cacheOptimisticRefillThreshold;

    /**
     * Note about contention: Calls to commitMarker() should be worksync'ed externally and will therefore not contend.
     * This lock is about guarding for calls to reuseMarker(), which comes in at arbitrary times outside transactions.
     */
    private final Lock commitAndReuseLock = new ReentrantLock();

    /**
     * {@link GBPTree} {@link Layout} for this id generator.
     */
    private final IdRangeLayout layout;

    /**
     * Scans the stored ids and places into cache for quick access in {@link #nextId(PageCursorTracer)}.
     */
    private final FreeIdScanner scanner;

    /**
     * High id of this id generator (and to some extent the store this covers).
     */
    private final AtomicLong highId = new AtomicLong();

    /**
     * Maximum id that this id generator can allocate.
     */
    private final long maxId;

    /**
     * Means of communicating whether or not there are stored free ids that {@link FreeIdScanner} could pick up. Is also cleared by
     * {@link FreeIdScanner} as soon as it notices that it has run out of stored free ids.
     */
    private final AtomicBoolean atLeastOneIdOnFreelist = new AtomicBoolean();

    /**
     * Current generation of this id generator. Generation is used to normalize id states so that a deleted id of a previous generation
     * can be seen as free in the current generation. Generation is bumped on restart.
     */
    private final long generation;

    /**
     * Internal state kept between constructor and {@link #start(FreeIds, PageCursorTracer)},
     * whether or not to rebuild the id generator from the supplied {@link FreeIds}.
     */
    private final boolean needsRebuild;

    /**
     * Highest ever written id in this id generator. This is used to not lose track of ids allocated off of high id that are not committed.
     * See more in {@link IdRangeMarker}.
     */
    private final AtomicLong highestWrittenId = new AtomicLong();
    private final Path path;
    private final boolean readOnly;

    /**
     * {@code false} after construction and before a call to {@link IdGenerator#start(FreeIds, PageCursorTracer)},
     * where false means that operations made this freelist
     * is to be treated as recovery operations. After a call to {@link IdGenerator#start(FreeIds, PageCursorTracer)}
     * the operations are to be treated as normal operations.
     */
    private volatile boolean started;

    private final IdRangeMerger defaultMerger;
    private final IdRangeMerger recoveryMerger;

    private final Monitor monitor;

    public IndexedIdGenerator( PageCache pageCache, Path path, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, IdType idType,
            boolean allowLargeIdCaches, LongSupplier initialHighId, long maxId, boolean readOnly, Config config, PageCursorTracer cursorTracer )
    {
        this( pageCache, path, recoveryCleanupWorkCollector, idType, allowLargeIdCaches, initialHighId, maxId, readOnly, config, cursorTracer,
                immutable.empty() );
    }

    public IndexedIdGenerator( PageCache pageCache, Path path, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, IdType idType,
            boolean allowLargeIdCaches, LongSupplier initialHighId, long maxId, boolean readOnly, Config config, PageCursorTracer cursorTracer,
            ImmutableSet<OpenOption> openOptions )
    {
        this( pageCache, path, recoveryCleanupWorkCollector, idType, allowLargeIdCaches, initialHighId, maxId, readOnly, config, cursorTracer, NO_MONITOR,
                openOptions );
    }

    public IndexedIdGenerator( PageCache pageCache, Path path, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, IdType idType,
            boolean allowLargeIdCaches, LongSupplier initialHighId, long maxId, boolean readOnly, Config config, PageCursorTracer cursorTracer, Monitor monitor,
            ImmutableSet<OpenOption> openOptions )
    {
        this.path = path;
        this.readOnly = readOnly;
        int cacheCapacity = idType.highActivity() && allowLargeIdCaches ? LARGE_CACHE_CAPACITY : SMALL_CACHE_CAPACITY;
        this.idType = idType;
        this.cacheOptimisticRefillThreshold = cacheCapacity / 4;
        this.cache = new SpmcLongQueue( cacheCapacity );
        this.maxId = maxId;
        this.monitor = monitor;
        this.defaultMerger = new IdRangeMerger( false, monitor );
        this.recoveryMerger = new IdRangeMerger( true, monitor );

        Optional<HeaderReader> header = readHeader( pageCache, path, cursorTracer );
        // We check generation here too since we could get into this scenario:
        // 1. start on existing store, but with missing .id file so that it gets created
        // 2. rebuild will happen in start(), but perhaps the db was shut down or killed before or during start()
        // 3. next startup would have said that it wouldn't need rebuild
        this.needsRebuild = header.isEmpty() || header.get().generation == STARTING_GENERATION;
        if ( !needsRebuild )
        {
            // This id generator exists, use the values from its header
            this.highId.set( header.get().highId );
            this.highestWrittenId.set( header.get().highestWrittenId );
            this.generation = header.get().generation + 1;
            this.idsPerEntry = header.get().idsPerEntry;
            // Let's optimistically think assume that there may be some free ids in here. This will ensure that a scan is triggered on first request
            this.atLeastOneIdOnFreelist.set( true );
        }
        else
        {
            // We'll create this index when constructing the GBPTree below. The generation on its creation will be STARTING_GENERATION,
            // as written by the HeaderWriter, but the active generation has to be +1 that
            this.highId.set( initialHighId.getAsLong() );
            this.highestWrittenId.set( highId.get() - 1 );
            this.generation = STARTING_GENERATION + 1;
            this.idsPerEntry = IDS_PER_ENTRY;
        }
        monitor.opened( highestWrittenId.get(), highId.get() );

        this.layout = new IdRangeLayout( idsPerEntry );
        this.tree = instantiateTree( pageCache, path, recoveryCleanupWorkCollector, readOnly, openOptions );

        boolean strictlyPrioritizeFreelist = config.get( GraphDatabaseInternalSettings.strictly_prioritize_id_freelist );
        this.scanner = readOnly ? null : new FreeIdScanner( idsPerEntry, tree, cache, atLeastOneIdOnFreelist,
                tracer -> lockAndInstantiateMarker( true, tracer ), generation, strictlyPrioritizeFreelist, monitor );
    }

    private GBPTree<IdRangeKey,IdRange> instantiateTree( PageCache pageCache, Path path, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            boolean readOnly, ImmutableSet<OpenOption> openOptions )
    {
        try
        {
            final HeaderWriter headerWriter = new HeaderWriter( highId::get, highestWrittenId::get, STARTING_GENERATION, idsPerEntry );
            return new GBPTree<>( pageCache, path, layout, GBPTree.NO_MONITOR, NO_HEADER_READER, headerWriter, recoveryCleanupWorkCollector,
                    readOnly, NULL, openOptions, "Indexed ID generator" );
        }
        catch ( TreeFileNotFoundException e )
        {
            throw new IllegalStateException(
                    "Id generator file could not be found, most likely this database needs to be recovered, file:" + path, e );
        }
    }

    @Override
    public long nextId( PageCursorTracer cursorTracer )
    {
        assertNotReadOnly();
        // To try and minimize the gap where the cache is empty and scanner is trying to find more to put in the cache
        // we can see if the cache is starting to dry out and if so do a scan right here.
        // There may be multiple allocation requests doing this, but it should be very cheap:
        // comparing two ints, reading an AtomicBoolean and trying to CAS an AtomicBoolean.
        maintenance( false, cursorTracer );

        // try get from cache
        long id = cache.takeOrDefault( NO_ID );
        if ( id != NO_ID )
        {
            // We got an ID from the cache, all good
            monitor.allocatedFromReused( id );
            return id;
        }

        // There was no ID in the cache. This could be that either there are no free IDs in here (the typical case), or a benign
        // race where the cache ran out of IDs and it's very soon filled with more IDs from an ongoing scan. We have made the decision
        // to prioritise performance and so we don't just sit here waiting for an ongoing scan to find IDs (fast as it may be, although it can be I/O bound)
        // so we allocate from highId instead. This make highId slide a little even if there actually are free ids available,
        // but this should be a fairly rare event.
        do
        {
            id = highId.getAndIncrement();
            IdValidator.assertIdWithinMaxCapacity( idType, id, maxId );
        }
        while ( IdValidator.isReservedId( id ) );
        monitor.allocatedFromHigh( id );
        return id;
    }

    @Override
    public org.neo4j.internal.id.IdRange nextIdBatch( int size, boolean forceConsecutiveAllocation, PageCursorTracer cursorTracer )
    {
        assertNotReadOnly();
        maintenance( false, cursorTracer );

        if ( forceConsecutiveAllocation )
        {
            long startId;
            do
            {
                startId = highId.getAndAdd( size );
            }
            while ( IdValidator.hasReservedIdInRange( startId, startId + size ) );
            return new org.neo4j.internal.id.IdRange( EMPTY_LONG_ARRAY, startId, size );
        }

        long prev = -1;
        long startOfRange = -1;
        int rangeLength = 0;
        MutableLongList other = null;
        for ( int i = 0; i < size; i++ )
        {
            long id = nextId( cursorTracer );
            if ( other != null )
            {
                other.add( id );
            }
            else
            {
                if ( i == 0 )
                {
                    prev = id;
                    startOfRange = id;
                    rangeLength = 1;
                }
                else
                {
                    if ( id == prev + 1 )
                    {
                        prev = id;
                        rangeLength++;
                    }
                    else
                    {
                        other = LongLists.mutable.empty();
                        other.add( id );
                    }
                }
            }
        }
        return new org.neo4j.internal.id.IdRange( other != null ? other.toArray() : EMPTY_LONG_ARRAY, startOfRange, rangeLength );
    }

    @Override
    public Marker marker( PageCursorTracer cursorTracer )
    {
        if ( !started && needsRebuild )
        {
            // If we're in recovery and know that we're building the id generator from scratch after recovery has completed then don't make any updates
            return NOOP_MARKER;
        }

        return lockAndInstantiateMarker( true, cursorTracer );
    }

    IdRangeMarker lockAndInstantiateMarker( boolean bridgeIdGaps, PageCursorTracer cursorTracer )
    {
        assertNotReadOnly();
        commitAndReuseLock.lock();
        try
        {
            return new IdRangeMarker( idsPerEntry, layout, tree.writer( cursorTracer ), commitAndReuseLock,
                    started ? defaultMerger : recoveryMerger,
                    started, atLeastOneIdOnFreelist, generation, highestWrittenId, bridgeIdGaps, monitor );
        }
        catch ( Exception e )
        {
            commitAndReuseLock.unlock();
            throw new RuntimeException( e );
        }
    }

    @Override
    public void close()
    {
        closeAllUnchecked( scanner, tree, monitor );
    }

    @Override
    public long getHighId()
    {
        return highId.get();
    }

    @Override
    public void setHighId( long newHighId )
    {
        assertNotReadOnly();
        // Apparently there's this thing where there's a check that highId is only set if it's higher than the current highId,
        // i.e. highId cannot be set to something lower than it already is. This check is done in the store implementation.
        // But can we rely on it always guarding this, and can this even happen at all? Anyway here's a simple guard for not setting it to something lower.
        long expect;
        do
        {
            expect = highId.get();
        }
        while ( newHighId > expect && !highId.compareAndSet( expect, newHighId ) );
    }

    @Override
    public void start( FreeIds freeIdsForRebuild, PageCursorTracer cursorTracer ) throws IOException
    {
        if ( needsRebuild )
        {
            assertNotReadOnly();
            // This id generator was created right now, it needs to be populated with all free ids from its owning store so that it's in sync
            try ( IdRangeMarker idRangeMarker = lockAndInstantiateMarker( false, cursorTracer ) )
            {
                // We can mark the ids as free right away since this is before started which means we get the very liberal merger
                long highestId = freeIdsForRebuild.accept( id ->
                {
                    idRangeMarker.markDeleted( id );
                    idRangeMarker.markFree( id );
                } );
                highId.set( highestId + 1 );
                highestWrittenId.set( highestId );
            }
            // We can checkpoint here since the free ids we read are committed
            checkpoint( IOLimiter.UNLIMITED, cursorTracer );
            atLeastOneIdOnFreelist.set( true );
        }

        started = true;

        // After potentially recovery has been run and everything is prepared to get going let's call maintenance,
        // which will fill the ID buffers right away before any request comes to the db.
        maintenance( false, cursorTracer );
    }

    @Override
    public void checkpoint( IOLimiter ioLimiter, PageCursorTracer cursorTracer )
    {
        tree.checkpoint( ioLimiter, new HeaderWriter( highId::get, highestWrittenId::get, generation, idsPerEntry ), cursorTracer );
        monitor.checkpoint( highestWrittenId.get(), highId.get() );
    }

    @Override
    public void maintenance( boolean awaitOngoing, PageCursorTracer cursorTracer )
    {
        if ( !readOnly && cache.size() < cacheOptimisticRefillThreshold )
        {
            // We're just helping other allocation requests and avoiding unwanted sliding of highId here
            scanner.tryLoadFreeIdsIntoCache( awaitOngoing, cursorTracer );
        }
    }

    @Override
    public void clearCache( PageCursorTracer cursorTracer )
    {
        if ( !readOnly )
        {
            // Make the scanner clear it because it needs to coordinate with the scan lock
            monitor.clearingCache();
            scanner.clearCache( cursorTracer );
            monitor.clearedCache();
        }
    }

    @Override
    public long getHighestPossibleIdInUse()
    {
        return getHighId() - 1;
    }

    @Override
    public long getNumberOfIdsInUse()
    {
        return getHighId();
    }

    @Override
    public long getDefragCount()
    {
        // This is only correct up to cache capacity, but this method only seems to be used in tests and those tests only
        // check whether or not this id generator have a small number of ids that it just freed.
        return cache.size();
    }

    /**
     * A peculiar being this one. It's for the import case where all records are written w/o even touching the id generator.
     * When all have been written the id generator is told that it should consider highest written where it's at right now
     * So that it won't mark all ids as deleted on the first write (the id bridging).
     */
    @Override
    public void markHighestWrittenAtHighId()
    {
        assertNotReadOnly();
        this.highestWrittenId.set( highId.get() - 1 );
    }

    @Override
    public long getHighestWritten()
    {
        return highestWrittenId.get();
    }

    public Path path()
    {
        return path;
    }

    /**
     * Reads contents of a header in an existing {@link IndexedIdGenerator}.
     *
     * @param pageCache {@link PageCache} to map id generator in.
     * @param path {@link Path} pointing to the id generator.
     * @return {@link Optional} with the data embedded inside the {@link HeaderReader} if the id generator existed and the header was read correctly,
     * otherwise {@link Optional#empty()}.
     */
    private static Optional<HeaderReader> readHeader( PageCache pageCache, Path path, PageCursorTracer cursorTracer )
    {
        try
        {
            HeaderReader headerReader = new HeaderReader();
            GBPTree.readHeader( pageCache, path, headerReader, cursorTracer );
            return Optional.of( headerReader );
        }
        catch ( NoSuchFileException e )
        {
            // That's OK, looks like we're creating this id generator for the first time
            return Optional.empty();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    /**
     * Dumps the contents of an {@link IndexedIdGenerator} as human-readable text.
     *
     * @param pageCache {@link PageCache} to map id generator in.
     * @param path {@link Path} pointing to the id generator.
     * @param cacheTracer underlying page cache tracer
     * @throws IOException if the file was missing or some other I/O error occurred.
     */
    public static void dump( PageCache pageCache, Path path, PageCacheTracer cacheTracer ) throws IOException
    {
        try ( var cursorTracer = cacheTracer.createPageCursorTracer( "IndexDump" ) )
        {
            HeaderReader header = readHeader( pageCache, path, cursorTracer ).orElseThrow( () -> new NoSuchFileException( path.toAbsolutePath().toString() ) );
            IdRangeLayout layout = new IdRangeLayout( header.idsPerEntry );
            try ( GBPTree<IdRangeKey,IdRange> tree = new GBPTree<>( pageCache, path, layout, GBPTree.NO_MONITOR, NO_HEADER_READER, NO_HEADER_WRITER,
                    immediate(), true, cacheTracer, immutable.empty(), "Indexed ID generator" ) )
            {
                tree.visit( new GBPTreeVisitor.Adaptor<>()
                {
                    private IdRangeKey key;

                    @Override
                    public void key( IdRangeKey key, boolean isLeaf, long offloadId )
                    {
                        this.key = key;
                    }

                    @Override
                    public void value( IdRange value )
                    {
                        System.out.println( format( "%s [%d]", value, key.getIdRangeIdx() ) );
                    }
                }, cursorTracer );
                System.out.println( header );
            }
        }
    }

    @Override
    public boolean consistencyCheck( ReporterFactory reporterFactory, PageCursorTracer cursorTracer )
    {
        return consistencyCheck( reporterFactory.getClass( GBPTreeConsistencyCheckVisitor.class ), cursorTracer );
    }

    private boolean consistencyCheck( GBPTreeConsistencyCheckVisitor<IdRangeKey> visitor, PageCursorTracer cursorTracer )
    {
        try
        {
            return tree.consistencyCheck( visitor, cursorTracer );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void assertNotReadOnly()
    {
        if ( readOnly )
        {
            throw new UnsupportedOperationException( "Can not write to id generator while in read only mode." );
        }
    }

    interface ReservedMarker extends AutoCloseable
    {
        void markReserved( long id );
        void markUnreserved( long id );
        @Override
        void close();
    }
}
