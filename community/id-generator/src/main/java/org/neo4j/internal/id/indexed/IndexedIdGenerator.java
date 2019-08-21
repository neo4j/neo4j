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
package org.neo4j.internal.id.indexed;

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeVisitor;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.FreeIds;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.util.FeatureToggles.flag;

/**
 * At the heart of this free-list sits a {@link GBPTree}, containing all deleted and freed ids. The tree is used as a bit-set and since it's
 * sorted then it can be later extended to allocate multiple consecutive ids. Another design feature of this free-list is that it's crash-safe,
 * that is if the {@link #commitMarker()} is only used for applying committed data.
 */
public class IndexedIdGenerator implements IdGenerator
{
    /**
     * Default value whether or not to strictly prioritize ids from freelist, as opposed to allocating from high id.
     * Given a scenario where there are multiple concurrent calls to {@link #nextId()} or {@link #nextIdBatch(int)} and there are
     * free ids on the freelist, some perhaps cached, some not. Thread noticing that there are no free ids cached will try to acquire scanner lock and if
     * it succeeds it will perform a scan and place found free ids in the cache and return. Otherwise:
     * <ul>
     *     <li>If {@code false}: thread will allocate from high id and return, to not block id allocation request.</li>
     *     <li>If {@code true}: thread will await lock released and check cache afterwards. If no id is cached even then it will allocate from high id.</li>
     * </ul>
     */
    private static final boolean STRICTLY_PRIORITIZE_FREELIST_DEFAULT = false;
    public static final String STRICTLY_PRIORITIZE_FREELIST_NAME = "strictlyPrioritizeFreelist";

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
     * 2^15 == 32768 and one ID takes up 8B, which results in a memory usage of 32768 * 8 = 262k memory
     */
    private static final int LARGE_CACHE_CAPACITY = 1 << 15;

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
     * Cache of free ids to be handed out from {@link #nextId()}. Populated by {@link FreeIdScanner}.
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
     * Scans the stored ids and places into cache for quick access in {@link #nextId()}.
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
     * Internal state kept between constructor and {@link #start(FreeIds)}, whether or not to rebuild the id generator from the supplied {@link FreeIds}.
     */
    private final boolean needsRebuild;

    /**
     * Highest ever written id in this id generator. This is used to not lose track of ids allocated off of high id that are not committed.
     * See more in {@link IdRangeMarker}.
     */
    private final AtomicLong highestWrittenId = new AtomicLong();

    /**
     * {@code false} after construction and before a call to {@link IdGenerator#start(FreeIds)}, where false means that operations made this freelist
     * is to be treated as recovery operations. After a call to {@link IdGenerator#start(FreeIds)} the operations are to be treated as normal operations.
     */
    private volatile boolean started;

    public IndexedIdGenerator( PageCache pageCache, File file, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, IdType idType,
            LongSupplier initialHighId, long maxId, OpenOption... openOptions )
    {
        this( pageCache, file, recoveryCleanupWorkCollector, idType, IDS_PER_ENTRY, initialHighId, maxId, openOptions );
    }

    @VisibleForTesting
    IndexedIdGenerator( PageCache pageCache, File file, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, IdType idType,
            int idsPerEntryOnCreate, LongSupplier initialHighId, long maxId, OpenOption... openOptions )
    {
        Preconditions.checkArgument( Integer.bitCount( idsPerEntryOnCreate ) == 1, "Requires idsPerEntry to be a power of 2, was %d", idsPerEntryOnCreate );
        int cacheCapacity = idType.highActivity() ? LARGE_CACHE_CAPACITY : SMALL_CACHE_CAPACITY;
        this.idType = idType;
        this.cacheOptimisticRefillThreshold = cacheCapacity / 4;
        this.cache = new SpmcLongQueue( cacheCapacity );
        this.maxId = maxId;

        Optional<HeaderReader> header = readHeader( pageCache, file );
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
            this.idsPerEntry = idsPerEntryOnCreate;
        }

        this.layout = new IdRangeLayout( idsPerEntry );
        this.tree = new GBPTree<>( pageCache, file, layout, 0, NO_MONITOR, NO_HEADER_READER,
                new HeaderWriter( highId::get, highestWrittenId::get, STARTING_GENERATION, idsPerEntry ), recoveryCleanupWorkCollector, openOptions );

        boolean strictlyPrioritizeFreelist = flag( IndexedIdGenerator.class, STRICTLY_PRIORITIZE_FREELIST_NAME, STRICTLY_PRIORITIZE_FREELIST_DEFAULT );
        this.scanner = new FreeIdScanner( idsPerEntry, tree, cache, atLeastOneIdOnFreelist, this::reuseMarker, generation, strictlyPrioritizeFreelist );
    }

    @Override
    public long nextId()
    {
        // To try and minimize the gap where the cache is empty and scanner is trying to find more to put in the cache
        // we can see if the cache is starting to dry out and if so do a scan right here.
        // There may be multiple allocation requests doing this, but it should be very cheap:
        // comparing two ints, reading an AtomicBoolean and trying to CAS an AtomicBoolean.
        maintenance();

        // try get from cache
        long id = cache.takeOrDefault( NO_ID );
        if ( id != NO_ID )
        {
            // We got an ID from the cache, all good
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
        return id;
    }

    @Override
    public org.neo4j.internal.id.IdRange nextIdBatch( int size )
    {
        long prev = -1;
        long startOfRange = -1;
        int rangeLength = 0;
        MutableLongList other = null;
        for ( int i = 0; i < size; i++ )
        {
            long id = nextId();
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
    public CommitMarker commitMarker()
    {
        if ( !started && needsRebuild )
        {
            // If we're in recovery and know that we're building the id generator from scratch after recovery has completed then don't make any updates
            return NOOP_COMMIT_MARKER;
        }

        return lockAndInstantiateMarker( true );
    }

    @Override
    public ReuseMarker reuseMarker()
    {
        if ( !started && needsRebuild )
        {
            // If we're in recovery and know that we're building the id generator from scratch after recovery has completed then don't make any updates
            return NOOP_REUSE_MARKER;
        }

        return lockAndInstantiateMarker( true );
    }

    private IdRangeMarker lockAndInstantiateMarker( boolean bridgeIdGaps )
    {
        commitAndReuseLock.lock();
        try
        {
            return new IdRangeMarker( idsPerEntry, layout, tree.writer(), commitAndReuseLock,
                    started ? IdRangeMerger.DEFAULT : IdRangeMerger.RECOVERY,
                    started, atLeastOneIdOnFreelist, generation, highestWrittenId, bridgeIdGaps );
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
        closeAllUnchecked( scanner, tree );
    }

    @Override
    public long getHighId()
    {
        return highId.get();
    }

    @Override
    public void setHighId( long newHighId )
    {
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
    public void start( FreeIds freeIdsForRebuild ) throws IOException
    {
        if ( needsRebuild )
        {
            // This id generator was created right now, it needs to be populated with all free ids from its owning store so that it's in sync
            try ( IdRangeMarker idRangeMarker = lockAndInstantiateMarker( false ) )
            {
                // We can mark the ids as free right away since this is before started which means we get the very liberal merger
                long highestFreeId = freeIdsForRebuild.accept( id ->
                {
                    idRangeMarker.markDeleted( id );
                    idRangeMarker.markFree( id );
                } );
                highId.set( highestFreeId + 1 );
                highestWrittenId.set( highestFreeId );
            }
            // We can checkpoint here since the free ids we read are committed
            checkpoint( IOLimiter.UNLIMITED );
            atLeastOneIdOnFreelist.set( true );
        }

        started = true;

        // After potentially recovery has been run and everything is prepared to get going let's call maintenance,
        // which will fill the ID buffers right away before any request comes to the db.
        maintenance();
    }

    @Override
    public void checkpoint( IOLimiter ioLimiter )
    {
        tree.checkpoint( ioLimiter, new HeaderWriter( highId::get, highestWrittenId::get, generation, idsPerEntry ) );
    }

    @Override
    public void maintenance()
    {
        if ( cache.size() < cacheOptimisticRefillThreshold )
        {
            // We're just helping other allocation requests and avoiding unwanted sliding of highId here
            scanner.tryLoadFreeIdsIntoCache();
        }
    }

    @Override
    public void clearCache()
    {
        // Make the scanner clear it because it needs to coordinate with the scan lock
        scanner.clearCache();
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
        this.highestWrittenId.set( highId.get() - 1 );
    }

    /**
     * Reads contents of a header in an existing {@link IndexedIdGenerator}.
     *
     * @param pageCache {@link PageCache} to map id generator in.
     * @param file {@link File} pointing to the id generator.
     * @return {@link Optional} with the data embedded inside the {@link HeaderReader} if the id generator existed and the header was read correctly,
     * otherwise {@link Optional#empty()}.
     */
    private static Optional<HeaderReader> readHeader( PageCache pageCache, File file )
    {
        try
        {
            HeaderReader headerReader = new HeaderReader();
            GBPTree.readHeader( pageCache, file, headerReader );
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
     * @param file {@link File} pointing to the id generator.
     * @throws IOException if the file was missing or some other I/O error occurred.
     */
    public static void dump( PageCache pageCache, File file ) throws IOException
    {
        HeaderReader header = readHeader( pageCache, file ).orElseThrow( () -> new NoSuchFileException( file.getAbsolutePath() ) );
        IdRangeLayout layout = new IdRangeLayout( header.idsPerEntry );
        try ( GBPTree<IdRangeKey,IdRange> tree = new GBPTree<>( pageCache, file, layout, 0, NO_MONITOR, NO_HEADER_READER, NO_HEADER_WRITER,
                immediate() ) )
        {
            tree.visit( new GBPTreeVisitor.Adaptor<>()
            {
                private IdRangeKey key;

                @Override
                public void key( IdRangeKey key, boolean isLeaf )
                {
                    this.key = key;
                }

                @Override
                public void value( IdRange value )
                {
                    System.out.println( format( "%s [%d]", value.toString(), key.getIdRangeIdx() ) );
                }
            } );
            System.out.println( header );
        }
    }
}
