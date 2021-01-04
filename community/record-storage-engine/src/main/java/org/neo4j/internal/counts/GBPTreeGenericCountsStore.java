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
package org.neo4j.internal.counts;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongConsumer;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.collection.PrimitiveLongArrayQueue;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeConsistencyCheckVisitor;
import org.neo4j.index.internal.gbptree.GBPTreeVisitor;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;

import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.internal.counts.CountsChanges.ABSENT;
import static org.neo4j.internal.counts.CountsKey.MAX_STRAY_TX_ID;
import static org.neo4j.internal.counts.CountsKey.MIN_STRAY_TX_ID;
import static org.neo4j.internal.counts.CountsKey.strayTxId;
import static org.neo4j.internal.counts.TreeWriter.merge;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

/**
 * A "counts store" backed by {@link GBPTree}. It solves the problem of incrementing/decrementing counts for arbitrary keys, while at the same time
 * being persistent and minimizing contention from concurrent writers.
 *
 * Updates that are {@link #updater(long, PageCursorTracer) applied} are relative values (e.g. +10 or -5) and counts are read as their absolute values.
 * Multiple transactions can update counts concurrently where counts are CAS:ed to minimize contention.
 * Updates between {@link #checkpoint(IOLimiter, PageCursorTracer) checkpoints} are kept in an internal {@link CountsChanges} map and only written
 * as part of a checkpoint. Checkpoint has a very short critical section where it switches over to a new {@link CountsChanges} instance
 * and also snapshots data about which transactions have applied before letting updaters continue to make changes while the checkpointing thread
 * writes the changes to the backing tree concurrently.
 *
 * Data flow wise updates are accumulated and written in each checkpoint. Reads are served from the tree or directly from {@link CountsChanges}
 * if there's changes to that particular key.
 */
public class GBPTreeGenericCountsStore implements AutoCloseable, ConsistencyCheckable
{
    public static final Monitor NO_MONITOR = txId -> {};
    private static final long NEEDS_REBUILDING_HIGH_ID = 0;
    private static final String OPEN_COUNT_STORE_TAG = "openCountStore";

    protected final GBPTree<CountsKey,CountsValue> tree;
    private final OutOfOrderSequence idSequence;
    /**
     * Guards interaction between checkpoint (write-lock) and transactions (read-lock).
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock( true );
    protected final CountsLayout layout = new CountsLayout();
    private final Rebuilder rebuilder;
    private final boolean readOnly;
    private final String name;
    private final Monitor monitor;
    protected volatile CountsChanges changes = new CountsChanges();
    private final TxIdInformation txIdInformation;
    private volatile boolean started;

    public GBPTreeGenericCountsStore( PageCache pageCache, Path file, FileSystemAbstraction fileSystem, RecoveryCleanupWorkCollector recoveryCollector,
            Rebuilder rebuilder, boolean readOnly, String name, PageCacheTracer pageCacheTracer, Monitor monitor ) throws IOException
    {
        this.readOnly = readOnly;
        this.name = name;
        this.monitor = monitor;

        // First just read the header so that we can avoid creating it if this store is read-only
        CountsHeader header = new CountsHeader( NEEDS_REBUILDING_HIGH_ID );
        GBPTree<CountsKey,CountsValue> instantiatedTree;
        try
        {
            instantiatedTree = instantiateTree( pageCache, file, recoveryCollector, readOnly, header, pageCacheTracer );
        }
        catch ( MetadataMismatchException e )
        {
            // Corrupt, delete and rebuild
            fileSystem.deleteFileOrThrow( file );
            header = new CountsHeader( NEEDS_REBUILDING_HIGH_ID );
            instantiatedTree = instantiateTree( pageCache, file, recoveryCollector, readOnly, header, pageCacheTracer );
        }
        this.tree = instantiatedTree;
        boolean successful = false;
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( OPEN_COUNT_STORE_TAG ) )
        {
            this.txIdInformation = readTxIdInformation( header.highestGapFreeTxId(), cursorTracer );
            // Recreate the tx id state as it was from last checkpoint (or base if empty)
            this.idSequence = new ArrayQueueOutOfOrderSequence( txIdInformation.highestGapFreeTxId, 200, EMPTY_LONG_ARRAY );
            this.txIdInformation.strayTxIds.forEach( txId -> idSequence.offer( txId, EMPTY_LONG_ARRAY ) );
            // Only care about initial counts rebuilding if the tree was created right now when opening this tree
            // The actual rebuilding will happen in start()
            this.rebuilder = header.wasRead() && header.highestGapFreeTxId() != NEEDS_REBUILDING_HIGH_ID ? null : rebuilder;
            successful = true;
        }
        finally
        {
            if ( !successful )
            {
                closeAllUnchecked( tree );
            }
        }
    }

    private GBPTree<CountsKey,CountsValue> instantiateTree( PageCache pageCache, Path file, RecoveryCleanupWorkCollector recoveryCollector, boolean readOnly,
            CountsHeader header, PageCacheTracer pageCacheTracer )
    {
        try
        {
            return new GBPTree<>( pageCache, file, layout, GBPTree.NO_MONITOR, header, header, recoveryCollector, readOnly, pageCacheTracer,
                    immutable.empty(), name );
        }
        catch ( TreeFileNotFoundException e )
        {
            throw new IllegalStateException(
                    "Counts store file could not be found, most likely this database needs to be recovered, file:" + file, e );
        }
    }

    // === Life cycle ===

    public void start( PageCursorTracer cursorTracer, MemoryTracker memoryTracker ) throws IOException
    {
        // Execute the initial counts building if we need to, i.e. if instantiation of this counts store had to create it
        if ( rebuilder != null )
        {
            if ( readOnly )
            {
                throw new IllegalStateException( "Counts store needs rebuilding, most likely this database needs to be recovered." );
            }
            Lock lock = lock( this.lock.writeLock() );
            long txId = rebuilder.lastCommittedTxId();
            try ( CountUpdater updater = new CountUpdater( new TreeWriter( tree.writer( cursorTracer ), idSequence, txId ), lock ) )
            {
                rebuilder.rebuild( updater, cursorTracer, memoryTracker );
            }
        }
        started = true;
    }

    @Override
    public void close()
    {
        closeAllUnchecked( tree );
    }

    // === Writes ===

    protected CountUpdater updater( long txId, PageCursorTracer cursorTracer )
    {
        Preconditions.checkState( !readOnly, "This counts store is read-only" );
        Lock lock = lock( this.lock.readLock() );

        boolean alreadyApplied = txIdInformation.txIdIsAlreadyApplied( txId );
        // Why have this check below? Why should we not apply transactions before started when we have an initial counts builder?
        // Consider the following scenario:
        // - Create node N
        // - Checkpoint
        // - Delete node N
        // - Crash
        // - Delete counts store
        // - Startup, where recovery starts
        // - Recovery replays deletion of N
        // - After recovery the counts store is rebuilt from scratch
        //
        // The deletion of N on the empty counts store would have resulted in a count of -1, which is not OK to write to the tree,
        // since there can never be a negative amount of, say nodes. The counts store will be rebuilt after recovery anyway,
        // so ignore these transactions.
        boolean inRecoveryOnEmptyCountsStore = rebuilder != null && !started;
        if ( alreadyApplied || inRecoveryOnEmptyCountsStore )
        {
            lock.unlock();
            monitor.ignoredTransaction( txId );
            return null;
        }
        return new CountUpdater( new MapWriter( key -> readCountFromTree( key, cursorTracer ), changes, idSequence, txId ), lock );
    }

    public void checkpoint( IOLimiter ioLimiter, PageCursorTracer cursorTracer ) throws IOException
    {
        if ( readOnly )
        {
            return;
        }

        // First acquire the write lock. This is a fair lock and will wait for currently applying transactions to finish.
        // This could potentially block appliers around this point since they will respect the fairness too.
        // The good thing is that the lock is held very very briefly.
        Lock writeLock = lock( this.lock.writeLock() );

        // When we have the lock we do two things (no updates will come in while we have it):
        CountsChanges changesToWrite;
        OutOfOrderSequence.Snapshot txIdSnapshot;
        try
        {
            // Take a snapshot of applied transactions (but write it later, no need to write it under the lock)
            txIdSnapshot = idSequence.snapshot();

            // Take the changes and instantiate a new map for other updates to apply to after we release this lock
            changesToWrite = changes;
            changes = changes.freezeAndFork();
        }
        finally
        {
            writeLock.unlock();
        }

        // Now write all the things to the tree
        writeCountsChanges( changesToWrite, cursorTracer );
        changes.clearPreviousChanges();
        updateTxIdInformationInTree( txIdSnapshot, cursorTracer );

        // Good, check-point all these changes
        tree.checkpoint( ioLimiter, new CountsHeader( txIdSnapshot.highestGapFree()[0] ), cursorTracer );
    }

    private void writeCountsChanges( CountsChanges changes, PageCursorTracer cursorTracer ) throws IOException
    {
        // Sort the entries in the natural tree order to get more performance in the writer
        Iterable<Map.Entry<CountsKey,AtomicLong>> changeList = changes.sortedChanges( layout );
        try ( Writer<CountsKey,CountsValue> writer = tree.writer( cursorTracer ) )
        {
            CountsValue value = new CountsValue();
            for ( Map.Entry<CountsKey,AtomicLong> entry : changeList )
            {
                long count = entry.getValue().get();
                merge( writer, entry.getKey(), value.initialize( count ) );
            }
        }
    }

    private void updateTxIdInformationInTree( OutOfOrderSequence.Snapshot txIdSnapshot, PageCursorTracer cursorTracer ) throws IOException
    {
        PrimitiveLongArrayQueue strayIds = new PrimitiveLongArrayQueue();
        visitStrayTxIdsInTree( strayIds::enqueue, cursorTracer );

        try ( Writer<CountsKey,CountsValue> writer = tree.writer( cursorTracer ) )
        {
            // First clear all the stray ids from the previous checkpoint
            CountsValue value = new CountsValue();
            while ( !strayIds.isEmpty() )
            {
                long strayTxId = strayIds.dequeue();
                writer.remove( strayTxId( strayTxId ) );
            }

            // And write all stray txIds into the tree
            value.initialize( 0 );
            long[][] strayTxIds = txIdSnapshot.idsOutOfOrder();
            for ( long[] strayTxId : strayTxIds )
            {
                long txId = strayTxId[0];
                writer.put( strayTxId( txId ), value );
            }
        }
    }

    // === Reads ===

    public long txId()
    {
        return idSequence.getHighestGapFreeNumber();
    }

    protected long read( CountsKey key, PageCursorTracer cursorTracer )
    {
        long changedCount = changes.get( key );
        return changedCount != ABSENT ? changedCount : readCountFromTree( key, cursorTracer );
    }

    public void visitAllCounts( CountVisitor visitor, PageCursorTracer cursorTracer )
    {
        // First visit the changes that we haven't check-pointed yet
        for ( Map.Entry<CountsKey,AtomicLong> changedEntry : changes.sortedChanges( layout ) )
        {
            // Our simplistic approach to the changes map makes it contain 0 counts at times, we don't remove entries from it
            if ( changedEntry.getValue().get() != 0 )
            {
                visitor.visit( changedEntry.getKey(), changedEntry.getValue().get() );
            }
        }

        // Then visit the remaining stored changes from the last check-point
        try ( Seeker<CountsKey,CountsValue> seek = tree.seek( CountsKey.MIN_COUNT, CountsKey.MAX_COUNT, cursorTracer ) )
        {
            while ( seek.next() )
            {
                CountsKey key = seek.key();
                if ( !changes.containsChange( key ) )
                {
                    visitor.visit( key, seek.value().count );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    /**
     * Read the count from the store. For writes this is done on an unchanging tree because we have the read lock where check-pointing
     * (where changes are written to the tree) can only be done if the write-lock is acquired. For plain unmodified reads this is read from the tree
     * without a lock, which is fine and follows general transaction isolation guarantees.
     * @param key count value to read from the tree.
     * @return AtomicLong with the read count, or initialized to 0 if the count didn't exist in the tree.
     */
    private long readCountFromTree( CountsKey key, PageCursorTracer cursorTracer )
    {
        try ( Seeker<CountsKey,CountsValue> seek = tree.seek( key, key, cursorTracer ) )
        {
            return seek.next() ? seek.value().count : 0;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void visitStrayTxIdsInTree( LongConsumer visitor, PageCursorTracer cursorTracer ) throws IOException
    {
        try ( Seeker<CountsKey,CountsValue> seek = tree.seek( MIN_STRAY_TX_ID, MAX_STRAY_TX_ID, cursorTracer ) )
        {
            while ( seek.next() )
            {
                visitor.accept( seek.key().first );
            }
        }
    }

    private TxIdInformation readTxIdInformation( long highestGapFreeTxId, PageCursorTracer cursorTracer ) throws IOException
    {
        MutableLongSet strayTxIds = new LongHashSet();
        visitStrayTxIdsInTree( strayTxIds::add, cursorTracer );
        return new TxIdInformation( highestGapFreeTxId, strayTxIds );
    }

    private static Lock lock( Lock lock )
    {
        lock.lock();
        return lock;
    }

    @Override
    public boolean consistencyCheck( ReporterFactory reporterFactory, PageCursorTracer cursorTracer )
    {
        return consistencyCheck( reporterFactory.getClass( GBPTreeConsistencyCheckVisitor.class ), cursorTracer );
    }

    private boolean consistencyCheck( GBPTreeConsistencyCheckVisitor<CountsKey> visitor, PageCursorTracer cursorTracer )
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

    /**
     * Dumps the contents of a counts store.
     *
     * @param pageCache {@link PageCache} to use to map the counts store file into.
     * @param file {@link Path} pointing out the counts store.
     * @param out to print to.
     * @param name of the {@link GBPTree}.
     * @throws IOException on missing file or I/O error.
     */
    protected static void dump( PageCache pageCache, Path file, PrintStream out, String name, PageCursorTracer cursorTracer ) throws IOException
    {
        // First check if it even exists as we don't really want to create it as part of dumping it. readHeader will throw if not found
        CountsHeader header = new CountsHeader( BASE_TX_ID );
        GBPTree.readHeader( pageCache, file, header, cursorTracer );

        // Now open it and dump its contents
        try ( GBPTree<CountsKey,CountsValue> tree = new GBPTree<>( pageCache, file, new CountsLayout(), GBPTree.NO_MONITOR, header, GBPTree.NO_HEADER_WRITER,
                RecoveryCleanupWorkCollector.ignore(), true, NULL, immutable.empty(), name ) )
        {
            out.printf( "Highest gap-free txId: %d%n", header.highestGapFreeTxId() );
            tree.visit( new GBPTreeVisitor.Adaptor<>()
            {
                private CountsKey key;

                @Override
                public void key( CountsKey key, boolean isLeaf, long offloadId )
                {
                    this.key = key;
                }

                @Override
                public void value( CountsValue value )
                {
                    out.printf( "%s = %d%n", key, value.count );
                }
            }, cursorTracer );
        }
    }

    public interface Monitor
    {
        void ignoredTransaction( long txId );
    }

    public interface Rebuilder
    {
        long lastCommittedTxId();

        void rebuild( CountUpdater updater, PageCursorTracer cursorTracer, MemoryTracker memoryTracker );
    }

    public interface CountVisitor
    {
        void visit( CountsKey key, long count );
    }
}
