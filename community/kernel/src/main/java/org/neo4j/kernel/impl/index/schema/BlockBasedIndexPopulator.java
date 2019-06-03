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
package org.neo4j.kernel.impl.index.schema;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.BatchingMultipleIndexPopulator;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.index.schema.ByteBufferFactory.Allocator;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsWriter;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.util.FeatureToggles;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;

import static org.neo4j.helpers.collection.Iterables.first;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.index.schema.BlockStorage.Monitor.NO_MONITOR;
import static org.neo4j.kernel.impl.index.schema.NativeIndexUpdater.initializeKeyFromUpdate;
import static org.neo4j.kernel.impl.index.schema.NativeIndexes.deleteIndex;
import static org.neo4j.util.concurrent.Runnables.runAll;

/**
 * {@link IndexPopulator} for native indexes that stores scan updates in parallel append-only files. When all scan updates have been collected
 * each file is sorted and then all of them merged together into the resulting index.
 *
 * Note on buffers: basically each thread adding scan updates will make use of a {@link ByteBufferFactory#acquireThreadLocalBuffer() thread-local buffer}.
 * This together with {@link ByteBufferFactory#globalAllocator() a global buffer for external updates} and carefully reused
 * {@link ByteBufferFactory#newLocalAllocator() local buffers} for merging allows memory consumption to stay virtually the same regardless
 * how many indexes are being built concurrently by the same job and regardless of index sizes. Formula for peak number of buffers in use is roughly
 * {@code 10 * numberOfPopulationWorkers} where numberOfPopulationWorkers is currently capped to 8. So given a buffer size of 1 MiB then maximum memory
 * usage for one population job (which can populate multiple index) is ~80 MiB.
 *
 * @param <KEY>
 * @param <VALUE>
 */
public abstract class BlockBasedIndexPopulator<KEY extends NativeIndexKey<KEY>,VALUE extends NativeIndexValue> extends NativeIndexPopulator<KEY,VALUE>
{
    public static final String BLOCK_SIZE_NAME = "blockSize";

    private final IndexDirectoryStructure directoryStructure;
    private final IndexDropAction dropAction;
    private final boolean archiveFailedIndex;
    /**
     * When merging all blocks together the algorithm does multiple passes over the block storage, until the number of blocks reaches 1.
     * Every pass does one or more merges and every merge merges up to {@link #mergeFactor} number of blocks into one block,
     * i.e. the number of blocks shrinks by a factor {@link #mergeFactor} every pass, until one block is left.
     */
    private final int mergeFactor;
    private final BlockStorage.Monitor blockStorageMonitor;
    // written to in a synchronized method when creating new thread-local instances, read from when population completes
    private final List<ThreadLocalBlockStorage> allScanUpdates = new CopyOnWriteArrayList<>();
    private final ThreadLocal<ThreadLocalBlockStorage> scanUpdates;
    private final ByteBufferFactory bufferFactory;
    private IndexUpdateStorage<KEY,VALUE> externalUpdates;
    // written in a synchronized method when creating new thread-local instances, read when processing external updates
    private volatile boolean scanCompleted;
    private final CloseCancellation cancellation = new CloseCancellation();
    // Will be instantiated right before merging and can be used to neatly await merge to complete
    private volatile CountDownLatch mergeOngoingLatch;

    // progress state
    private volatile long numberOfAppliedScanUpdates;
    private volatile long numberOfAppliedExternalUpdates;

    BlockBasedIndexPopulator( PageCache pageCache, FileSystemAbstraction fs, File file, IndexLayout<KEY,VALUE> layout, IndexProvider.Monitor monitor,
            StoreIndexDescriptor descriptor, IndexSpecificSpaceFillingCurveSettingsCache spatialSettings,
            IndexDirectoryStructure directoryStructure, IndexDropAction dropAction, boolean archiveFailedIndex, ByteBufferFactory bufferFactory )
    {
        this( pageCache, fs, file, layout, monitor, descriptor, spatialSettings, directoryStructure, dropAction, archiveFailedIndex, bufferFactory,
                FeatureToggles.getInteger( BlockBasedIndexPopulator.class, "mergeFactor", 8 ), NO_MONITOR );
    }

    BlockBasedIndexPopulator( PageCache pageCache, FileSystemAbstraction fs, File file, IndexLayout<KEY,VALUE> layout, IndexProvider.Monitor monitor,
            StoreIndexDescriptor descriptor, IndexSpecificSpaceFillingCurveSettingsCache spatialSettings,
            IndexDirectoryStructure directoryStructure, IndexDropAction dropAction, boolean archiveFailedIndex, ByteBufferFactory bufferFactory,
            int mergeFactor, BlockStorage.Monitor blockStorageMonitor )
    {
        super( pageCache, fs, file, layout, monitor, descriptor, new SpaceFillingCurveSettingsWriter( spatialSettings ) );
        this.directoryStructure = directoryStructure;
        this.dropAction = dropAction;
        this.archiveFailedIndex = archiveFailedIndex;
        this.mergeFactor = mergeFactor;
        this.blockStorageMonitor = blockStorageMonitor;
        this.scanUpdates = ThreadLocal.withInitial( this::newThreadLocalBlockStorage );
        this.bufferFactory = bufferFactory;
    }

    private synchronized ThreadLocalBlockStorage newThreadLocalBlockStorage()
    {
        Preconditions.checkState( !cancellation.cancelled(), "Already closed" );
        Preconditions.checkState( !scanCompleted, "Scan has already been completed" );
        try
        {
            int id = allScanUpdates.size();
            ThreadLocalBlockStorage blockStorage = new ThreadLocalBlockStorage( id );
            allScanUpdates.add( blockStorage );
            return blockStorage;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    /**
     * Base size of blocks of entries. As entries gets written to a BlockStorage, they are buffered up to this size, then sorted and written out.
     * As blocks gets merged into bigger blocks, this is still the size of the read buffer for each block no matter its size.
     * Each thread has its own buffer when writing and each thread has {@link #mergeFactor} buffers when merging.
     * The memory usage will be at its biggest during merge and a total memory usage sum can be calculated like so:
     *
     * blockSize * numberOfPopulationWorkers * {@link #mergeFactor}
     *
     * where typically {@link BatchingMultipleIndexPopulator} controls the number of population workers. The setting
     * `unsupported.dbms.multi_threaded_schema_index_population_enabled` controls whether or not the multi-threaded {@link BatchingMultipleIndexPopulator}
     * is used, otherwise a single-threaded populator is used instead.
     */
    public static int parseBlockSize()
    {
        long blockSize = ByteUnit.parse( FeatureToggles.getString( BlockBasedIndexPopulator.class, BLOCK_SIZE_NAME, "1M" ) );
        Preconditions.checkArgument( blockSize >= 20 && blockSize < Integer.MAX_VALUE, "Block size need to fit in int. Was " + blockSize );
        return (int) blockSize;
    }

    @Override
    public void create()
    {
        try
        {
            deleteIndex( fileSystem, directoryStructure, descriptor.getId(), archiveFailedIndex );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        super.create();
        try
        {
            File externalUpdatesFile = new File( storeFile.getParent(), storeFile.getName() + ".ext" );
            externalUpdates = new IndexUpdateStorage<>( fileSystem, externalUpdatesFile, bufferFactory.globalAllocator(), smallerBufferSize(), layout );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private int smallerBufferSize()
    {
        return bufferFactory.bufferSize() / 2;
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates )
    {
        if ( !updates.isEmpty() )
        {
            BlockStorage<KEY,VALUE> blockStorage = scanUpdates.get().blockStorage;
            for ( IndexEntryUpdate<?> update : updates )
            {
                storeUpdate( update, blockStorage );
            }
        }
    }

    private void storeUpdate( long entityId, Value[] values, BlockStorage<KEY,VALUE> blockStorage )
    {
        try
        {
            KEY key = layout.newKey();
            VALUE value = layout.newValue();
            initializeKeyFromUpdate( key, entityId, values );
            value.from( values );
            blockStorage.add( key, value );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void storeUpdate( IndexEntryUpdate<?> update, BlockStorage<KEY,VALUE> blockStorage )
    {
        storeUpdate( update.getEntityId(), update.values(), blockStorage );
    }

    private synchronized boolean markMergeStarted()
    {
        scanCompleted = true;
        if ( cancellation.cancelled() )
        {
            return false;
        }
        mergeOngoingLatch = new CountDownLatch( 1 );
        return true;
    }

    @Override
    public void scanCompleted( PhaseTracker phaseTracker ) throws IndexEntryConflictException
    {
        if ( !markMergeStarted() )
        {
            // This populator has already been closed, either from an external cancel or drop call.
            // Either way we're not supposed to do this merge.
            return;
        }

        try
        {
            phaseTracker.enterPhase( PhaseTracker.Phase.MERGE );
            if ( !allScanUpdates.isEmpty() )
            {
                mergeScanUpdates();
            }

            externalUpdates.doneAdding();
            // don't merge and sort the external updates

            // Build the tree from the scan updates
            if ( cancellation.cancelled() )
            {
                // Do one additional check before starting to write to the tree
                return;
            }
            phaseTracker.enterPhase( PhaseTracker.Phase.BUILD );
            File duplicatesFile = new File( storeFile.getParentFile(), storeFile.getName() + ".dup" );
            int readBufferSize = smallerBufferSize();
            try ( Allocator allocator = bufferFactory.newLocalAllocator();
                    IndexKeyStorage<KEY> indexKeyStorage = new IndexKeyStorage<>( fileSystem, duplicatesFile, allocator, readBufferSize, layout ) )
            {
                RecordingConflictDetector<KEY,VALUE> recordingConflictDetector = new RecordingConflictDetector<>( !descriptor.isUnique(), indexKeyStorage );
                writeScanUpdatesToTree( recordingConflictDetector, allocator, readBufferSize );

                // Apply the external updates
                phaseTracker.enterPhase( PhaseTracker.Phase.APPLY_EXTERNAL );
                writeExternalUpdatesToTree( recordingConflictDetector );

                // Verify uniqueness
                if ( descriptor.isUnique() )
                {
                    try ( IndexKeyStorage.KeyEntryCursor<KEY> allConflictingKeys = recordingConflictDetector.allConflicts() )
                    {
                        verifyUniqueKeys( allConflictingKeys );
                    }
                }
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( "Got interrupted, so merge not completed", e );
        }
        catch ( ExecutionException e )
        {
            // Propagating merge exception from other thread
            Throwable executionException = e.getCause();
            if ( executionException instanceof RuntimeException )
            {
                throw (RuntimeException) executionException;
            }
            throw new RuntimeException( executionException );
        }
        finally
        {
            mergeOngoingLatch.countDown();
        }
    }

    private void mergeScanUpdates() throws InterruptedException, ExecutionException, IOException
    {
        ExecutorService executorService = Executors.newFixedThreadPool( allScanUpdates.size() );
        List<Future<?>> mergeFutures = new ArrayList<>();
        for ( ThreadLocalBlockStorage part : allScanUpdates )
        {
            BlockStorage<KEY,VALUE> scanUpdates = part.blockStorage;
            // Call doneAdding here so that the buffer it allocates if it needs to flush something will be shared with other indexes
            scanUpdates.doneAdding();
            mergeFutures.add( executorService.submit( () ->
            {
                scanUpdates.merge( mergeFactor, cancellation );
                return null;
            } ) );
        }
        executorService.shutdown();
        while ( !executorService.awaitTermination( 1, TimeUnit.SECONDS ) )
        {
            // just wait longer
        }
        // Let potential exceptions in the merge threads have a chance to propagate
        for ( Future<?> mergeFuture : mergeFutures )
        {
            mergeFuture.get();
        }
    }

    /**
     * We will loop over all external updates once to add them to the tree. This is done without checking any uniqueness.
     * If index is a uniqueness index we will then loop over external updates again and for each ADD or CHANGED update
     * we will verify that those entries are unique in the tree and throw as soon as we find a duplicate.
     * @throws IOException If something goes wrong while reading from index.
     * @throws IndexEntryConflictException If a duplicate is found.
     */
    private void writeExternalUpdatesToTree( RecordingConflictDetector<KEY,VALUE> recordingConflictDetector ) throws IOException, IndexEntryConflictException
    {
        try ( Writer<KEY,VALUE> writer = tree.writer();
              IndexUpdateCursor<KEY,VALUE> updates = externalUpdates.reader() )
        {
            while ( updates.next() && !cancellation.cancelled() )
            {
                switch ( updates.updateMode() )
                {
                case ADDED:
                    writeToTree( writer, recordingConflictDetector, updates.key(), updates.value() );
                    break;
                case REMOVED:
                    writer.remove( updates.key() );
                    break;
                case CHANGED:
                    writer.remove( updates.key() );
                    writeToTree( writer, recordingConflictDetector, updates.key2(), updates.value() );
                    break;
                default:
                    throw new IllegalArgumentException( "Unknown update mode " + updates.updateMode() );
                }
                numberOfAppliedExternalUpdates++;
            }
        }
    }

    private void verifyUniqueKeys( IndexKeyStorage.KeyEntryCursor<KEY> allConflictingKeys ) throws IOException, IndexEntryConflictException
    {
        while ( allConflictingKeys.next() && !cancellation.cancelled() )
        {
            KEY key = allConflictingKeys.key();
            key.setCompareId( false );
            verifyUniqueSeek( tree.seek( key, key ) );
        }
    }

    private void verifyUniqueSeek( RawCursor<Hit<KEY,VALUE>,IOException> seek ) throws IOException, IndexEntryConflictException
    {
        if ( seek != null )
        {
            if ( seek.next() )
            {
                long firstEntityId = seek.get().key().getEntityId();
                if ( seek.next() )
                {
                    long secondEntityId = seek.get().key().getEntityId();
                    throw new IndexEntryConflictException( firstEntityId, secondEntityId, seek.get().key().asValues() );
                }
            }
        }
    }

    private void writeScanUpdatesToTree( RecordingConflictDetector<KEY,VALUE> recordingConflictDetector, Allocator allocator, int bufferSize )
            throws IOException, IndexEntryConflictException
    {
        try ( MergingBlockEntryReader<KEY,VALUE> allEntries = new MergingBlockEntryReader<>( layout ) )
        {
            ByteBuffer singleBlockAssertionBuffer = allocator.allocate( (int) kibiBytes( 8 ) );
            for ( ThreadLocalBlockStorage part : allScanUpdates )
            {
                try ( BlockReader<KEY,VALUE> reader = part.blockStorage.reader() )
                {
                    BlockEntryReader<KEY,VALUE> singleMergedBlock = reader.nextBlock( allocator.allocate( bufferSize ) );
                    if ( singleMergedBlock != null )
                    {
                        allEntries.addSource( singleMergedBlock );
                        // Pass in some sort of ByteBuffer here. The point is that there should be no more data to read,
                        // if there is then it's due to a bug in the code and must be fixed.
                        if ( reader.nextBlock( singleBlockAssertionBuffer ) != null )
                        {
                            throw new IllegalStateException( "Final BlockStorage had multiple blocks" );
                        }
                    }
                }
            }

            int asMuchAsPossibleToTheLeft = 1;
            try ( Writer<KEY,VALUE> writer = tree.writer( asMuchAsPossibleToTheLeft ) )
            {
                while ( allEntries.next() && !cancellation.cancelled() )
                {
                    writeToTree( writer, recordingConflictDetector, allEntries.key(), allEntries.value() );
                    numberOfAppliedScanUpdates++;
                }
            }
        }
    }

    @Override
    public IndexUpdater newPopulatingUpdater()
    {
        if ( scanCompleted )
        {
            // Will need the reader from newReader, which a sub-class of this class implements
            return super.newPopulatingUpdater();
        }

        return new IndexUpdater()
        {
            private volatile boolean closed;

            @Override
            public void process( IndexEntryUpdate<?> update )
            {
                assertOpen();
                try
                {
                    externalUpdates.add( update );
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( e );
                }
            }

            @Override
            public void close()
            {
                closed = true;
            }

            private void assertOpen()
            {
                if ( closed )
                {
                    throw new IllegalStateException( "Updater has been closed" );
                }
            }
        };
    }

    @Override
    public synchronized void drop()
    {
        runAll( "Failed while trying to drop index",
                this::closeBlockStorage /* Close internal resources */,
                super::drop /* Super drop will close inherited resources */,
                () -> dropAction.drop( descriptor.getId(), archiveFailedIndex ) /* Cleanup files */
        );
    }

    @Override
    public synchronized void close( boolean populationCompletedSuccessfully )
    {
        runAll( "Failed while trying to close index",
                this::closeBlockStorage /* Close internal resources */,
                () -> super.close( populationCompletedSuccessfully ) /* Super close will close inherited resources */
        );
    }

    // Always called from synchronized method
    private void closeBlockStorage()
    {
        // This method may be called while scanCompleted is running. This could be a drop or shutdown(?) which happens when this population
        // is in its final stages. scanCompleted merges things in multiple threads. Those threads will abort when they see that setCancel
        // has been called.
        cancellation.setCancel();

        // If there's a merge concurrently running it will very soon notice the cancel request and abort whatever it's doing as soon as it can.
        // Let's wait for that merge to be fully aborted by simply waiting for the merge latch.
        if ( mergeOngoingLatch != null )
        {
            try
            {
                // We want to await any ongoing merge because it becomes problematic to close the channels otherwise
                mergeOngoingLatch.await();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                // We still want to go ahead and try to close things properly, so get by only restoring the interrupt flag on the thread
            }
        }

        List<Closeable> toClose = allScanUpdates.stream().map( local -> local.blockStorage ).collect( Collectors.toCollection( ArrayList::new ) );
        toClose.add( externalUpdates );
        IOUtils.closeAllUnchecked( toClose );
    }

    @Override
    public PopulationProgress progress( PopulationProgress scanProgress )
    {
        // A general note on scanProgress.getTotal(). Before the scan is completed most progress parts will base their estimates on that value.
        // It is known that it may be slightly higher since it'll be based on store high-id, not the actual count.
        // This is fine, but it creates this small "jump" in the progress in the middle somewhere when it switches from scan to merge.
        // This also exists in the most basic population progress reports, but there it will be less visible since it will jump from
        // some close-to-100 percentage to 100% and ONLINE.

        // This progress report will consist of a couple of smaller parts, weighted differently based on empirically collected values.
        // The weights will not be absolutely correct in all environments, but they don't have to be either, it will just result in some
        // slices of the percentage progression range progressing at slightly different paces. However, progression of progress reporting
        // naturally fluctuates anyway due to data set and I/O etc. so this is not an actual problem.
        PopulationProgress.MultiBuilder builder = PopulationProgress.multiple();

        // Add scan progress (this one weights a bit heavier than the others)
        builder.add( scanProgress, 4 );

        // Add merge progress
        if ( !allScanUpdates.isEmpty() )
        {
            // The parts are merged in parallel so just take the first one and it will represent the whole merge progress.
            // It will be fairly accurate, but slightly off sometimes if other threads gets scheduling problems, i.e. if this part
            // finish far apart from others.
            long completed = 0;
            long total = 0;
            if ( scanCompleted )
            {
                // We know the actual entry count to write during merge since we have been monitoring those values
                ThreadLocalBlockStorage part = first( allScanUpdates );
                completed = part.entriesMerged;
                total = part.totalEntriesToMerge;
            }
            builder.add( PopulationProgress.single( completed, total ), 1 );
        }

        // Add tree building incl. external updates
        PopulationProgress treeBuildProgress;
        if ( allScanUpdates.stream().allMatch( part -> part.mergeStarted ) )
        {
            long entryCount = allScanUpdates.stream().mapToLong( part -> part.count ).sum() + externalUpdates.count();
            treeBuildProgress = PopulationProgress.single( numberOfAppliedScanUpdates + numberOfAppliedExternalUpdates, entryCount );
        }
        else
        {
            treeBuildProgress = PopulationProgress.NONE;
        }
        builder.add( treeBuildProgress, 2 );

        return builder.build();
    }

    /**
     * Write key and value to tree and record duplicates if any.
     */
    private void writeToTree( Writer<KEY,VALUE> writer, RecordingConflictDetector<KEY,VALUE> recordingConflictDetector, KEY key, VALUE value )
            throws IndexEntryConflictException
    {
        recordingConflictDetector.controlConflictDetection( key );
        writer.merge( key, value, recordingConflictDetector );
        handleMergeConflict( writer, recordingConflictDetector, key, value );
    }

    /**
     * Will check if recording conflict detector saw a conflict. If it did, that conflict has been recorded and we will verify uniqueness for this
     * value later on. But for now we try and insert conflicting value again but with a relaxed uniqueness constraint. Insert is done with a throwing
     * conflict checker which means it will throw if we see same value AND same id in one key.
     */
    private void handleMergeConflict( Writer<KEY,VALUE> writer, RecordingConflictDetector<KEY,VALUE> recordingConflictDetector, KEY key, VALUE value )
            throws IndexEntryConflictException
    {
        if ( recordingConflictDetector.wasConflicting() )
        {
            // Report conflict
            KEY copy = layout.newKey();
            layout.copyKey( key, copy );
            recordingConflictDetector.reportConflict( copy );

            // Insert and overwrite with relaxed uniqueness constraint
            recordingConflictDetector.relaxUniqueness( key );
            writer.put( key, value );
        }
    }

    /**
     * Keeps track of a {@link BlockStorage} instance as well as monitoring some aspects of it to be able to provide a fairly accurate
     * progress report from {@link BlockBasedIndexPopulator#progress(PopulationProgress)}.
     */
    private class ThreadLocalBlockStorage extends BlockStorage.Monitor.Delegate
    {
        private final BlockStorage<KEY,VALUE> blockStorage;
        private volatile long count;
        private volatile boolean mergeStarted;
        private volatile long totalEntriesToMerge;
        private volatile long entriesMerged;

        ThreadLocalBlockStorage( int id ) throws IOException
        {
            super( blockStorageMonitor );
            File blockFile = new File( storeFile.getParentFile(), storeFile.getName() + ".scan-" + id );
            this.blockStorage = new BlockStorage<>( layout, bufferFactory, fileSystem, blockFile, this );
        }

        @Override
        public void mergeStarted( long entryCount, long totalEntriesToWriteDuringMerge )
        {
            super.mergeStarted( entryCount, totalEntriesToWriteDuringMerge );
            this.count = entryCount;
            this.totalEntriesToMerge = totalEntriesToWriteDuringMerge;
            this.mergeStarted = true;
        }

        @Override
        public void entriesMerged( int entries )
        {
            super.entriesMerged( entries );
            entriesMerged += entries;
        }
    }

    private static class CloseCancellation implements BlockStorage.Cancellation
    {
        private volatile boolean cancelled;

        void setCancel()
        {
            this.cancelled = true;
        }

        @Override
        public boolean cancelled()
        {
            return cancelled;
        }
    }

    private static class RecordingConflictDetector<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
            extends ConflictDetectingValueMerger<KEY,VALUE,KEY>
    {
        private final IndexKeyStorage<KEY> allConflictingKeys;

        RecordingConflictDetector( boolean compareEntityIds, IndexKeyStorage<KEY> indexKeyStorage )
        {
            super( compareEntityIds );
            allConflictingKeys = indexKeyStorage;
        }

        @Override
        void doReportConflict( long existingNodeId, long addedNodeId, KEY conflictingKey )
        {
            try
            {
                allConflictingKeys.add( conflictingKey );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        IndexKeyStorage.KeyEntryCursor<KEY> allConflicts() throws IOException
        {
            allConflictingKeys.doneAdding();
            return allConflictingKeys.reader();
        }

        void relaxUniqueness( KEY key )
        {
            key.setCompareId( true );
        }
    }
}
