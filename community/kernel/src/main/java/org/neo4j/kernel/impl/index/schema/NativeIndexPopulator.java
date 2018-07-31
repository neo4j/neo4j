/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.util.concurrent.Work;
import org.neo4j.util.concurrent.WorkSync;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.storageengine.api.schema.IndexDescriptor.Type.GENERAL;
import static org.neo4j.storageengine.api.schema.IndexDescriptor.Type.UNIQUE;

/**
 * {@link IndexPopulator} backed by a {@link GBPTree}.
 *
 * @param <KEY> type of {@link NativeIndexSingleValueKey}.
 * @param <VALUE> type of {@link NativeIndexValue}.
 */
public abstract class NativeIndexPopulator<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
        extends NativeIndex<KEY,VALUE> implements IndexPopulator
{
    public static final byte BYTE_FAILED = 0;
    static final byte BYTE_ONLINE = 1;
    static final byte BYTE_POPULATING = 2;

    private final KEY treeKey;
    private final VALUE treeValue;
    private final UniqueIndexSampler uniqueSampler;
    final IndexSamplingConfig samplingConfig;

    private WorkSync<IndexUpdateApply<KEY,VALUE>,IndexUpdateWork<KEY,VALUE>> additionsWorkSync;
    private WorkSync<IndexUpdateApply<KEY,VALUE>,IndexUpdateWork<KEY,VALUE>> updatesWorkSync;

    private byte[] failureBytes;
    private boolean dropped;
    private boolean closed;

    NativeIndexPopulator( PageCache pageCache, FileSystemAbstraction fs, File storeFile, IndexLayout<KEY,VALUE> layout, IndexProvider.Monitor monitor,
                                StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        super( pageCache, fs, storeFile, layout, monitor, descriptor );
        this.treeKey = layout.newKey();
        this.treeValue = layout.newValue();
        this.samplingConfig = samplingConfig;
        switch ( descriptor.type() )
        {
        case GENERAL:
            uniqueSampler = null;
            break;
        case UNIQUE:
            uniqueSampler = new UniqueIndexSampler();
            break;
        default:
            throw new IllegalArgumentException( "Unexpected index type " + descriptor.type() );
        }
    }

    public void clear()
    {
        deleteFileIfPresent( fileSystem, storeFile );
    }

    @Override
    public synchronized void create()
    {
        create( new NativeIndexHeaderWriter( BYTE_POPULATING ) );
    }

    protected synchronized void create( Consumer<PageCursor> headerWriter )
    {
        assertNotDropped();
        assertNotClosed();

        deleteFileIfPresent( fileSystem, storeFile );
        instantiateTree( RecoveryCleanupWorkCollector.immediate(), headerWriter );

        // true:  tree uniqueness is (value,entityId)
        // false: tree uniqueness is (value) <-- i.e. more strict
        boolean compareIds = descriptor.type() == GENERAL;
        additionsWorkSync = new WorkSync<>( new IndexUpdateApply<>( tree, treeKey, treeValue, new ConflictDetectingValueMerger<>( compareIds ) ) );

        // for updates we have to have uniqueness on (value,entityId) to allow for intermediary violating updates.
        // there are added conflict checks after updates have been applied.
        updatesWorkSync = new WorkSync<>( new IndexUpdateApply<>( tree, treeKey, treeValue, new ConflictDetectingValueMerger<>( true ) ) );
    }

    @Override
    public synchronized void drop()
    {
        try
        {
            closeTree();
            deleteFileIfPresent( fileSystem, storeFile );
        }
        finally
        {
            dropped = true;
            closed = true;
        }
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException
    {
        applyWithWorkSync( additionsWorkSync, updates );
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor )
    {
        // No-op, uniqueness is checked for each update in add(IndexEntryUpdate)
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
    {
        IndexUpdater updater = new IndexUpdater()
        {
            private boolean closed;
            private final Collection<IndexEntryUpdate<?>> updates = new ArrayList<>();

            @Override
            public void process( IndexEntryUpdate<?> update )
            {
                assertOpen();
                updates.add( update );
            }

            @Override
            public void close() throws IndexEntryConflictException
            {
                applyWithWorkSync( updatesWorkSync, updates );
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

        if ( descriptor.type() == UNIQUE )
        {
            // The index population detects conflicts on the fly, however for updates coming in we're in a position
            // where we cannot detect conflicts while applying, but instead afterwards.
            updater = new DeferredConflictCheckingIndexUpdater( updater, this::newReader, descriptor );
        }
        return updater;
    }

    abstract IndexReader newReader();

    @Override
    public synchronized void close( boolean populationCompletedSuccessfully )
    {
        if ( populationCompletedSuccessfully && failureBytes != null )
        {
            throw new IllegalStateException( "Can't mark index as online after it has been marked as failure" );
        }

        try
        {
            if ( populationCompletedSuccessfully )
            {
                assertPopulatorOpen();
                markTreeAsOnline();
            }
            else
            {
                assertNotDropped();
                ensureTreeInstantiated();
                markTreeAsFailed();
            }
        }
        finally
        {
            closeTree();
            closed = true;
        }
    }

    private void applyWithWorkSync( WorkSync<IndexUpdateApply<KEY,VALUE>,IndexUpdateWork<KEY,VALUE>> workSync,
            Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException
    {
        try
        {
            workSync.apply( new IndexUpdateWork<>( updates ) );
        }
        catch ( ExecutionException e )
        {
            Throwable cause = e.getCause();
            if ( cause instanceof IOException )
            {
                throw new UncheckedIOException( (IOException) cause );
            }
            if ( cause instanceof IndexEntryConflictException )
            {
                throw (IndexEntryConflictException) cause;
            }
            throw new RuntimeException( cause );
        }
    }

    private void assertNotDropped()
    {
        if ( dropped )
        {
            throw new IllegalStateException( "Populator has already been dropped." );
        }
    }

    private void assertNotClosed()
    {
        if ( closed )
        {
            throw new IllegalStateException( "Populator has already been closed." );
        }
    }

    @Override
    public void markAsFailed( String failure )
    {
        failureBytes = failure.getBytes( StandardCharsets.UTF_8 );
    }

    private void ensureTreeInstantiated()
    {
        if ( tree == null )
        {
            instantiateTree( RecoveryCleanupWorkCollector.ignore(), NO_HEADER_WRITER );
        }
    }

    private void assertPopulatorOpen()
    {
        if ( tree == null )
        {
            throw new IllegalStateException( "Populator has already been closed." );
        }
    }

    private void markTreeAsFailed()
    {
        if ( failureBytes == null )
        {
            failureBytes = ArrayUtils.EMPTY_BYTE_ARRAY;
        }
        tree.checkpoint( IOLimiter.UNLIMITED, new FailureHeaderWriter( failureBytes ) );
    }

    void markTreeAsOnline()
    {
        tree.checkpoint( IOLimiter.UNLIMITED, pc -> pc.putByte( BYTE_ONLINE ) );
    }

    static class IndexUpdateApply<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
    {
        private final GBPTree<KEY,VALUE> tree;
        private final KEY treeKey;
        private final VALUE treeValue;
        private final ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger;

        IndexUpdateApply( GBPTree<KEY,VALUE> tree, KEY treeKey, VALUE treeValue, ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger )
        {
            this.tree = tree;
            this.treeKey = treeKey;
            this.treeValue = treeValue;
            this.conflictDetectingValueMerger = conflictDetectingValueMerger;
        }

        void process( Iterable<? extends IndexEntryUpdate<?>> indexEntryUpdates ) throws Exception
        {
            try ( Writer<KEY,VALUE> writer = tree.writer() )
            {
                for ( IndexEntryUpdate<?> indexEntryUpdate : indexEntryUpdates )
                {
                    NativeIndexUpdater.processUpdate( treeKey, treeValue, indexEntryUpdate, writer, conflictDetectingValueMerger );
                }
            }
        }
    }

    static class IndexUpdateWork<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
            implements Work<IndexUpdateApply<KEY,VALUE>,IndexUpdateWork<KEY,VALUE>>
    {
        private final Collection<? extends IndexEntryUpdate<?>> updates;

        IndexUpdateWork( Collection<? extends IndexEntryUpdate<?>> updates )
        {
            this.updates = updates;
        }

        @Override
        public IndexUpdateWork<KEY,VALUE> combine( IndexUpdateWork<KEY,VALUE> work )
        {
            ArrayList<IndexEntryUpdate<?>> combined = new ArrayList<>( updates );
            combined.addAll( work.updates );
            return new IndexUpdateWork<>( combined );
        }

        @Override
        public void apply( IndexUpdateApply<KEY,VALUE> indexUpdateApply ) throws Exception
        {
            indexUpdateApply.process( updates );
        }
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        switch ( descriptor.type() )
        {
        case GENERAL:
            // Don't do anything here, we'll do a scan in the end instead
            break;
        case UNIQUE:
            updateUniqueSample( update );
            break;
        default:
            throw new IllegalArgumentException( "Unexpected index type " + descriptor.type() );
        }
    }

    private void updateUniqueSample( IndexEntryUpdate<?> update )
    {
        switch ( update.updateMode() )
        {
        case ADDED:
            uniqueSampler.increment( 1 );
            break;
        case REMOVED:
            uniqueSampler.increment( -1 );
            break;
        case CHANGED:
            break;
        default:
            throw new IllegalArgumentException( "Unsupported update mode type:" + update.updateMode() );
        }
    }

    @Override
    public IndexSample sampleResult()
    {
        switch ( descriptor.type() )
        {
        case GENERAL:
            return new FullScanNonUniqueIndexSampler<>( tree, layout ).result();
        case UNIQUE:
            return uniqueSampler.result();
        default:
            throw new IllegalArgumentException( "Unexpected index type " + descriptor.type() );
        }
    }

    private static void deleteFileIfPresent( FileSystemAbstraction fs, File storeFile )
    {
        try
        {
            fs.deleteFileOrThrow( storeFile );
        }
        catch ( NoSuchFileException e )
        {
            // File does not exist, we don't need to delete
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
