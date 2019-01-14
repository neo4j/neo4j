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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.neo4j.concurrent.Work;
import org.neo4j.concurrent.WorkSync;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
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
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.DefaultNonUniqueIndexSampler;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.NonUniqueIndexSampler;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor.Type.GENERAL;
import static org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor.Type.UNIQUE;

/**
 * {@link IndexPopulator} backed by a {@link GBPTree}.
 *
 * @param <KEY> type of {@link NativeSchemaKey}.
 * @param <VALUE> type of {@link NativeSchemaValue}.
 */
public abstract class NativeSchemaIndexPopulator<KEY extends NativeSchemaKey<KEY>, VALUE extends NativeSchemaValue>
        extends NativeSchemaIndex<KEY,VALUE> implements IndexPopulator
{
    public static final byte BYTE_FAILED = 0;
    static final byte BYTE_ONLINE = 1;
    static final byte BYTE_POPULATING = 2;

    private final KEY treeKey;
    private final VALUE treeValue;
    private final UniqueIndexSampler uniqueSampler;
    private final NonUniqueIndexSampler nonUniqueSampler;
    final IndexSamplingConfig samplingConfig;

    private WorkSync<IndexUpdateApply<KEY,VALUE>,IndexUpdateWork<KEY,VALUE>> additionsWorkSync;
    private WorkSync<IndexUpdateApply<KEY,VALUE>,IndexUpdateWork<KEY,VALUE>> updatesWorkSync;

    private byte[] failureBytes;
    private boolean dropped;
    private boolean closed;

    NativeSchemaIndexPopulator( PageCache pageCache, FileSystemAbstraction fs, File storeFile, Layout<KEY,VALUE> layout,
                                IndexProvider.Monitor monitor, SchemaIndexDescriptor descriptor, long indexId, IndexSamplingConfig samplingConfig )
    {
        super( pageCache, fs, storeFile, layout, monitor, descriptor, indexId );
        this.treeKey = layout.newKey();
        this.treeValue = layout.newValue();
        this.samplingConfig = samplingConfig;
        switch ( descriptor.type() )
        {
        case GENERAL:
            uniqueSampler = null;
            nonUniqueSampler = new DefaultNonUniqueIndexSampler( samplingConfig.sampleSizeLimit() );
            break;
        case UNIQUE:
            uniqueSampler = new UniqueIndexSampler();
            nonUniqueSampler = null;
            break;
        default:
            throw new IllegalArgumentException( "Unexpected index type " + descriptor.type() );
        }
    }

    public void clear() throws IOException
    {
        gbpTreeFileUtil.deleteFileIfPresent( storeFile );
    }

    @Override
    public synchronized void create() throws IOException
    {
        create( new NativeSchemaIndexHeaderWriter( BYTE_POPULATING ) );
    }

    protected synchronized void create( Consumer<PageCursor> headerWriter ) throws IOException
    {
        assertNotDropped();
        assertNotClosed();

        gbpTreeFileUtil.deleteFileIfPresent( storeFile );
        instantiateTree( RecoveryCleanupWorkCollector.immediate(), headerWriter );

        // true:  tree uniqueness is (value,entityId)
        // false: tree uniqueness is (value) <-- i.e. more strict
        additionsWorkSync = new WorkSync<>( new IndexUpdateApply<>( tree, treeKey, treeValue, getMainConflictDetector() ) );

        // for updates we have to have uniqueness on (value,entityId) to allow for intermediary violating updates.
        // there are added conflict checks after updates have been applied.
        updatesWorkSync = new WorkSync<>( new IndexUpdateApply<>( tree, treeKey, treeValue, new ConflictDetectingValueMerger<>( true ) ) );
    }

    ConflictDetectingValueMerger<KEY,VALUE> getMainConflictDetector()
    {
        return new ConflictDetectingValueMerger<>( descriptor.type() == GENERAL );
    }

    @Override
    public synchronized void drop() throws IOException
    {
        try
        {
            closeTree();
            gbpTreeFileUtil.deleteFileIfPresent( storeFile );
        }
        finally
        {
            dropped = true;
            closed = true;
        }
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IOException, IndexEntryConflictException
    {
        applyWithWorkSync( additionsWorkSync, updates );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor ) throws IndexEntryConflictException
    {
        // No-op, uniqueness is checked for each update in add(IndexEntryUpdate)
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor )
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
            public void close() throws IOException, IndexEntryConflictException
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

        if ( descriptor.type() == UNIQUE && canCheckConflictsWithoutStoreAccess() )
        {
            // The index population detects conflicts on the fly, however for updates coming in we're in a position
            // where we cannot detect conflicts while applying, but instead afterwards.
            updater = new DeferredConflictCheckingIndexUpdater( updater, this::newReader, descriptor );
        }
        return updater;
    }

    boolean canCheckConflictsWithoutStoreAccess()
    {
        return true;
    }

    abstract IndexReader newReader();

    @Override
    public synchronized void close( boolean populationCompletedSuccessfully ) throws IOException
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
            Collection<? extends IndexEntryUpdate<?>> updates ) throws IOException, IndexEntryConflictException
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
                throw (IOException) cause;
            }
            if ( cause instanceof IndexEntryConflictException )
            {
                throw (IndexEntryConflictException) cause;
            }
            throw new IOException( cause );
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

    private void ensureTreeInstantiated() throws IOException
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

    private void markTreeAsFailed() throws IOException
    {
        if ( failureBytes == null )
        {
            failureBytes = new byte[0];
        }
        tree.checkpoint( IOLimiter.unlimited(), new FailureHeaderWriter( failureBytes ) );
    }

    void markTreeAsOnline() throws IOException
    {
        tree.checkpoint( IOLimiter.unlimited(), pc -> pc.putByte( BYTE_ONLINE ) );
    }

    static class IndexUpdateApply<KEY extends NativeSchemaKey<KEY>, VALUE extends NativeSchemaValue>
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
                    NativeSchemaIndexUpdater.processUpdate( treeKey, treeValue, indexEntryUpdate, writer, conflictDetectingValueMerger );
                }
            }
        }
    }

    static class IndexUpdateWork<KEY extends NativeSchemaKey<KEY>, VALUE extends NativeSchemaValue>
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
            nonUniqueSampler.include( SamplingUtil.encodedStringValuesForSampling( (Object[]) update.values() ) );
            break;
        case UNIQUE:
            uniqueSampler.increment( 1 );
            break;
        default:
            throw new IllegalArgumentException( "Unexpected index type " + descriptor.type() );
        }
    }

    @Override
    public IndexSample sampleResult()
    {
        switch ( descriptor.type() )
        {
        case GENERAL:
            return nonUniqueSampler.result();
        case UNIQUE:
            return uniqueSampler.result();
        default:
            throw new IllegalArgumentException( "Unexpected index type " + descriptor.type() );
        }
    }
}
