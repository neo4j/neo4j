/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.neo4j.concurrent.Work;
import org.neo4j.concurrent.WorkSync;
import org.neo4j.helpers.Exceptions;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Type.GENERAL;

/**
 * {@link IndexPopulator} backed by a {@link GBPTree}.
 *
 * @param <KEY> type of {@link SchemaNumberKey}.
 * @param <VALUE> type of {@link SchemaNumberValue}.
 */
public abstract class NativeSchemaNumberIndexPopulator<KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue>
        extends NativeSchemaNumberIndex<KEY,VALUE> implements IndexPopulator
{
    static final byte BYTE_FAILED = 0;
    static final byte BYTE_ONLINE = 1;
    static final byte BYTE_POPULATING = 2;

    private final KEY treeKey;
    private final VALUE treeValue;
    private WorkSync<IndexUpdateApply,IndexUpdateWork> additionsWorkSync;
    private WorkSync<IndexUpdateApply,IndexUpdateWork> updatesWorkSync;

    private byte[] failureBytes;
    private boolean dropped;

    NativeSchemaNumberIndexPopulator( PageCache pageCache, FileSystemAbstraction fs, File storeFile, Layout<KEY,VALUE> layout,
            SchemaIndexProvider.Monitor monitor, IndexDescriptor descriptor, long indexId )
    {
        super( pageCache, fs, storeFile, layout, monitor, descriptor, indexId );
        this.treeKey = layout.newKey();
        this.treeValue = layout.newValue();
    }

    @Override
    public synchronized void create() throws IOException
    {
        gbpTreeFileUtil.deleteFileIfPresent( storeFile );
        instantiateTree( RecoveryCleanupWorkCollector.IMMEDIATE, new NativeSchemaIndexHeaderWriter( BYTE_POPULATING ) );

        // true:  tree uniqueness is (value,entityId)
        // false: tree uniqueness is (value) <-- i.e. more strict
        boolean compareIds = descriptor.type() == GENERAL;
        additionsWorkSync = new WorkSync<>( new IndexUpdateApply( treeKey, treeValue, new ConflictDetectingValueMerger<>( compareIds ) ) );

        // for updates we have to have uniqueness on (value,entityId) to allow for intermediary violating updates.
        // there are added conflict checks after updates have been applied.
        updatesWorkSync = new WorkSync<>( new IndexUpdateApply( treeKey, treeValue, new ConflictDetectingValueMerger<>( true ) ) );
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
        }
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
    {
        applyWithWorkSync( additionsWorkSync, updates );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        // No-op, uniqueness is checked for each update in add(IndexEntryUpdate)
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor ) throws IOException
    {
        return new IndexUpdater()
        {
            private boolean closed;
            private final Collection<IndexEntryUpdate<?>> updates = new ArrayList<>();

            @Override
            public void process( IndexEntryUpdate<?> update ) throws IOException, IndexEntryConflictException
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
    }

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
        }
    }

    private void applyWithWorkSync( WorkSync<IndexUpdateApply,IndexUpdateWork> workSync,
            Collection<? extends IndexEntryUpdate<?>> updates ) throws IOException, IndexEntryConflictException
    {
        try
        {
            workSync.apply( new IndexUpdateWork( updates ) );
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
            throw Exceptions.launderedException( e );
        }
    }

    private void assertNotDropped()
    {
        if ( dropped )
        {
            throw new IllegalStateException( "Populator has already been dropped." );
        }
    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        failureBytes = failure.getBytes( StandardCharsets.UTF_8 );
    }

    private void ensureTreeInstantiated() throws IOException
    {
        if ( tree == null )
        {
            instantiateTree( RecoveryCleanupWorkCollector.IGNORE, NO_HEADER_WRITER );
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

    private void markTreeAsOnline() throws IOException
    {
        tree.checkpoint( IOLimiter.unlimited(), pc -> pc.putByte( BYTE_ONLINE ) );
    }

    private class IndexUpdateApply
    {
        private final KEY treeKey;
        private final VALUE treeValue;
        private final ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger;

        IndexUpdateApply( KEY treeKey, VALUE treeValue, ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger )
        {
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
                    NativeSchemaNumberIndexUpdater.processUpdate( treeKey, treeValue, indexEntryUpdate, writer, conflictDetectingValueMerger );
                }
            }
        }
    }

    private class IndexUpdateWork implements Work<IndexUpdateApply,IndexUpdateWork>
    {
        private final Collection<? extends IndexEntryUpdate<?>> updates;

        IndexUpdateWork( Collection<? extends IndexEntryUpdate<?>> updates )
        {
            this.updates = updates;
        }

        @Override
        public IndexUpdateWork combine( IndexUpdateWork work )
        {
            ArrayList<IndexEntryUpdate<?>> combined = new ArrayList<>( updates );
            combined.addAll( work.updates );
            return new IndexUpdateWork( combined );
        }

        @Override
        public void apply( IndexUpdateApply indexUpdateApply ) throws Exception
        {
            indexUpdateApply.process( updates );
        }
    }
}
