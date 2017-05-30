/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.index.GBPTreeUtil;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.kernel.api.schema.OrderedPropertyValues.ofUndefined;

/**
 * {@link IndexPopulator} backed by a {@link GBPTree}.
 *
 * @param <KEY> type of {@link SchemaNumberKey}.
 * @param <VALUE> type of {@link SchemaNumberValue}.
 */
public abstract class NativeSchemaIndexPopulator<KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue>
        implements IndexPopulator
{
    static final byte BYTE_ONLINE = 1;
    static final byte BYTE_FAILED = 0;

    private final PageCache pageCache;
    private final File storeFile;
    private final KEY treeKey;
    private final VALUE treeValue;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final ConflictDetectingValueMerger<VALUE> conflictDetectingValueMerger;
    protected final Layout<KEY,VALUE> layout;

    private Writer<KEY,VALUE> singleWriter;
    private byte[] failureBytes;
    private boolean dropped;

    GBPTree<KEY,VALUE> tree;

    NativeSchemaIndexPopulator( PageCache pageCache, File storeFile, Layout<KEY,VALUE> layout,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        this.pageCache = pageCache;
        this.storeFile = storeFile;
        this.layout = layout;
        this.treeKey = layout.newKey();
        this.treeValue = layout.newValue();
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.conflictDetectingValueMerger = new ConflictDetectingValueMerger<>();
    }

    @Override
    public synchronized void create() throws IOException
    {
        GBPTreeUtil.deleteIfPresent( pageCache, storeFile );
        instantiateTree();
        instantiateWriter();
    }

    private void instantiateTree() throws IOException
    {
        tree = new GBPTree<>( pageCache, storeFile, layout, 0, NO_MONITOR, NO_HEADER,
                recoveryCleanupWorkCollector );
    }

    void instantiateWriter() throws IOException
    {
        assert singleWriter == null;
        singleWriter = tree.writer();
    }

    @Override
    public synchronized void drop() throws IOException
    {
        try
        {
            closeWriter();
            closeTree();
            GBPTreeUtil.deleteIfPresent( pageCache, storeFile );
        }
        finally
        {
            dropped = true;
        }
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
    {
        for ( IndexEntryUpdate<?> update : updates )
        {
            add( update );
        }
    }

    @Override
    public void add( IndexEntryUpdate<?> update ) throws IndexEntryConflictException, IOException
    {
        treeKey.from( update.getEntityId(), update.values() );
        treeValue.from( update.getEntityId(), update.values() );
        singleWriter.merge( treeKey, treeValue, conflictDetectingValueMerger );
        if ( conflictDetectingValueMerger.wasConflict() )
        {
            long existingNodeId = conflictDetectingValueMerger.existingNodeId();
            long addedNodeId = conflictDetectingValueMerger.addedNodeId();
            // TODO: not sure about the OrderedPropertyValues#ofUndefined bit
            throw new IndexEntryConflictException( existingNodeId, addedNodeId, ofUndefined( update.values() ) );
        }
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
        return new NativeSchemaIndexUpdater();
    }

    @Override
    public synchronized void close( boolean populationCompletedSuccessfully ) throws IOException
    {
        try
        {
            closeWriter();
            if ( populationCompletedSuccessfully && failureBytes != null )
            {
                throw new IllegalStateException( "Can't mark index as online after it has been marked as failure" );
            }
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
            instantiateTree();
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
            failureBytes = "".getBytes();
        }
        tree.checkpoint( IOLimiter.unlimited(), new FailureHeaderWriter( failureBytes ) );
    }

    private void markTreeAsOnline() throws IOException
    {
        tree.checkpoint( IOLimiter.unlimited(), ( pc ) -> pc.putByte( BYTE_ONLINE ) );
    }

    private <T extends Closeable> T closeIfPresent( T closeable ) throws IOException
    {
        if ( closeable != null )
        {
            closeable.close();
        }
        return null;
    }

    void closeWriter() throws IOException
    {
        singleWriter = closeIfPresent( singleWriter );
    }

    private void closeTree() throws IOException
    {
        tree = closeIfPresent( tree );
    }

    private class NativeSchemaIndexUpdater implements IndexUpdater
    {
        private boolean closed;

        @Override
        public void process( IndexEntryUpdate update ) throws IOException, IndexEntryConflictException
        {
            if ( closed )
            {
                throw new IllegalStateException( "Index updater has been closed." );
            }
            add( update );
        }

        @Override
        public void remove( PrimitiveLongSet nodeIds ) throws IOException
        {
            throw new UnsupportedOperationException( "Implement me" );
        }

        @Override
        public void close() throws IOException, IndexEntryConflictException
        {
            closed = true;
        }
    }
}
