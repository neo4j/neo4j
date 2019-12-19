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
import java.io.UncheckedIOException;
import java.util.function.Consumer;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.TreeInconsistencyException;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.NodePropertyAccessor;

import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_ONLINE;

public abstract class NativeIndexAccessor<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> extends NativeIndex<KEY,VALUE>
        implements IndexAccessor
{
    private final NativeIndexUpdater<KEY,VALUE> singleUpdater;
    final NativeIndexHeaderWriter headerWriter;

    NativeIndexAccessor( DatabaseIndexContext databaseIndexContext, IndexFiles indexFiles, IndexLayout<KEY,VALUE> layout,
            IndexDescriptor descriptor, Consumer<PageCursor> additionalHeaderWriter )
    {
        super( databaseIndexContext, layout, indexFiles, descriptor, GBPTree.NO_MONITOR );
        singleUpdater = new NativeIndexUpdater<>( layout.newKey(), layout.newValue() );
        headerWriter = new NativeIndexHeaderWriter( BYTE_ONLINE, additionalHeaderWriter );
    }

    @Override
    public void drop()
    {
        closeTree();
        indexFiles.clear();
    }

    @Override
    public NativeIndexUpdater<KEY, VALUE> newUpdater( IndexUpdateMode mode )
    {
        assertOpen();
        try
        {
            return singleUpdater.initialize( tree.writer( TRACER_SUPPLIER.get() ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void force( IOLimiter ioLimiter )
    {
        tree.checkpoint( ioLimiter, TRACER_SUPPLIER.get() );
    }

    @Override
    public void refresh()
    {
        // not required in this implementation
    }

    @Override
    public void close()
    {
        closeTree();
    }

    @Override
    public abstract IndexReader newReader();

    @Override
    public BoundedIterable<Long> newAllEntriesReader( long fromIdInclusive, long toIdExclusive )
    {
        return new NativeAllEntriesReader<>( tree, layout, fromIdInclusive, toIdExclusive );
    }

    @Override
    public ResourceIterator<File> snapshotFiles()
    {
        return asResourceIterator( iterator( indexFiles.getStoreFile() ) );
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor )
    {   // Not needed since uniqueness is verified automatically w/o cost for every update.
    }

    @Override
    public long estimateNumberOfEntries()
    {
        try
        {
            return tree.estimateNumberOfEntriesInTree( TRACER_SUPPLIER.get() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        catch ( TreeInconsistencyException e )
        {
            return UNKNOWN_NUMBER_OF_ENTRIES;
        }
    }
}
