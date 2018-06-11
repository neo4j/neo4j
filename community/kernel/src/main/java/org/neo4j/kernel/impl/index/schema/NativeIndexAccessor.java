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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;

import static org.neo4j.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;

public abstract class NativeIndexAccessor<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> extends NativeIndex<KEY,VALUE>
        implements IndexAccessor
{
    private final NativeIndexUpdater<KEY,VALUE> singleUpdater;
    final IndexSamplingConfig samplingConfig;

    NativeIndexAccessor( PageCache pageCache, FileSystemAbstraction fs, File storeFile, Layout<KEY,VALUE> layout,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, IndexProvider.Monitor monitor, StoreIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig ) throws IOException
    {
        super( pageCache, fs, storeFile, layout, monitor, descriptor );
        singleUpdater = new NativeIndexUpdater<>( layout.newKey(), layout.newValue() );
        this.samplingConfig = samplingConfig;
        instantiateTree( recoveryCleanupWorkCollector, NO_HEADER_WRITER );
    }

    @Override
    public void drop() throws IOException
    {
        closeTree();
        fileSystem.deleteFileOrThrow( storeFile );
    }

    @Override
    public NativeIndexUpdater<KEY, VALUE> newUpdater( IndexUpdateMode mode )
    {
        assertOpen();
        try
        {
            return singleUpdater.initialize( tree.writer() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void force( IOLimiter ioLimiter ) throws IOException
    {
        tree.checkpoint( ioLimiter );
    }

    @Override
    public void refresh()
    {
        // not required in this implementation
    }

    @Override
    public void close() throws IOException
    {
        closeTree();
    }

    @Override
    public boolean isDirty()
    {
        return tree.wasDirtyOnStartup();
    }

    @Override
    public abstract IndexReader newReader();

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        return new NativeAllEntriesReader<>( tree, layout );
    }

    @Override
    public ResourceIterator<File> snapshotFiles()
    {
        return asResourceIterator( iterator( storeFile ) );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
    {   // Not needed since uniqueness is verified automatically w/o cost for every update.
    }
}
