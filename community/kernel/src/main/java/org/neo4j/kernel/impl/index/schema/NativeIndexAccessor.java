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
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

import static org.neo4j.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_ONLINE;

public abstract class NativeIndexAccessor<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> extends NativeIndex<KEY,VALUE>
        implements IndexAccessor
{
    private final NativeIndexUpdater<KEY,VALUE> singleUpdater;
    final NativeIndexHeaderWriter headerWriter;

    NativeIndexAccessor( PageCache pageCache, FileSystemAbstraction fs, File storeFile, IndexLayout<KEY,VALUE> layout,
            IndexProvider.Monitor monitor, StoreIndexDescriptor descriptor, Consumer<PageCursor> additionalHeaderWriter )
    {
        super( pageCache, fs, storeFile, layout, monitor, descriptor );
        singleUpdater = new NativeIndexUpdater<>( layout.newKey(), layout.newValue() );
        headerWriter = new NativeIndexHeaderWriter( BYTE_ONLINE, additionalHeaderWriter );
    }

    @Override
    public void drop()
    {
        closeTree();
        try
        {
            fileSystem.deleteFileOrThrow( storeFile );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
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
    public void force( IOLimiter ioLimiter )
    {
        tree.checkpoint( ioLimiter );
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
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
    {   // Not needed since uniqueness is verified automatically w/o cost for every update.
    }
}
