/*
 * Copyright (c) "Neo4j"
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

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;

import static org.neo4j.kernel.impl.index.schema.NativeIndexUpdater.initializeKeyFromUpdate;

/**
 * Buffer {@link IndexEntryUpdate} by writing them out to a file. Can be read back in insert order through {@link #reader()}.
 */
public class IndexUpdateStorage<KEY extends NativeIndexKey<KEY>>
        extends SimpleEntryStorage<IndexEntryUpdate<?>,IndexUpdateCursor<KEY,NullValue>>
{
    private final IndexLayout<KEY> layout;
    private final KEY key1;
    private final KEY key2;
    private final NullValue value = NullValue.INSTANCE;

    IndexUpdateStorage( FileSystemAbstraction fs, Path file, ByteBufferFactory.Allocator byteBufferFactory, int blockSize, IndexLayout<KEY> layout,
            MemoryTracker memoryTracker )
    {
        super( fs, file, byteBufferFactory, blockSize, memoryTracker );
        this.layout = layout;
        this.key1 = layout.newKey();
        this.key2 = layout.newKey();
    }

    @Override
    public void add( IndexEntryUpdate<?> update, PageCursor pageCursor ) throws IOException
    {
        ValueIndexEntryUpdate<?> valueUpdate = (ValueIndexEntryUpdate<?>) update;
        int entrySize = TYPE_SIZE;
        UpdateMode updateMode = valueUpdate.updateMode();
        switch ( updateMode )
        {
        case ADDED:
            initializeKeyFromUpdate( key1, valueUpdate.getEntityId(), valueUpdate.values() );
            entrySize += BlockEntry.entrySize( layout, key1, value );
            break;
        case REMOVED:
            initializeKeyFromUpdate( key1, valueUpdate.getEntityId(), valueUpdate.values() );
            entrySize += BlockEntry.keySize( layout, key1 );
            break;
        case CHANGED:
            initializeKeyFromUpdate( key1, valueUpdate.getEntityId(), valueUpdate.beforeValues() );
            initializeKeyFromUpdate( key2, valueUpdate.getEntityId(), valueUpdate.values() );
            entrySize += BlockEntry.keySize( layout, key1 ) + BlockEntry.entrySize( layout, key2, value );
            break;
        default:
            throw new IllegalArgumentException( "Unknown update mode " + updateMode );
        }

        prepareWrite( entrySize );

        pageCursor.putByte( (byte) updateMode.ordinal() );
        IndexUpdateEntry.write( pageCursor, layout, updateMode, key1, key2, value );
    }

    @Override
    public IndexUpdateCursor<KEY,NullValue> reader( PageCursor pageCursor )
    {
        return new IndexUpdateCursor<>( pageCursor, layout );
    }
}
