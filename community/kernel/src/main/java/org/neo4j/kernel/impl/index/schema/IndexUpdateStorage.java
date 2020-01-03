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
package org.neo4j.kernel.impl.index.schema;

import java.io.File;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.impl.api.index.UpdateMode;

import static org.neo4j.kernel.impl.index.schema.NativeIndexUpdater.initializeKeyAndValueFromUpdate;
import static org.neo4j.kernel.impl.index.schema.NativeIndexUpdater.initializeKeyFromUpdate;

/**
 * Buffer {@link IndexEntryUpdate} by writing them out to a file. Can be read back in insert order through {@link #reader()}.
 */
public class IndexUpdateStorage<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
        extends SimpleEntryStorage<IndexEntryUpdate<?>,IndexUpdateCursor<KEY,VALUE>>
{
    private final Layout<KEY,VALUE> layout;
    private final KEY key1;
    private final KEY key2;
    private final VALUE value;

    IndexUpdateStorage( FileSystemAbstraction fs, File file, ByteBufferFactory.Allocator byteBufferFactory, int blockSize, Layout<KEY,VALUE> layout )
            throws IOException
    {
        super( fs, file, byteBufferFactory, blockSize );
        this.layout = layout;
        this.key1 = layout.newKey();
        this.key2 = layout.newKey();
        this.value = layout.newValue();
    }

    @Override
    public void add( IndexEntryUpdate<?> update, PageCursor pageCursor ) throws IOException
    {
        int entrySize = TYPE_SIZE;
        UpdateMode updateMode = update.updateMode();
        switch ( updateMode )
        {
        case ADDED:
            initializeKeyAndValueFromUpdate( key1, value, update.getEntityId(), update.values() );
            entrySize += BlockEntry.entrySize( layout, key1, value );
            break;
        case REMOVED:
            initializeKeyFromUpdate( key1, update.getEntityId(), update.values() );
            entrySize += BlockEntry.keySize( layout, key1 );
            break;
        case CHANGED:
            initializeKeyFromUpdate( key1, update.getEntityId(), update.beforeValues() );
            initializeKeyAndValueFromUpdate( key2, value, update.getEntityId(), update.values() );
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
    public IndexUpdateCursor<KEY,VALUE> reader( PageCursor pageCursor )
    {
        return new IndexUpdateCursor<>( pageCursor, layout );
    }
}
