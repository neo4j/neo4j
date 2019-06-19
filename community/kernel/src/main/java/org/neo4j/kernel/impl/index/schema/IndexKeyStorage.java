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

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;

class IndexKeyStorage<KEY extends NativeIndexKey<KEY>> extends SimpleEntryStorage<KEY,IndexKeyStorage.KeyEntryCursor<KEY>>
{
    private static final byte KEY_TYPE = 1;
    private final Layout<KEY,?> layout;

    IndexKeyStorage( FileSystemAbstraction fs, File file, ByteBufferFactory.Allocator byteBufferFactory, int blockSize, Layout<KEY,?> layout )
            throws IOException
    {
        super( fs, file, byteBufferFactory, blockSize );
        this.layout = layout;
    }

    @Override
    void add( KEY key, PageCursor pageCursor ) throws IOException
    {
        int entrySize = TYPE_SIZE + BlockEntry.keySize( layout, key );
        prepareWrite( entrySize );
        pageCursor.putByte( KEY_TYPE );
        BlockEntry.write( pageCursor, layout, key );
    }

    @Override
    KeyEntryCursor<KEY> reader( PageCursor pageCursor )
    {
        return new KeyEntryCursor<>( pageCursor, layout );
    }

    static class KeyEntryCursor<KEY> implements BlockEntryCursor<KEY,Void>
    {
        private final PageCursor pageCursor;
        private final Layout<KEY,?> layout;
        private final KEY key;

        KeyEntryCursor( PageCursor pageCursor, Layout<KEY,?> layout )
        {
            this.pageCursor = pageCursor;
            this.layout = layout;
            this.key = layout.newKey();
        }

        @Override
        public boolean next() throws IOException
        {
            byte type = pageCursor.getByte();
            if ( type == STOP_TYPE )
            {
                return false;
            }
            if ( type != KEY_TYPE )
            {
                throw new RuntimeException( format( "Unexpected entry type. Expected %d or %d, but was %d.", STOP_TYPE, KEY_TYPE, type ) );
            }
            BlockEntry.read( pageCursor, layout, key );
            return true;
        }

        @Override
        public KEY key()
        {
            return key;
        }

        @Override
        public Void value()
        {
            return null;
        }

        @Override
        public void close() throws IOException
        {
            pageCursor.close();
        }
    }
}
