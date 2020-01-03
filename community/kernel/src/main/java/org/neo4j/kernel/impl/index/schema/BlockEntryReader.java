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

import java.io.IOException;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

/**
 * Reads {@link BlockEntry} from a block in sequential order. Key and value instances are handed out though {@link #key()} and {@link #value()} but are reused
 * internally so consumer need to either create a copy or finish all operations on key and value before progressing reader.
 * Reader will figure out when to stop reading based on Block header wish contains total size of this Block in bytes and total number of entries in Block.
 */
public class BlockEntryReader<KEY,VALUE> implements BlockEntryCursor<KEY,VALUE>
{
    private final long blockSize;
    private final long entryCount;
    private final PageCursor pageCursor;
    private final KEY key;
    private final VALUE value;
    private final Layout<KEY,VALUE> layout;
    private long readEntries;

    BlockEntryReader( PageCursor pageCursor, Layout<KEY,VALUE> layout )
    {
        this.pageCursor = pageCursor;
        this.blockSize = pageCursor.getLong();
        this.entryCount = pageCursor.getLong();
        this.layout = layout;
        this.key = layout.newKey();
        this.value = layout.newValue();
    }

    public boolean next() throws IOException
    {
        if ( readEntries >= entryCount )
        {
            return false;
        }
        BlockEntry.read( pageCursor, layout, key, value );
        readEntries++;
        return true;
    }

    public long blockSize()
    {
        return blockSize;
    }

    public long entryCount()
    {
        return entryCount;
    }

    public KEY key()
    {
        return key;
    }

    public VALUE value()
    {
        return value;
    }

    @Override
    public void close() throws IOException
    {
        pageCursor.close();
    }
}
