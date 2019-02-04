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

import java.io.Closeable;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

public class BlockReader<KEY,VALUE> implements Closeable
{
    private final long entryCount;
    private final PageCursor pageCursor;
    private final KEY key;
    private final VALUE value;
    private final Layout<KEY,VALUE> layout;
    private long readEntries;

    BlockReader( PageCursor pageCursor, Layout<KEY,VALUE> layout )
    {
        this.pageCursor = pageCursor;
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

    public long entryCount()
    {
        return entryCount;
    }

    KEY key()
    {
        return key;
    }

    VALUE value()
    {
        return value;
    }

    @Override
    public void close() throws IOException
    {
        pageCursor.close();
    }
}
