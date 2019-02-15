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

import java.io.IOException;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.api.index.UpdateMode;

import static org.neo4j.kernel.impl.index.schema.IndexUpdateStorage.STOP_TYPE;

/**
 * Cursor over serialized {@link org.neo4j.kernel.api.index.IndexEntryUpdate} represented by {@link UpdateMode}, 2x{@link KEY} and {@link VALUE}.
 * Reads the updates in sequential order. Field instances are reused, so consumer is responsible for creating copies if result needs to be cached.
 */
public class IndexUpdateCursor<KEY, VALUE> implements BlockEntryCursor<KEY,VALUE>
{
    private final PageCursor cursor;
    private final Layout<KEY,VALUE> layout;

    // Fields for the last entry
    private UpdateMode updateMode;
    private final KEY key1;
    private final KEY key2;
    private final VALUE value;

    IndexUpdateCursor( PageCursor cursor, Layout<KEY,VALUE> layout )
    {
        this.cursor = cursor;
        this.layout = layout;
        this.key1 = layout.newKey();
        this.key2 = layout.newKey();
        this.value = layout.newValue();
    }

    @Override
    public boolean next() throws IOException
    {
        byte updateModeType = cursor.getByte();
        if ( updateModeType == STOP_TYPE )
        {
            return false;
        }

        updateMode = UpdateMode.MODES[updateModeType];
        IndexUpdateEntry.read( cursor, layout, updateMode, key1, key2, value );
        return true;
    }

    @Override
    public KEY key()
    {
        return key1;
    }

    @Override
    public VALUE value()
    {
        return value;
    }

    public KEY key2()
    {
        return key2;
    }

    public UpdateMode updateMode()
    {
        return updateMode;
    }

    @Override
    public void close() throws IOException
    {
        cursor.close();
    }
}
