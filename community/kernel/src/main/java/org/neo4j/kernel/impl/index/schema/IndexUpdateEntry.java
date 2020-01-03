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

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.api.index.UpdateMode;

public class IndexUpdateEntry
{
    private IndexUpdateEntry()
    {
        // Static utility class
    }

    public static <KEY, VALUE> void read( PageCursor cursor, Layout<KEY,VALUE> layout, UpdateMode updateMode, KEY key1, KEY key2, VALUE value )
    {
        switch ( updateMode )
        {
        case ADDED:
            BlockEntry.read( cursor, layout, key1, value );
            break;
        case REMOVED:
            BlockEntry.read( cursor, layout, key1 );
            break;
        case CHANGED:
            BlockEntry.read( cursor, layout, key1 );
            BlockEntry.read( cursor, layout, key2, value );
            break;
        default:
            throw new IllegalArgumentException( "Unknown update mode " + updateMode );
        }
    }

    public static <KEY,VALUE> void write( PageCursor cursor, Layout<KEY,VALUE> layout, UpdateMode updateMode, KEY key1, KEY key2, VALUE value )
    {
        switch ( updateMode )
        {
        case ADDED:
            BlockEntry.write( cursor, layout, key1, value );
            break;
        case REMOVED:
            BlockEntry.write( cursor, layout, key1 );
            break;
        case CHANGED:
            BlockEntry.write( cursor, layout, key1 );
            BlockEntry.write( cursor, layout, key2, value );
            break;
        default:
            throw new IllegalArgumentException( "Unknown update mode " + updateMode );
        }
    }
}
