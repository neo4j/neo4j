/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.index.btree;

import org.neo4j.io.pagecache.PageCursor;

public class PathIndexLayout implements TreeItemLayout<TwoLongs,TwoLongs>
{
    private static final int SIZE_KEY = 2 * Long.BYTES;
    private static final int SIZE_VALUE = 2 * Long.BYTES;

    @Override
    public int compare( TwoLongs o1, TwoLongs o2 )
    {
        int compareId = Long.compare( o1.first, o2.first );
        return compareId != 0 ? compareId : Long.compare( o1.other, o2.other );
    }

    @Override
    public TwoLongs newKey()
    {
        return new TwoLongs();
    }

    @Override
    public TwoLongs newValue()
    {
        return new TwoLongs();
    }

    @Override
    public int keySize()
    {
        return SIZE_KEY;
    }

    @Override
    public int valueSize()
    {
        return SIZE_VALUE;
    }

    @Override
    public void writeKey( PageCursor cursor, TwoLongs key )
    {
        cursor.putLong( key.first );
        cursor.putLong( key.other );
    }

    @Override
    public void writeValue( PageCursor cursor, TwoLongs value )
    {
        cursor.putLong( value.first );
        cursor.putLong( value.other );
    }

    @Override
    public void readKey( PageCursor cursor, TwoLongs into )
    {
        into.first = cursor.getLong();
        into.other = cursor.getLong();
    }

    @Override
    public void readValue( PageCursor cursor, TwoLongs into )
    {
        into.first = cursor.getLong();
        into.other = cursor.getLong();
    }
}
