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
package org.neo4j.internal.counts;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

class CountsLayout extends Layout.Adapter<CountsKey,CountsValue>
{
    CountsLayout()
    {
        super( true, Layout.namedIdentifier( "CoLa", 987 ), 0, 1 );
    }

    @Override
    public CountsKey newKey()
    {
        return new CountsKey();
    }

    @Override
    public CountsKey copyKey( CountsKey key, CountsKey into )
    {
        into.type = key.type;
        into.first = key.first;
        into.second = key.second;
        return into;
    }

    @Override
    public CountsValue newValue()
    {
        return new CountsValue();
    }

    @Override
    public int keySize( CountsKey key )
    {
        return CountsKey.SIZE;
    }

    @Override
    public int valueSize( CountsValue value )
    {
        return CountsValue.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, CountsKey key )
    {
        cursor.putByte( key.type );
        cursor.putLong( key.first );
        cursor.putInt( key.second );
    }

    @Override
    public void writeValue( PageCursor cursor, CountsValue value )
    {
        cursor.putLong( value.count );
    }

    @Override
    public void readKey( PageCursor cursor, CountsKey into, int keySize )
    {
        into.type = cursor.getByte();
        into.first = cursor.getLong();
        into.second = cursor.getInt();
    }

    @Override
    public void readValue( PageCursor cursor, CountsValue into, int valueSize )
    {
        into.count = cursor.getLong();
    }

    @Override
    public int compare( CountsKey o1, CountsKey o2 )
    {
        int typeCompare = Byte.compare( o1.type, o2.type );
        if ( typeCompare != 0 )
        {
            return typeCompare;
        }
        int keyFirstCompare = Long.compare( o1.first, o2.first );
        if ( keyFirstCompare != 0 )
        {
            return keyFirstCompare;
        }
        return Long.compare( o1.second, o2.second );
    }
}
