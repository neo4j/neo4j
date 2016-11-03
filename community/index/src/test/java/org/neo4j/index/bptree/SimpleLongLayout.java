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
package org.neo4j.index.bptree;

import org.apache.commons.lang3.mutable.MutableLong;

import org.neo4j.io.pagecache.PageCursor;

public class SimpleLongLayout implements Layout<MutableLong,MutableLong>
{
    @Override
    public int compare( MutableLong o1, MutableLong o2 )
    {
        return Long.compare( o1.longValue(), o2.longValue() );
    }

    @Override
    public MutableLong newKey()
    {
        return new MutableLong();
    }

    @Override
    public MutableLong minKey( MutableLong into )
    {
        into.setValue( Long.MIN_VALUE );
        return into;
    }

    @Override
    public MutableLong maxKey( MutableLong into )
    {
        into.setValue( Long.MAX_VALUE );
        return into;
    }

    @Override
    public void copyKey( MutableLong key, MutableLong into )
    {
        into.setValue( key.longValue() );
    }

    @Override
    public MutableLong newValue()
    {
        return new MutableLong();
    }

    @Override
    public int keySize()
    {
        return Long.BYTES;
    }

    @Override
    public int valueSize()
    {
        return Long.BYTES;
    }

    @Override
    public void writeKey( PageCursor cursor, MutableLong key )
    {
        cursor.putLong( key.longValue() );
    }

    @Override
    public void writeValue( PageCursor cursor, MutableLong value )
    {
        cursor.putLong( value.longValue() );
    }

    @Override
    public void readKey( PageCursor cursor, MutableLong into )
    {
        into.setValue( cursor.getLong() );
    }

    @Override
    public void readValue( PageCursor cursor, MutableLong into )
    {
        into.setValue( cursor.getLong() );
    }

    @Override
    public long identifier()
    {
        return 999;
    }

    @Override
    public int majorVersion()
    {
        return 0;
    }

    @Override
    public int minorVersion()
    {
        return 0;
    }

    @Override
    public void writeMetaData( PageCursor cursor )
    {
    }

    @Override
    public void readMetaData( PageCursor cursor )
    {
    }
}
