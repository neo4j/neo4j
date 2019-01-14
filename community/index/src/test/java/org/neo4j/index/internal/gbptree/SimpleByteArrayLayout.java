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
package org.neo4j.index.internal.gbptree;

import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.PageCursor;

public class SimpleByteArrayLayout extends TestLayout<RawBytes,RawBytes>
{
    private final boolean useFirstLongAsSeed;

    SimpleByteArrayLayout()
    {
        this( true );
    }

    SimpleByteArrayLayout( boolean useFirstLongAsSeed )
    {
        this.useFirstLongAsSeed = useFirstLongAsSeed;
    }

    @Override
    public RawBytes newKey()
    {
        return new RawBytes();
    }

    @Override
    public RawBytes copyKey( RawBytes rawBytes, RawBytes into )
    {
        into.bytes = rawBytes.bytes.clone();
        return into;
    }

    @Override
    public RawBytes newValue()
    {
        return new RawBytes();
    }

    @Override
    public int keySize( RawBytes rawBytes )
    {
        if ( rawBytes == null )
        {
            return -1;
        }
        return rawBytes.bytes.length;
    }

    @Override
    public int valueSize( RawBytes rawBytes )
    {
        if ( rawBytes == null )
        {
            return -1;
        }
        return rawBytes.bytes.length;
    }

    @Override
    public void writeKey( PageCursor cursor, RawBytes rawBytes )
    {
        cursor.putBytes( rawBytes.bytes );
    }

    @Override
    public void writeValue( PageCursor cursor, RawBytes rawBytes )
    {
        cursor.putBytes( rawBytes.bytes );
    }

    @Override
    public void readKey( PageCursor cursor, RawBytes into, int keySize )
    {
        into.bytes = new byte[keySize];
        cursor.getBytes( into.bytes );
    }

    @Override
    public void readValue( PageCursor cursor, RawBytes into, int valueSize )
    {
        into.bytes = new byte[valueSize];
        cursor.getBytes( into.bytes );
    }

    @Override
    public boolean fixedSize()
    {
        return false;
    }

    @Override
    public long identifier()
    {
        return 666;
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
    public int compare( RawBytes o1, RawBytes o2 )
    {
        if ( o1.bytes == null )
        {
            return -1;
        }
        if ( o2.bytes == null )
        {
            return 1;
        }
        if ( useFirstLongAsSeed )
        {
            int compare = Long.compare( keySeed( o1 ), keySeed( o2 ) );
            return compare != 0 ? compare : byteArrayCompare( o1.bytes, o2.bytes, Long.BYTES );
        }
        else
        {
            return byteArrayCompare( o1.bytes, o2.bytes, 0 );
        }
    }

    @Override
    int compareValue( RawBytes v1, RawBytes v2 )
    {
        return compare( v1, v2 );
    }

    private int byteArrayCompare( byte[] a, byte[] b, int fromPos )
    {
        assert a != null && b != null : "Null arrays not supported.";

        if ( a == b )
        {
            return 0;
        }

        int length = Math.min( a.length, b.length );
        for ( int i = fromPos; i < length; i++ )
        {
            int compare = Byte.compare( a[i], b[i] );
            if ( compare != 0 )
            {
                return compare;
            }
        }

        return Integer.compare( a.length, b.length );
    }

    @Override
    public RawBytes key( long seed )
    {
        RawBytes key = newKey();
        key.bytes = fromSeed( seed );
        return key;
    }

    @Override
    public RawBytes value( long seed )
    {
        RawBytes value = newValue();
        value.bytes = fromSeed( seed );
        return value;
    }

    @Override
    public long keySeed( RawBytes rawBytes )
    {
        return toSeed( rawBytes );
    }

    @Override
    public long valueSeed( RawBytes rawBytes )
    {
        return toSeed( rawBytes );
    }

    private long toSeed( RawBytes rawBytes )
    {
        ByteBuffer buffer = ByteBuffer.allocate( Long.BYTES );
        // Because keySearch is done inside the same shouldRetry block as keyCount()
        // We risk reading crap data. This is not a problem because we will retry
        // but buffer will throw here if we don't take that into consideration.
        byte[] bytes = rawBytes.bytes;
        if ( bytes.length >= Long.BYTES )
        {
            buffer.put( bytes, 0, Long.BYTES );
            buffer.flip();
            return buffer.getLong();
        }
        return 0;
    }

    private byte[] fromSeed( long seed )
    {
        int tail = (int) Math.abs( seed % Long.BYTES );
        ByteBuffer buffer = ByteBuffer.allocate( Long.BYTES + tail );
        buffer.putLong( seed );
        buffer.put( new byte[tail] );
        return buffer.array();
    }
}
