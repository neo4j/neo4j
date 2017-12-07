/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index.internal.gbptree;

import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.PageCursor;

public class SimpleByteArrayLayout extends TestLayout<RawBytes,RawBytes>
{
    @Override
    public RawBytes newKey()
    {
        return new RawBytes();
    }

    @Override
    public RawBytes copyKey( RawBytes rawBytes, RawBytes into )
    {
        byte[] src = rawBytes.bytes;
        byte[] target = new byte[src.length];
        System.arraycopy( src, 0, target, 0, src.length );
        into.bytes = target;
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
        int compare = Long.compare( getSeed( o1 ), getSeed( o2 ) );
        return compare != 0 ? compare : byteArrayCompare( o1.bytes, o2.bytes, Long.BYTES );
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

        if ( a.length < b.length )
        {
            return -1;
        }

        if ( a.length > b.length )
        {
            return 1;
        }
        return 0;
    }

    @Override
    public RawBytes key( long seed )
    {
        RawBytes key = newKey();
        key.bytes = ByteBuffer.allocate( 16 ).putLong( seed ).putLong( seed ).array();
        return key;
    }

    @Override
    public RawBytes value( long seed )
    {
        RawBytes value = newValue();
        value.bytes = ByteBuffer.allocate( 17 ).putLong( seed ).putLong( seed ).array();
        return value;
    }

    @Override
    public long getSeed( RawBytes rawBytes )
    {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put( rawBytes.bytes, 0, Long.BYTES );
        buffer.flip();
        return buffer.getLong();
    }
}
