/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.Page;

/** A page backed by a simple byte buffer. */
public class ByteBufferPage implements Page
{
    private static final MethodHandle addressOfMH = addressOfMH();

    private static MethodHandle addressOfMH()
    {
        try
        {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Field addressField = Buffer.class.getDeclaredField( "address" );
            addressField.setAccessible( true );
            return lookup.unreflectGetter( addressField );
        }
        catch ( Exception e )
        {
            throw new AssertionError( e );
        }
    }

    private static long addressOf( Buffer buffer )
    {
        try
        {
            return (long) addressOfMH.invokeExact( buffer );
        }
        catch ( Throwable throwable )
        {
            throw new AssertionError( throwable );
        }
    }

    protected ByteBuffer buffer;

    public ByteBufferPage( ByteBuffer buffer )
    {
        assert addressOf( buffer ) != 0:
                "Probably not a DirectByteBuffer: " + buffer + " (address = " + addressOf( buffer ) + ")";
        this.buffer = buffer;
    }

    public byte getByte( int offset )
    {
        return buffer.get( offset );
    }

    public long getLong( int offset )
    {
        return buffer.getLong( offset );
    }

    public void putLong( long value, int offset )
    {
        buffer.putLong( offset, value );
    }

    public int getInt( int offset )
    {
        return buffer.getInt(offset);
    }

    public void putInt( int value, int offset )
    {
        buffer.putInt( offset, value );
    }

    public void getBytes( byte[] data, int pageOffset, int arrayOffset, int length )
    {
        for (int i = 0; i < length; i++)
        {
            data[arrayOffset + i] = getByte( pageOffset + i );
        }
    }

    public void putBytes( byte[] data, int pageOffset, int arrayOffset, int length )
    {
        for (int i = 0; i < length; i++)
        {
            putByte( data[arrayOffset + i], pageOffset + i );
        }
    }

    public void putByte( byte value, int offset )
    {
        buffer.put( offset, value );
    }

    public short getShort( int offset )
    {
        return buffer.getShort( offset );
    }

    public void putShort( short value, int offset )
    {
        buffer.putShort( offset, value );
    }

    @Override
    public int size()
    {
        return buffer.capacity();
    }

    @Override
    public long address()
    {
        return addressOf( buffer );
    }
}
