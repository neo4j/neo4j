/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.muninn;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

class UnsafeUtil
{
    private final static Unsafe unsafe;

    static
    {
        Unsafe theUnsafe = null;
        try
        {
            Field unsafeField = Unsafe.class.getDeclaredField( "theUnsafe" );
            unsafeField.setAccessible( true );
            theUnsafe = (Unsafe) unsafeField.get( null );
        }
        catch ( NoSuchFieldException | IllegalAccessException e )
        {
            e.printStackTrace();
        }
        unsafe = theUnsafe;
    }

    public static long getFieldOffset( Class<?> type, String field )
    {
        try
        {
            return unsafe.objectFieldOffset( type.getDeclaredField( field ) );
        }
        catch ( NoSuchFieldException e )
        {
            throw new Error( e );
        }
    }

    public static int getAndAddInt( Object obj, long offset, int delta )
    {
        return unsafe.getAndAddInt( obj, offset, delta );
    }

    public static boolean compareAndSwapLong( Object obj, long offset, long expected, long update )
    {
        return unsafe.compareAndSwapLong( obj, offset, expected, update );
    }

    public static long malloc( long sizeInBytes )
    {
        long pointer = unsafe.allocateMemory( sizeInBytes );
        unsafe.setMemory( pointer, sizeInBytes, (byte) 0 );
        return pointer;
    }

    public static void free( long pointer )
    {
        unsafe.freeMemory( pointer );
    }

    public static byte getByte( long address )
    {
        return unsafe.getByte( address );
    }

    public static void putByte( long address, byte value )
    {
        unsafe.putByte( address, value );
    }

    public static long getLong( long address )
    {
        return unsafe.getLong( address );
    }

    public static void putLong( long address, long value )
    {
        unsafe.putLong( address, value );
    }

    public static int getInt( long address )
    {
        return unsafe.getInt( address );
    }

    public static void putInt( long address, int value )
    {
        unsafe.putInt( address, value );
    }

    public static short getShort( long address )
    {
        return unsafe.getShort( address );
    }

    public static void putShort( long address, short value )
    {
        unsafe.putShort( address, value );
    }

    public static void putOrderedInt( Object obj, int address, int value )
    {
        unsafe.putOrderedInt( obj, address, value );
    }

    public static boolean hasUnsafe()
    {
        return unsafe != null;
    }
}
