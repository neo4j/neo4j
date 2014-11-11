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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

/**
 * Off-heap version of {@link LongArray} using {@code sun.misc.Unsafe}. Supports arrays with length beyond
 * Integer.MAX_VALUE.
 */
public class OffHeapLongArray implements LongArray
{
    private final long address;
    private final long length;
    private final long defaultValue;
    private long highestSetIndex = -1;
    private long size;

    public OffHeapLongArray( long length, long defaultValue )
    {
        this.length = length;
        this.defaultValue = defaultValue;
        this.address = unsafe.allocateMemory( length << 3 );
        clear();
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public long get( long index )
    {
        return unsafe.getLong( addressOf( index ) );
    }

    private long addressOf( long index )
    {
        if ( index < 0 || index >= length )
        {
            throw new ArrayIndexOutOfBoundsException( "Requested index " + index + ", but length is " + length );
        }
        return address + (index<<3);
    }

    @Override
    public void set( long index, long value )
    {
        long address = addressOf( index );
        if ( unsafe.getLong( address ) == defaultValue )
        {
            size++;
        }
        unsafe.putLong( address, value );
        if ( index > highestSetIndex )
        {
            highestSetIndex = index;
        }
    }

    @Override
    public long highestSetIndex()
    {
        return highestSetIndex;
    }

    @Override
    public long size()
    {
        return size;
    }

    @Override
    public void clear()
    {
        if ( isByteUniform( defaultValue ) )
        {
            unsafe.setMemory( address, length << 3, (byte)defaultValue );
        }
        else
        {
            for ( long i = 0, adr = address; i < length; i++, adr += 8 )
            {
                unsafe.putLong( adr, defaultValue );
            }
        }
        highestSetIndex = -1;
        size = 0;
    }

    private boolean isByteUniform( long value )
    {
        byte any = 0; // assignment not really needed
        for ( int i = 0; i < 8; i++ )
        {
            byte test = (byte)(value >>> 8*i);
            if ( i == 0 )
            {
                any = test;
            }
            else if ( test != any )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void swap( long fromIndex, long toIndex, int numberOfEntries )
    {
        long fromAddress = addressOf( fromIndex );
        long toAddress = addressOf( toIndex );

        for ( int i = 0; i < numberOfEntries; i++, fromAddress += 8, toAddress += 8 )
        {
            long fromValue = unsafe.getLong( fromAddress );
            long toValue = unsafe.getLong( toAddress );
            unsafe.putLong( fromAddress, toValue );
            unsafe.putLong( toAddress, fromValue );
        }
    }

    @Override
    public void visitMemoryStats( MemoryStatsVisitor visitor )
    {
        visitor.offHeapUsage( length*8 );
    }

    private static final Unsafe unsafe = getUnsafe();

    private static Unsafe getUnsafe()
    {
        try
        {
            Field singleoneInstanceField = Unsafe.class.getDeclaredField( "theUnsafe" );
            singleoneInstanceField.setAccessible( true );
            return (Unsafe) singleoneInstanceField.get( null );
        }
        catch ( Exception e )
        {
            throw new Error( e );
        }
    }
}
