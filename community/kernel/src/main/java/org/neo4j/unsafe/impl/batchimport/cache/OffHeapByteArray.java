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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

public class OffHeapByteArray extends OffHeapNumberArray<ByteArray> implements ByteArray
{
    private final byte[] defaultValue;

    protected OffHeapByteArray( long length, byte[] defaultValue, long base )
    {
        super( length, defaultValue.length, base );
        this.defaultValue = defaultValue;
        clear();
    }

    @Override
    public void swap( long fromIndex, long toIndex, int numberOfEntries )
    {
        int size = numberOfEntries * itemSize;
        long intermediary = UnsafeUtil.allocateMemory( size );
        UnsafeUtil.copyMemory( address( fromIndex, 0 ), intermediary, size );
        UnsafeUtil.copyMemory( address( toIndex, 0 ), address( fromIndex, 0 ), size );
        UnsafeUtil.copyMemory( intermediary, address( toIndex, 0 ), size );
        UnsafeUtil.free( intermediary );
    }

    @Override
    public void clear()
    {
        if ( isByteUniform( defaultValue ) )
        {
            UnsafeUtil.setMemory( address, length * itemSize, defaultValue[0] );
        }
        else
        {
            long intermediary = UnsafeUtil.allocateMemory( itemSize );
            for ( int i = 0; i < defaultValue.length; i++ )
            {
                UnsafeUtil.putByte( intermediary + i, defaultValue[i] );
            }

            for ( long i = 0, adr = address; i < length; i++, adr += itemSize )
            {
                UnsafeUtil.copyMemory( intermediary, adr, itemSize );
            }
            UnsafeUtil.free( intermediary );
        }
    }

    private boolean isByteUniform( byte[] bytes )
    {
        byte reference = bytes[0];
        for ( int i = 1; i < bytes.length; i++ )
        {
            if ( reference != bytes[i] )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void get( long index, byte[] into )
    {
        long address = address( index, 0 );
        for ( int i = 0; i < itemSize; i++, address++ )
        {
            into[i] = UnsafeUtil.getByte( address );
        }
    }

    @Override
    public byte getByte( long index, int offset )
    {
        return UnsafeUtil.getByte( address( index, offset ) );
    }

    @Override
    public short getShort( long index, int offset )
    {
        return UnsafeUtil.getShort( address( index, offset ) );
    }

    @Override
    public int getInt( long index, int offset )
    {
        return UnsafeUtil.getInt( address( index, offset ) );
    }

    @Override
    public long get6ByteLong( long index, int offset )
    {
        long address = address( index, offset );
        long low4b = (UnsafeUtil.getInt( address )) & 0xFFFFFFFFL;
        long high2b = UnsafeUtil.getShort( address + Integer.BYTES );
        return low4b | (high2b << 32);
    }

    @Override
    public long getLong( long index, int offset )
    {
        return UnsafeUtil.getLong( address( index, offset ) );
    }

    @Override
    public void set( long index, byte[] value )
    {
        long address = address( index, 0 );
        for ( int i = 0; i < itemSize; i++, address++ )
        {
            UnsafeUtil.putByte( address, value[i] );
        }
    }

    @Override
    public void setByte( long index, int offset, byte value )
    {
        UnsafeUtil.putByte( address( index, offset ), value );
    }

    @Override
    public void setShort( long index, int offset, short value )
    {
        UnsafeUtil.putShort( address( index, offset ), value );
    }

    @Override
    public void setInt( long index, int offset, int value )
    {
        UnsafeUtil.putInt( address( index, offset ), value );
    }

    @Override
    public void set6ByteLong( long index, int offset, long value )
    {
        long address = address( index, offset );
        UnsafeUtil.putInt( address, (int) value );
        UnsafeUtil.putShort( address + Integer.BYTES, (short) (value >>> 32) );
    }

    @Override
    public void setLong( long index, int offset, long value )
    {
        UnsafeUtil.putLong( address( index, offset ), value );
    }

    private long address( long index, int offset )
    {
        return address + (rebase( index ) * itemSize) + offset;
    }
}
