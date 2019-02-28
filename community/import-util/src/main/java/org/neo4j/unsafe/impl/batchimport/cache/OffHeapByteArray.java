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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

public class OffHeapByteArray extends OffHeapNumberArray<ByteArray> implements ByteArray
{
    private final byte[] defaultValue;

    protected OffHeapByteArray( long length, byte[] defaultValue, long base, MemoryAllocationTracker allocationTracker )
    {
        super( length, defaultValue.length, base, allocationTracker );
        this.defaultValue = defaultValue;
        clear();
    }

    @Override
    public void swap( long fromIndex, long toIndex )
    {
        int bytesLeft = itemSize;
        long fromAddress = address( fromIndex, 0 );
        long toAddress = address( toIndex, 0 );

        // piece-wise swap, as large chunks as possible: long, int, short and finally byte-wise swap
        while ( bytesLeft > 0 )
        {
            int chunkSize;
            if ( bytesLeft >= Long.BYTES )
            {
                chunkSize = Long.BYTES;
                long intermediary = getLong( fromAddress );
                UnsafeUtil.copyMemory( toAddress, fromAddress, chunkSize );
                putLong( toAddress, intermediary );
            }
            else if ( bytesLeft >= Integer.BYTES )
            {
                chunkSize = Integer.BYTES;
                int intermediary = getInt( fromAddress );
                UnsafeUtil.copyMemory( toAddress, fromAddress, chunkSize );
                putInt( toAddress, intermediary );
            }
            else if ( bytesLeft >= Short.BYTES )
            {
                chunkSize = Short.BYTES;
                short intermediary = getShort( fromAddress );
                UnsafeUtil.copyMemory( toAddress, fromAddress, chunkSize );
                putShort( toAddress, intermediary );
            }
            else
            {
                chunkSize = Byte.BYTES;
                byte intermediary = getByte( fromAddress );
                UnsafeUtil.copyMemory( toAddress, fromAddress, chunkSize );
                putByte( toAddress, intermediary );
            }
            fromAddress += chunkSize;
            toAddress += chunkSize;
            bytesLeft -= chunkSize;
        }
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
            long intermediary = UnsafeUtil.allocateMemory( itemSize, allocationTracker );
            for ( int i = 0; i < defaultValue.length; i++ )
            {
                UnsafeUtil.putByte( intermediary + i, defaultValue[i] );
            }

            for ( long i = 0, adr = address; i < length; i++, adr += itemSize )
            {
                UnsafeUtil.copyMemory( intermediary, adr, itemSize );
            }
            UnsafeUtil.free( intermediary, itemSize, allocationTracker );
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

    private byte getByte( long p )
    {
        return UnsafeUtil.getByte( p );
    }

    @Override
    public short getShort( long index, int offset )
    {
        return getShort( address( index, offset ) );
    }

    private short getShort( long p )
    {
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            return UnsafeUtil.getShort( p );
        }

        return UnsafeUtil.getShortByteWiseLittleEndian( p );
    }

    @Override
    public int getInt( long index, int offset )
    {
        return getInt( address( index, offset ) );
    }

    private int getInt( long p )
    {
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            return UnsafeUtil.getInt( p );
        }

        return UnsafeUtil.getIntByteWiseLittleEndian( p );
    }

    @Override
    public long get5ByteLong( long index, int offset )
    {
        long address = address( index, offset );
        long low4b = getInt( address ) & 0xFFFFFFFFL;
        long high1b = UnsafeUtil.getByte( address + Integer.BYTES ) & 0xFF;
        long result = low4b | (high1b << 32);
        return result == 0xFFFFFFFFFFL ? -1 : result;
    }

    @Override
    public long get6ByteLong( long index, int offset )
    {
        long address = address( index, offset );
        long low4b = getInt( address ) & 0xFFFFFFFFL;
        long high2b = getShort( address + Integer.BYTES ) & 0xFFFF;
        long result = low4b | (high2b << 32);
        return result == 0xFFFFFFFFFFFFL ? -1 : result;
    }

    @Override
    public long getLong( long index, int offset )
    {
        long p = address( index, offset );
        return getLong( p );
    }

    private long getLong( long p )
    {
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            return UnsafeUtil.getLong( p );
        }

        return UnsafeUtil.getLongByteWiseLittleEndian( p );
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

    private void putByte( long p, byte value )
    {
        UnsafeUtil.putByte( p, value );
    }

    @Override
    public void setShort( long index, int offset, short value )
    {
        putShort( address( index, offset ), value );
    }

    private void putShort( long p, short value )
    {
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            UnsafeUtil.putShort( p, value );
        }
        else
        {
            UnsafeUtil.putShortByteWiseLittleEndian( p, value );
        }
    }

    @Override
    public void setInt( long index, int offset, int value )
    {
        putInt( address( index, offset ), value );
    }

    private void putInt( long p, int value )
    {
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            UnsafeUtil.putInt( p, value );
        }
        else
        {
            UnsafeUtil.putIntByteWiseLittleEndian( p, value );
        }
    }

    @Override
    public void set5ByteLong( long index, int offset, long value )
    {
        long address = address( index, offset );
        putInt( address, (int) value );
        UnsafeUtil.putByte( address + Integer.BYTES, (byte) (value >>> 32) );
    }

    @Override
    public void set6ByteLong( long index, int offset, long value )
    {
        long address = address( index, offset );
        putInt( address, (int) value );
        putShort( address + Integer.BYTES, (short) (value >>> 32) );
    }

    @Override
    public void setLong( long index, int offset, long value )
    {
        long p = address( index, offset );
        putLong( p, value );
    }

    private void putLong( long p, long value )
    {
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            UnsafeUtil.putLong( p, value );
        }
        else
        {
            UnsafeUtil.putLongByteWiseLittleEndian( p, value );
        }
    }

    @Override
    public int get3ByteInt( long index, int offset )
    {
        long address = address( index, offset );
        int lowWord = UnsafeUtil.getShort( address ) & 0xFFFF;
        int highByte = UnsafeUtil.getByte( address + Short.BYTES ) & 0xFF;
        int result = lowWord | (highByte << Short.SIZE);
        return result == 0xFFFFFF ? -1 : result;
    }

    @Override
    public void set3ByteInt( long index, int offset, int value )
    {
        long address = address( index, offset );
        UnsafeUtil.putShort( address, (short) value );
        UnsafeUtil.putByte( address + Short.BYTES, (byte) (value >>> Short.SIZE) );
    }

    private long address( long index, int offset )
    {
        checkBounds( index );
        return address + (rebase( index ) * itemSize) + offset;
    }

    private void checkBounds( long index )
    {
        long rebased = rebase( index );
        if ( rebased < 0 || rebased >= length )
        {
            throw new IndexOutOfBoundsException( "Wanted to access " + rebased + " but range is " + base + "-" + length );
        }
    }
}
