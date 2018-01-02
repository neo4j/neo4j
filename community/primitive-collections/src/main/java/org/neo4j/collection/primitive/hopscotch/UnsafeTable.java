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
package org.neo4j.collection.primitive.hopscotch;


import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

public abstract class UnsafeTable<VALUE> extends PowerOfTwoQuantizedTable<VALUE>
{
    private final int bytesPerKey;
    private final int bytesPerEntry;
    private final long dataSize;
    private final long address;
    protected final VALUE valueMarker;

    protected UnsafeTable( int capacity, int bytesPerKey, VALUE valueMarker )
    {
        super( capacity, 32 );
        UnsafeUtil.assertHasUnsafe();
        this.bytesPerKey = bytesPerKey;
        this.bytesPerEntry = 4+bytesPerKey;
        this.valueMarker = valueMarker;
        this.dataSize = (long)this.capacity*bytesPerEntry;
        this.address = UnsafeUtil.allocateMemory( dataSize );
        clearTable();
    }

    private void clearTable()
    {
        UnsafeUtil.setMemory( address, dataSize, (byte)-1 );
    }

    @Override
    public void clear()
    {
        if ( size > 0 )
        {
            clearTable();
        }
        super.clear();
    }

    @Override
    public long key( int index )
    {
        return internalKey( keyAddress( index ) );
    }

    protected abstract long internalKey( long keyAddress );

    @Override
    public VALUE value( int index )
    {
        return valueMarker;
    }

    @Override
    public void put( int index, long key, VALUE value )
    {
        internalPut( keyAddress( index ), key, value );
        size++;
    }

    protected abstract void internalPut( long keyAddress, long key, VALUE value );

    @Override
    public VALUE putValue( int index, VALUE value )
    {
        return value;
    }

    @Override
    public long move( int fromIndex, int toIndex )
    {
        long adr = keyAddress( fromIndex );
        long key = internalKey( adr );
        VALUE value = internalRemove( adr );
        internalPut( keyAddress( toIndex ), key, value );
        return key;
    }

    @Override
    public VALUE remove( int index )
    {
        VALUE value = internalRemove( keyAddress( index ) );
        size--;
        return value;
    }

    protected VALUE internalRemove( long keyAddress )
    {
        UnsafeUtil.setMemory( keyAddress, bytesPerKey, (byte)-1 );
        return valueMarker;
    }

    @Override
    public long hopBits( int index )
    {
        return ~(UnsafeUtil.getInt( hopBitsAddress( index ) ) | 0xFFFFFFFF00000000L);
    }

    @Override
    public void putHopBit( int index, int hd )
    {
        long adr = hopBitsAddress( index );
        int hopBits = UnsafeUtil.getInt( adr );
        hopBits &= ~(1 << hd);
        UnsafeUtil.putInt( adr, hopBits );
    }

    @Override
    public void moveHopBit( int index, int hd, int delta )
    {
        long adr = hopBitsAddress( index );
        int hopBits = UnsafeUtil.getInt( adr );
        hopBits ^= (1 << hd) | (1 << (hd+delta));
        UnsafeUtil.putInt( adr, hopBits );
    }

    protected long keyAddress( int index )
    {
        return address + (index*bytesPerEntry) + 4;
    }

    protected long hopBitsAddress( int index )
    {
        return address + (index*bytesPerEntry);
    }

    @Override
    public void removeHopBit( int index, int hd )
    {
        long adr = hopBitsAddress( index );
        int hopBits = UnsafeUtil.getInt( adr );
        hopBits |= (1 << hd);
        UnsafeUtil.putInt( adr, hopBits );
    }

    @Override
    public void close()
    {
        UnsafeUtil.free( address );
    }
}
