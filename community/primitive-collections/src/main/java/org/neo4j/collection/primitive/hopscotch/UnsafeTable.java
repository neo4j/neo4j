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
package org.neo4j.collection.primitive.hopscotch;

import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

public abstract class UnsafeTable<VALUE> extends PowerOfTwoQuantizedTable<VALUE>
{
    private final int bytesPerKey;
    private final int bytesPerEntry;
    private final long dataSize;
    private final long allocatedBytes;
    // address which should be free when closing
    private final long allocatedAddress;
    // address which should be used to access the table, the address where the table actually starts at
    private final long address;
    protected final VALUE valueMarker;
    protected final MemoryAllocationTracker allocationTracker;

    protected UnsafeTable( int capacity, int bytesPerKey, VALUE valueMarker, MemoryAllocationTracker allocationTracker )
    {
        super( capacity, 32 );
        UnsafeUtil.assertHasUnsafe();
        this.allocationTracker = allocationTracker;
        this.bytesPerKey = bytesPerKey;
        this.bytesPerEntry = 4 + bytesPerKey;
        this.valueMarker = valueMarker;
        this.dataSize = (long) this.capacity * bytesPerEntry;

        // Below is a piece of code which ensures that allocated memory is aligned to 4-byte boundary
        // if memory system requires aligned memory access. The reason we pick 4-byte boundary is that
        // it's the lowest common denominator and the size of our hop-bits field for every entry.
        // So even for a table which would only deal with, say longs (8-byte), it would still need to
        // read and write 4-byte hop-bits fields. Therefore this table can, if required to, read anything
        // bigger than 4-byte fields as multiple 4-byte fields. This way it can play well with aligned
        // memory access requirements.

        assert bytesPerEntry % Integer.BYTES == 0 : "Bytes per entry needs to be divisible by 4, this constraint " +
                "is checked because on memory systems requiring aligned memory access this would otherwise break.";

        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            allocatedBytes = dataSize;
            this.allocatedAddress = this.address = UnsafeUtil.allocateMemory( allocatedBytes, this.allocationTracker );
        }
        else
        {
            // There's an assertion above also verifying this, but it's only an actual problem if our memory system
            // requires aligned access, which seems to be the case right here and now.
            if ( (bytesPerEntry % Integer.BYTES) != 0 )
            {
                throw new IllegalArgumentException( "Memory system requires aligned memory access and " +
                        getClass().getSimpleName() + " was designed to cope with this requirement by " +
                        "being able to accessing data in 4-byte chunks, if needed to. " +
                        "Although this table tried to be constructed with bytesPerKey:" + bytesPerKey +
                        " yielding a bytesPerEntry:" + bytesPerEntry + ", which isn't 4-byte aligned." );
            }

            allocatedBytes = dataSize + Integer.BYTES - 1;
            this.allocatedAddress = UnsafeUtil.allocateMemory( allocatedBytes, this.allocationTracker );
            this.address = UnsafeUtil.alignedMemory( allocatedAddress, Integer.BYTES );
        }

        clearMemory();
    }

    @Override
    public void clear()
    {
        if ( !isEmpty() )
        {
            clearMemory();
        }
        super.clear();
    }

    private void clearMemory()
    {
        UnsafeUtil.setMemory( address, dataSize, (byte)-1 );
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
        hopBits ^= (1 << hd) | (1 << (hd + delta));
        UnsafeUtil.putInt( adr, hopBits );
    }

    protected long keyAddress( int index )
    {
        return address + (index * ((long) bytesPerEntry)) + 4;
    }

    protected long hopBitsAddress( int index )
    {
        return address + (index * ((long) bytesPerEntry));
    }

    @Override
    public void removeHopBit( int index, int hd )
    {
        long adr = hopBitsAddress( index );
        int hopBits = UnsafeUtil.getInt( adr );
        hopBits |= 1 << hd;
        UnsafeUtil.putInt( adr, hopBits );
    }

    @Override
    public void close()
    {
        UnsafeUtil.free( allocatedAddress, allocatedBytes, allocationTracker );
    }

    protected static void alignmentSafePutLongAsTwoInts( long address, long value )
    {
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            UnsafeUtil.putLong( address, value );
        }
        else
        {
            // See javadoc in constructor as to why we do this
            UnsafeUtil.putInt( address, (int) value );
            UnsafeUtil.putInt( address + Integer.BYTES, (int) (value >>> Integer.SIZE) );
        }
    }

    protected static long alignmentSafeGetLongAsTwoInts( long address )
    {
        if ( UnsafeUtil.allowUnalignedMemoryAccess )
        {
            return UnsafeUtil.getLong( address );
        }

        // See javadoc in constructor as to why we do this
        long lsb = UnsafeUtil.getInt( address ) & 0xFFFFFFFFL;
        long msb = UnsafeUtil.getInt( address + Integer.BYTES ) & 0xFFFFFFFFL;
        return lsb | (msb << Integer.SIZE);
    }

}
