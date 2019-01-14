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

import java.util.Arrays;

import static java.util.Arrays.fill;

/**
 * Table implementation for handling primitive int/long keys and hop bits. The quantized unit is int so a
 * multiple of ints will be used for every entry.
 *
 * In this class, <code>index</code> refers to the index of an entry (key + value + hop bits), while
 * <code>address</code> refers to the position of an int word in the internal <code>table</code>.
 *
 * @param <VALUE> essentially ignored, since no values are stored in this table. Although subclasses can.
 */
public abstract class IntArrayBasedKeyTable<VALUE> extends PowerOfTwoQuantizedTable<VALUE>
{
    protected int[] table;
    protected final VALUE singleValue; // used as a pointer to pass around a primitive value in concrete subclasses
    private final int itemsPerEntry;

    protected IntArrayBasedKeyTable( int capacity, int itemsPerEntry, int h, VALUE singleValue )
    {
        super( capacity, h );
        this.singleValue = singleValue;
        this.itemsPerEntry = itemsPerEntry;
        initializeTable();
        clearTable();
    }

    protected void initializeTable()
    {
        this.table = new int[capacity * itemsPerEntry];
    }

    protected void clearTable()
    {
        fill( table, -1 );
    }

    protected long putLong( int actualIndex, long value )
    {
        long previous = getLong( actualIndex );
        table[actualIndex] = (int)value;
        table[actualIndex + 1] = (int) ((value & 0xFFFFFFFF00000000L) >>> 32);
        return previous;
    }

    protected long getLong( int actualIndex )
    {
        long low = table[actualIndex] & 0xFFFFFFFFL;
        long high = table[actualIndex + 1] & 0xFFFFFFFFL;
        return (high << 32) | low;
    }

    @Override
    public void put( int index, long key, VALUE value )
    {
        int address = address( index );
        internalPut( address, key, value );
        size++;
    }

    @Override
    public VALUE remove( int index )
    {
        int address = address( index );
        VALUE value = value( index );
        internalRemove( address );
        size--;
        return value;
    }

    @Override
    public long move( int fromIndex, int toIndex )
    {
        long key = key( fromIndex );
        int fromAddress = address( fromIndex );
        int toAddress = address( toIndex );
        for ( int i = 0; i < itemsPerEntry - 1; i++ )
        {
            int tempValue = table[fromAddress + i];
            table[fromAddress + i] = table[toAddress + i];
            table[toAddress + i] = tempValue;
        }
        return key;
    }

    protected void internalRemove( int actualIndex )
    {
        Arrays.fill( table, actualIndex, actualIndex + itemsPerEntry - 1 /*leave the hop bits alone*/, -1 );
    }

    protected abstract void internalPut( int actualIndex, long key, VALUE value );

    @Override
    public VALUE value( int index )
    {
        return singleValue;
    }

    @Override
    public VALUE putValue( int index, VALUE value )
    {
        return value;
    }

    @Override
    public long hopBits( int index )
    {
        return ~(table[address( index ) + itemsPerEntry - 1] | 0xFFFFFFFF00000000L);
    }

    private int hopBit( int hd )
    {
        return 1 << hd;
    }

    @Override
    public void putHopBit( int index, int hd )
    {
        table[address( index ) + itemsPerEntry - 1] &= ~hopBit( hd );
    }

    @Override
    public void moveHopBit( int index, int hd, int delta )
    {
        table[address( index ) + itemsPerEntry - 1] ^= hopBit( hd ) | hopBit( hd + delta );
    }

    @Override
    public void removeHopBit( int index, int hd )
    {
        table[address( index ) + itemsPerEntry - 1] |= hopBit( hd );
    }

    protected int address( int index )
    {
        return index * itemsPerEntry;
    }

    @Override
    public void clear()
    {
        if ( !isEmpty() )
        {
            clearTable();
        }
        super.clear();
    }
}
