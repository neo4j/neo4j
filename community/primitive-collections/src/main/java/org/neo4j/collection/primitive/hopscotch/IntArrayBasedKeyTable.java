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

import java.util.Arrays;

import static java.util.Arrays.fill;

/**
 * Table implementation for handling primitive int/long keys and hop bits. The quantized unit is int so a
 * multiple of ints will be used for every entry.
 *
 * @param <VALUE> essentially ignored, since no values are stored in this table. Although subclasses can.
 */
public abstract class IntArrayBasedKeyTable<VALUE> extends PowerOfTwoQuantizedTable<VALUE>
{
    protected int[] table;
    protected final VALUE singleValue;
    private final int itemsPerEntry;

    protected IntArrayBasedKeyTable( int itemsPerEntry, int h, VALUE singleValue )
    {
        this( baseCapacity( h ), itemsPerEntry, h, singleValue );
    }

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
        this.table = new int[capacity*itemsPerEntry];
    }

    protected void clearTable()
    {
        fill( table, -1 );
    }

    protected long putLong( int actualIndex, long value )
    {
        long previous = getLong( actualIndex );
        table[actualIndex] = (int)value;
        table[actualIndex+1] = (int)((value&0xFFFFFFFF00000000L) >>> 32);
        return previous;
    }

    protected long getLong( int actualIndex )
    {
        long low = table[actualIndex]&0xFFFFFFFFL;
        long high = table[actualIndex+1]&0xFFFFFFFFL;
        return (high << 32) | low;
    }

    @Override
    public void put( int index, long key, VALUE value )
    {
        int actualIndex = index( index );
        internalPut( actualIndex, key, value );
        size++;
    }

    @Override
    public VALUE remove( int index )
    {
        int actualIndex = index( index );
        VALUE value = value( index );
        internalRemove( actualIndex );
        size--;
        return value;
    }

    @Override
    public long move( int fromIndex, int toIndex )
    {
        long key = key( fromIndex );
        int actualFromIndex = index( fromIndex );
        int actualToIndex = index( toIndex );
        for ( int i = 0; i < itemsPerEntry-1; i++ )
        {
            int tempValue = table[actualFromIndex+i];
            table[actualFromIndex+i] = table[actualToIndex+i];
            table[actualToIndex+i] = tempValue;
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
        return ~(table[index( index )+itemsPerEntry-1] | 0xFFFFFFFF00000000L);
    }

    private int hopBit( int hd )
    {
        return 1 << hd;
    }

    @Override
    public void putHopBit( int index, int hd )
    {
        table[index( index )+itemsPerEntry-1] &= ~hopBit( hd );
    }

    @Override
    public void moveHopBit( int index, int hd, int delta )
    {
        table[index( index )+itemsPerEntry-1] ^= (hopBit( hd ) | hopBit( hd+delta ));
    }

    @Override
    public void removeHopBit( int index, int hd )
    {
        table[index( index )+itemsPerEntry-1] |= hopBit( hd );
    }

    protected int index( int index )
    {
        return index*itemsPerEntry;
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
}
