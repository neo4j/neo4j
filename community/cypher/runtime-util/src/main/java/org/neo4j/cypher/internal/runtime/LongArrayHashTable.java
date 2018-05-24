/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime;

import org.neo4j.helpers.collection.Pair;

import static org.neo4j.cypher.internal.runtime.LongArrayHash.CONTINUE_PROBING;
import static org.neo4j.cypher.internal.runtime.LongArrayHash.NOT_IN_USE;
import static org.neo4j.cypher.internal.runtime.LongArrayHash.SLOT_EMPTY;
import static org.neo4j.cypher.internal.runtime.LongArrayHash.VALUE_FOUND;

/**
 * This hash table is used as a backing store for the keys of set, map and multi-map where the keys are arrays of longs.
 */
class LongArrayHashTable
{
    public final long[] keys;
    public final int width;
    public final int capacity;
    private int numberOfEntries;
    private int resizeLimit;

    private static final double LOAD_FACTOR = 0.75;

    int tableMask;

    LongArrayHashTable( int capacity, int width )
    {
        resizeLimit = (int) (capacity * LOAD_FACTOR);
        tableMask = Integer.highestOneBit( capacity ) - 1;
        keys = new long[capacity * width];
        this.width = width;
        java.util.Arrays.fill( keys, NOT_IN_USE );
        this.capacity = capacity;
    }

    /**
     * Signals whether it's time to size up or not. The actual resizing is done slightly differently depending on if this is a map or set. Maps have, in
     * addition to a hash table also a separate array for the values.
     *
     * @return true if the number of keys in the hash table has reached capacity.
     */
    boolean timeToResize()
    {
        return numberOfEntries >= resizeLimit;
    }

    /***
     * Checks whether a slot in the table contains a given key.
     * @param slot Slot to check
     * @param key Key to check
     * @return Can return:
     *  SLOT_EMPTY - This slot is free. In other words - the key is not in the table
     *  CONTINUE_PROBING - This slot is taken by a different key
     *  VALUE_FOUND - The key is in the table at this slot
     */
    int checkSlot( int slot, long[] key )
    {
        assert LongArrayHash.validValue( key, width );

        int startOffset = slot * width;
        if ( keys[startOffset] == NOT_IN_USE )
        {
            return SLOT_EMPTY;
        }

        for ( int i = 0; i < width; i++ )
        {
            if ( keys[startOffset + i] != key[i] )
            {
                return CONTINUE_PROBING;
            }
        }

        return VALUE_FOUND;
    }

    /**
     * Writes the key to this slot.
     *
     * @param slot The slot to write to.
     * @param key Le key.
     */
    void claimSlot( int slot, long[] key )
    {
        int offset = slot * width;
        assert keys[offset] == NOT_IN_USE : "Tried overwriting an already used slot";
        System.arraycopy( key, 0, keys, offset, width );
        numberOfEntries++;
    }

    public boolean isEmpty()
    {
        return numberOfEntries == 0;
    }

    /**
     * Finds an slot not already claimed, starting from a given slot.
     *
     * @return First unused slot after fromSlot
     */
    private int findUnusedSlot( int fromSlot )
    {
        while ( true )
        {
            if ( keys[fromSlot * width] == NOT_IN_USE )
            {
                return fromSlot;
            }
            fromSlot = (fromSlot + 1) & tableMask;
        }
    }

    LongArrayHashTable doubleCapacity()
    {
        LongArrayHashTable toTable = new LongArrayHashTable( capacity * 2, width );
        toTable.numberOfEntries = numberOfEntries;

        for ( int fromOffset = 0; fromOffset < capacity * width; fromOffset = fromOffset + width )
        {
            if ( keys[fromOffset] != NOT_IN_USE )
            {
                int toSlot = LongArrayHash.hashCode( keys, fromOffset, width ) & toTable.tableMask;
                toSlot = toTable.findUnusedSlot( toSlot );
                System.arraycopy( keys, fromOffset, toTable.keys, toSlot * width, width );
            }
        }

        return toTable;
    }

    Pair<LongArrayHashTable,Object[]> doubleCapacity( Object[] fromValues )
    {
        LongArrayHashTable toTable = new LongArrayHashTable( capacity * 2, width );
        Object[] toValues = new Object[capacity * 2];
        long[] fromKeys = keys;
        toTable.numberOfEntries = numberOfEntries;
        for ( int fromSlot = 0; fromSlot < capacity; fromSlot = fromSlot + 1 )
        {
            int fromOffset = fromSlot * width;
            if ( fromKeys[fromOffset] != NOT_IN_USE )
            {
                int toSlot = LongArrayHash.hashCode( fromKeys, fromOffset, width ) & toTable.tableMask;
                toSlot = toTable.findUnusedSlot( toSlot );
                System.arraycopy( fromKeys, fromOffset, toTable.keys, toSlot * width, width );
                toValues[toSlot] = fromValues[fromSlot];
            }
        }
        return Pair.of( toTable, toValues );
    }
}
