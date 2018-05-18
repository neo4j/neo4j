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

import java.util.Iterator;

/**
 * A fast implementation of a multi map with long[] as keys.
 *
 * Multi maps are maps that can store multiple values per key.
 * @param <VALUE>
 */
public class LongArrayHashMultiMap<VALUE>
{
    private static final long NOT_IN_USE = -2;
    private static final int SLOT_EMPTY = 0;
    private static final int VALUE_FOUND = 1;
    private static final int CONTINUE_PROBING = -1;
    private static final double LOAD_FACTOR = 0.75;

    private final int width;
    private Table table;

    public LongArrayHashMultiMap( int initialCapacity, int width )
    {
        assert (initialCapacity & (initialCapacity - 1)) == 0 : "Size must be a power of 2";
        assert width > 0 : "Number of elements must be larger than 0";

        this.width = width;
        table = new Table( initialCapacity );
    }

    public void add( long[] key, VALUE value )
    {
        assert LongArrayHash.validValue( key, width );
        int slotNr = slotFor( key );

        while ( true )
        {
            int offset = slotNr * width;
            if ( table.keys[offset] == NOT_IN_USE )
            {
                if ( table.timeToResize() )
                {
                    // We know we need to add the value to the set, but there is no space left
                    resize();
                    // Need to restart linear probe after resizing
                    slotNr = slotFor( key );
                }
                else
                {
                    // We found an empty spot!
                    table.setFirstValue( slotNr, key, value );
                    return;
                }
            }
            else
            {
                for ( int i = 0; i < width; i++ )
                {
                    if ( table.keys[offset + i] != key[i] )
                    {
                        // Found a different value in this slot - continue probing
                        slotNr = (slotNr + 1) & table.tableMask;
                        break;
                    }
                    else if ( i == width - 1 )
                    {
                        // We found other matching values
                        table.addValue( slotNr, value );
                        return;
                    }
                }
            }
        }
    }

    public Iterator<VALUE> get( long[] key )
    {
        assert LongArrayHash.validValue( key, width );
        int slot = slotFor( key );

        int result = table.checkSlot( slot, key );
        while ( result == CONTINUE_PROBING )
        {
            result = table.checkSlot( slot, key );
            slot = (slot + 1) & table.tableMask;
        }
        @SuppressWarnings( "unchecked" ) Node current = (Node) table.values[slot];

        return new Result( current );
    }

    public boolean isEmpty()
    {
        for ( int i = 0; i < table.capacity; i++ )
        {
            if ( table.keys[i] != NOT_IN_USE )
            {
                return false;
            }
        }
        return true;
    }

    private void resize()
    {
        int oldSize = table.capacity;
        int oldNumberEntries = table.numberOfEntries;
        long[] srcKeys = table.keys;
        Object[] srcValues = table.values;
        table = new Table( oldSize * 2 );
        long[] dstKeys = table.keys;
        table.numberOfEntries = oldNumberEntries;

        for ( int fromSlot = 0; fromSlot < oldSize; fromSlot = fromSlot + 1 )
        {
            int fromOffset = fromSlot * width;
            if ( srcKeys[fromOffset] != NOT_IN_USE )
            {
                int toSlot = LongArrayHash.hashCode( srcKeys, fromOffset, width ) & table.tableMask;

                if ( dstKeys[toSlot * width] != NOT_IN_USE )
                {
                    // Linear probe until we find an unused slot.
                    // No need to check for size here - we are already inside of resize()
                    toSlot = findUnusedSlot( dstKeys, toSlot );
                }
                System.arraycopy( srcKeys, fromOffset, dstKeys, toSlot * width, width );
                table.values[toSlot] = srcValues[fromSlot];
            }
        }
    }

    private int findUnusedSlot( long[] to, int fromSlot )
    {
        while ( true )
        {
            if ( to[fromSlot * width] == NOT_IN_USE )
            {
                return fromSlot;
            }
            fromSlot = (fromSlot + 1) & table.tableMask;
        }
    }


    private int slotFor( long[] value )
    {
        return LongArrayHash.hashCode( value, 0, width ) & table.tableMask;
    }

    class Node
    {
        final VALUE value;
        final Node next;

        public Node( VALUE value, Node next )
        {
            this.value = value;
            this.next = next;
        }
    }

    class Result extends org.neo4j.helpers.collection.PrefetchingIterator<VALUE>
    {
        private Node current;

        public Result( Node first )
        {
            current = first;
        }

        @Override
        protected VALUE fetchNextOrNull()
        {
            if ( current == null )
            {
                return null;
            }
            VALUE value = current.value;
            current = current.next;
            return value;
        }
    }

    class Table
    {
        private final int capacity;
        private final long[] keys;
        private final Object[] values;
        int numberOfEntries;
        private int resizeLimit;

        int tableMask;

        Table( int capacity )
        {
            this.capacity = capacity;
            resizeLimit = (int) (capacity * LOAD_FACTOR);
            tableMask = Integer.highestOneBit( capacity ) - 1;
            keys = new long[capacity * width];
            java.util.Arrays.fill( keys, NOT_IN_USE );
            values = new Object[capacity];
        }

        boolean timeToResize()
        {
            return numberOfEntries == resizeLimit;
        }

        // This code is duplicated in LongArrayHashSet. We should measure if it's OK to extract into LongArrayHash
        int checkSlot( int slot, long[] value )
        {
            assert value.length == width;

            int startOffset = slot * width;
            if ( keys[startOffset] == NOT_IN_USE )
            {
                return SLOT_EMPTY;
            }

            for ( int i = 0; i < width; i++ )
            {
                if ( keys[startOffset + i] != value[i] )
                {
                    return CONTINUE_PROBING;
                }
            }

            return VALUE_FOUND;
        }

        void setFirstValue( int slot, long[] key, VALUE value )
        {
            int offset = slot * width;
            System.arraycopy( key, 0, keys, offset, width );
            values[slot] = new Node( value, null );
            numberOfEntries++;
        }

        void addValue( int slot, VALUE value )
        {
            @SuppressWarnings( "unchecked" ) Node current = (Node) values[slot];
            values[slot] = new Node( value, current );
        }
    }
}
