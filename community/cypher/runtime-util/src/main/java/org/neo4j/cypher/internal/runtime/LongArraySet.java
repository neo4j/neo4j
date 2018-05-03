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
package org.neo4j.cypher.internal.runtime;

/**
 * When you need to have a set of arrays of longs representing entities - look no further
 * <p>
 * This set will keep all it's state in a single long[] array, marking unused slots
 * using 0xF000000000000000L, a value that should never be used for node or relationship id's.
 * <p>
 * The set will be resized when either probing has to go on for too long when doing inserts,
 * or the load factor reaches 75%.
 *
 * The word "offset" here means the index into an array,
 * and slot is a number that multiplied by the width of the values will return the offset.
 */
public class LongArraySet
{
    private static final long NOT_IN_USE = 0xF000000000000000L;
    private static final int SLOT_EMPTY = 0;
    private static final int VALUE_FOUND = 1;
    private static final int CONTINUE_PROBING = -1;

    private Table table;
    private final int width;

    public LongArraySet( int initialCapacity, int width )
    {
        assert (initialCapacity & (initialCapacity - 1)) == 0 : "Size must be a power of 2";
        assert width > 0 : "Number of elements must be larger than 0";

        this.width = width;
        table = new Table( initialCapacity );
    }

    /**
     * Adds a value to the set.
     *
     * @param value The new value to be added to the set
     * @return The method returns true if the value was added and false if it already existed in the set.
     */
    public boolean add( long[] value )
    {
        assert value.length == width : "all elements must have the same size";
        int slotNr = slotFor( value );
        while ( true )
        {
            int offset = slotNr * width;
            if ( table.inner[offset] == NOT_IN_USE )
            {
                if ( table.timeToResize() )
                {
                    // We know we need to add the value to the set, but there is no space left
                    resize();
                    // Need to restart linear probe after resizing
                    slotNr = slotFor( value );
                }
                else
                {
                    table.setValue( slotNr, value );
                    return true;
                }
            }
            else
            {
                for ( int i = 0; i < width; i++ )
                {
                    if ( table.inner[offset + i] != value[i] )
                    {
                        // Found a different value in this slot
                        slotNr = (slotNr + 1) & table.tableMask;
                        break;
                    }
                    else if ( i == width - 1 )
                    {
                        return false;
                    }
                }
            }
        }
    }

    /***
     * Returns true if the value is in the set.
     * @param value The value to check for
    1     * @return whether the value is in the set or not.
     */
    public boolean contains( long[] value )
    {
        assert value.length == width : "all elements must have the same size";
        int slot = slotFor( value );

        int result;
        do
        {
            result = table.checkSlot( slot, value );
            slot = (slot + 1) & table.tableMask;
        }
        while ( result == CONTINUE_PROBING );
        return result == VALUE_FOUND;
    }

    private void resize()
    {
        int oldSize = table.capacity;
        int oldNumberEntries = table.numberOfEntries;
        long[] srcArray = table.inner;
        Table oldTable = table;
        table = new Table( oldSize * 2 );
        long[] dstArray = table.inner;
        table.numberOfEntries = oldNumberEntries;

        for ( int fromSlot = 0; fromSlot < oldSize; fromSlot++ )
        {
            int fromOffset = fromSlot * width;
            if ( srcArray[fromOffset] != NOT_IN_USE )
            {
                int toSlot = oldTable.hashCode( fromOffset ) & table.tableMask;

                // Linear probe until we find an unused slot.
                // No need to check for size here - we are already inside of resize()
                toSlot = findUnusedSlot( dstArray, toSlot );
                System.arraycopy( srcArray, fromOffset, dstArray, toSlot * width, width );
            }
        }
    }

    private int findUnusedSlot( long[] to, int startOffset )
    {
        while ( true )
        {
            if ( to[startOffset * width] == NOT_IN_USE )
            {
                return startOffset;
            }
            startOffset = (startOffset + 1) & table.tableMask;
        }
    }

    private int slotFor( long[] value )
    {
        return java.util.Arrays.hashCode( value ) & table.tableMask;
    }

    class Table
    {
        private final int capacity;
        private final long[] inner;
        int numberOfEntries;
        private int resizeLimit;

        int tableMask;

        Table( int capacity )
        {
            this.capacity = capacity;
            resizeLimit = (int) (capacity * 0.75);
            tableMask = Integer.highestOneBit( capacity ) - 1;
            inner = new long[capacity * width];
            java.util.Arrays.fill( inner, NOT_IN_USE );
        }

        boolean timeToResize()
        {
            return numberOfEntries == resizeLimit;
        }

        int hashCode( int offset )
        {
            int result = 1;
            int lastOffset = offset + width;
            for ( int i = offset; i < lastOffset; i++ )
            {
                long element = inner[i];
                int elementHash = (int) (element ^ (element >>> 32));
                result = 31 * result + elementHash;
            }

            return result;
        }

        int checkSlot( int slot, long[] value )
        {
            assert value.length == width;

            int startOffset = slot * width;
            if ( inner[startOffset] == NOT_IN_USE )
            {
                return SLOT_EMPTY;
            }

            for ( int i = 0; i < width; i++ )
            {
                if ( inner[startOffset + i] != value[i] )
                {
                    return CONTINUE_PROBING;
                }
            }

            return VALUE_FOUND;
        }

        void setValue( int slot, long[] value )
        {
            int offset = slot * width;
            System.arraycopy( value, 0, inner, offset, width );
            numberOfEntries++;
        }
    }
}
