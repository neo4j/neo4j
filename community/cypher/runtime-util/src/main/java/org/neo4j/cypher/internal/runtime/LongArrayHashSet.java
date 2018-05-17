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

import org.opencypher.v9_0.util.InternalException;

import static org.neo4j.cypher.internal.runtime.LongArrayHash.CONTINUE_PROBING;
import static org.neo4j.cypher.internal.runtime.LongArrayHash.SLOT_EMPTY;
import static org.neo4j.cypher.internal.runtime.LongArrayHash.VALUE_FOUND;

/**
 * When you need to have a set of arrays of longs representing entities - look no further
 * <p>
 * This set will keep all it's state in a single long[] array, marking unused slots
 * using -2, a value that should never be used for node or relationship id's.
 * <p>
 * The set will be resized when either probing has to go on for too long when doing inserts,
 * or the load factor reaches 75%.
 * <p>
 * The word "offset" here means the index into an array,
 * and slot is a number that multiplied by the width of the values will return the offset.
 */
public class LongArrayHashSet
{
    private LongArrayHashTable table;
    private final int width;

    public LongArrayHashSet( int initialCapacity, int width )
    {
        assert (initialCapacity & (initialCapacity - 1)) == 0 : "Size must be a power of 2";
        assert width > 0 : "Number of elements must be larger than 0";

        this.width = width;
        table = new LongArrayHashTable( initialCapacity, width );
    }

    /**
     * Adds a value to the set.
     *
     * @param value The new value to be added to the set
     * @return The method returns true if the value was added and false if it already existed in the set.
     */
    public boolean add( long[] value )
    {
        assert LongArrayHash.validValue( value, width );
        int slotNr = slotFor( value );

        while ( true )
        {
            int currentState = table.checkSlot( slotNr, value );
            switch ( currentState )
            {
            case SLOT_EMPTY:
                if ( table.timeToResize() )
                {
                    // We know we need to add the value to the set, but there is no space left
                    table = table.doubleCapacity();
                    // Need to restart linear probe after resizing
                    slotNr = slotFor( value );
                }
                else
                {
                    // We found an empty spot!
                    table.claimSlot( slotNr, value );
                    return true;
                }
                break;

            case CONTINUE_PROBING:
                slotNr = (slotNr + 1) & table.tableMask;
                break;

            case VALUE_FOUND:
                return false;

            default:
                throw new InternalException( "Unknown state returned from hash table " + currentState, null );
            }
        }
    }

    /***
     * Returns true if the value is in the set.
     * @param value The value to check for
     * @return whether the value is in the set or not.
     */
    public boolean contains( long[] value )
    {
        assert LongArrayHash.validValue( value, width );
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

    private int slotFor( long[] value )
    {
        return LongArrayHash.hashCode( value, 0, width ) & table.tableMask;
    }
}
