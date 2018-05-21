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

import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.helpers.collection.PrefetchingIterator;

import static org.neo4j.cypher.internal.runtime.LongArrayHash.CONTINUE_PROBING;
import static org.neo4j.cypher.internal.runtime.LongArrayHash.NOT_IN_USE;
import static org.neo4j.cypher.internal.runtime.LongArrayHash.SLOT_EMPTY;
import static org.neo4j.cypher.internal.runtime.LongArrayHash.VALUE_FOUND;

/**
 * A fast implementation of a hash map with long[] as keys.
 */
public class LongArrayHashMap<VALUE>
{
    private final int width;
    private LongArrayHashTable table;
    private Object[] values;

    public LongArrayHashMap( int initialCapacity, int width )
    {
        assert (initialCapacity & (initialCapacity - 1)) == 0 : "Size must be a power of 2";
        assert width > 0 : "Number of elements must be larger than 0";

        this.width = width;
        table = new LongArrayHashTable( initialCapacity, width );
        values = new Object[initialCapacity];
    }

    public VALUE getOrCreateAndAdd( long[] key, Supplier<VALUE> creator )
    {
        assert LongArrayHash.validValue( key, width );
        int slotNr = slotFor( key );
        while ( true )
        {
            int currentState = table.checkSlot( slotNr, key );
            switch ( currentState )
            {
            case SLOT_EMPTY:
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
                    table.claimSlot( slotNr, key );
                    VALUE newValue = creator.get();
                    values[slotNr] = newValue;
                    return newValue;
                }
                break;

            case CONTINUE_PROBING:
                slotNr = (slotNr + 1) & table.tableMask;
                break;

            case VALUE_FOUND:
                @SuppressWarnings( "unchecked" )
                VALUE oldValue = (VALUE) values[slotNr];
                return oldValue;

            default:
                throw new InternalException( "Unknown state returned from hash table " + currentState, null );
            }
        }
    }

    public VALUE get( long[] key )
    {
        assert LongArrayHash.validValue( key, width );
        int slotNr = slotFor( key );
        while ( true )
        {
            int currentState = table.checkSlot( slotNr, key );
            switch ( currentState )
            {
            case SLOT_EMPTY:
                return null;

            case CONTINUE_PROBING:
                slotNr = (slotNr + 1) & table.tableMask;
                break;

            case VALUE_FOUND:
                @SuppressWarnings( "unchecked" )
                VALUE oldValue = (VALUE) values[slotNr];
                return oldValue;

            default:
                throw new InternalException( "Unknown state returned from hash table " + currentState, null );
            }
        }
    }

    public boolean isEmpty()
    {
        return table.isEmpty();
    }

    private void resize()
    {
        Pair<LongArrayHashTable,Object[]> resized = LongArrayHash.doubleInSize( table, values );
        table = resized.first();
        values = resized.other();
    }

    private int slotFor( long[] value )
    {
        return LongArrayHash.hashCode( value, 0, width ) & table.tableMask;
    }

    public Iterator<Map.Entry<long[],VALUE>> iterator()
    {
        return new PrefetchingIterator<Map.Entry<long[],VALUE>>()
        {
            int current; // Initialized to 0

            @Override
            protected Map.Entry<long[],VALUE> fetchNextOrNull()
            {
                // First, find a good spot
                while ( current < table.capacity && table.keys[current * width] == NOT_IN_USE )
                {
                    current = current + 1;
                }

                // If we have reached the end, return null
                if ( current == table.capacity )
                {
                    return null;
                }

                // Otherwise, let's create the return object.
                long[] key = new long[width];
                System.arraycopy( table.keys, current * width, key, 0, width );

                @SuppressWarnings( "unchecked" )
                VALUE value = (VALUE) values[current];
                Entry result = new Entry( key, value );

                // Move
                current = current + 1;

                return result;
            }
        };
    }

    class Entry implements Map.Entry<long[],VALUE>
    {
        private final long[] key;
        private final VALUE value;

        Entry( long[] key, VALUE value )
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public long[] getKey()
        {
            return key;
        }

        @Override
        public VALUE getValue()
        {
            return value;
        }

        @Override
        public VALUE setValue( VALUE value )
        {
            return null;
        }
    }
}
