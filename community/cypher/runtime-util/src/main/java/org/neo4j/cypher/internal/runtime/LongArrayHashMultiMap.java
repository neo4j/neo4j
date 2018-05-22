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

import org.neo4j.helpers.collection.Pair;

import static org.neo4j.cypher.internal.runtime.LongArrayHash.CONTINUE_PROBING;
import static org.neo4j.cypher.internal.runtime.LongArrayHash.SLOT_EMPTY;
import static org.neo4j.cypher.internal.runtime.LongArrayHash.VALUE_FOUND;

/**
 * A fast implementation of a multi map with long[] as keys.
 * Multi maps are maps that can store multiple values per key.
 */
public class LongArrayHashMultiMap<VALUE>
{
    private final int width;
    private LongArrayHashTable table;
    private Object[] values;

    public LongArrayHashMultiMap( int initialCapacity, int width )
    {
        assert (initialCapacity & (initialCapacity - 1)) == 0 : "Size must be a power of 2";
        assert width > 0 : "Number of elements must be larger than 0";

        this.width = width;
        table = new LongArrayHashTable( initialCapacity, width );
        values = new Object[initialCapacity];
    }

    public void add( long[] key, VALUE value )
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
                    values[slotNr] = new Node( value, null );
                    return;
                }
                break;

            case CONTINUE_PROBING:
                slotNr = (slotNr + 1) & table.tableMask;
                break;

            case VALUE_FOUND:
                // Slot already taken by this key. We'll just add this new value to the list.
                @SuppressWarnings( "unchecked" )
                Node oldValue = (Node) values[slotNr];
                values[slotNr] = new Node( value, oldValue );
                return;

            default:
                throw new InternalException( "Unknown state returned from hash table " + currentState, null );
            }
        }
    }

    public Iterator<VALUE> get( long[] key )
    {
        assert LongArrayHash.validValue( key, width );
        int slot = slotFor( key );

        // Here we'll spin while the slot is taken by a different value.
        while ( table.checkSlot( slot, key ) == CONTINUE_PROBING )
        {
            slot = (slot + 1) & table.tableMask;
        }

        @SuppressWarnings( "unchecked" )
        Node current = (Node) values[slot];

        return new Result( current );
    }

    public boolean isEmpty()
    {
        return table.isEmpty();
    }

    private void resize()
    {
        Pair<LongArrayHashTable,Object[]> resized = table.doubleCapacity( values );
        table = resized.first();
        values = resized.other();
    }

    private int slotFor( long[] value )
    {
        return LongArrayHash.hashCode( value, 0, width ) & table.tableMask;
    }

    class Node
    {
        final VALUE value;
        final Node next;

        Node( VALUE value, Node next )
        {
            this.value = value;
            this.next = next;
        }
    }

    class Result extends org.neo4j.helpers.collection.PrefetchingIterator<VALUE>
    {
        private Node current;

        Result( Node first )
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
}
