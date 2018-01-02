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
package org.neo4j.collection.primitive;

import static java.util.Arrays.copyOf;

/**
 * Like a {@code Stack<Long>} but for primitive longs. Virtually GC free in that it has an {@code long[]}
 * and merely moves a cursor where to {@link #push(long)} and {@link #poll()} values to and from.
 * If many items goes in the stack the {@code long[]} will grow to accomodate all of them, but not shrink again.
 */
public class PrimitiveLongStack implements PrimitiveLongCollection
{
    private long[] array;
    private int cursor = -1; // where the top most item lives

    public PrimitiveLongStack( int initialSize )
    {
        this.array = new long[initialSize];
    }

    @Override
    public boolean isEmpty()
    {
        return cursor == -1;
    }

    @Override
    public void clear()
    {
        cursor = -1;
    }

    @Override
    public int size()
    {
        return cursor+1;
    }

    @Override
    public void close()
    {   // Nothing to close
    }

    @Override
    public PrimitiveLongIterator iterator()
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    @Override
    public void visitKeys( PrimitiveLongVisitor visitor )
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    public void push( long value )
    {
        ensureCapacity();
        array[++cursor] = value;
    }

    private void ensureCapacity()
    {
        if ( cursor == array.length-1 )
        {
            array = copyOf( array, array.length << 1 );
        }
    }

    /**
     * @return the top most item, or -1 if stack is empty
     */
    public long poll()
    {
        return cursor == -1 ? -1 : array[cursor--];
    }
}
