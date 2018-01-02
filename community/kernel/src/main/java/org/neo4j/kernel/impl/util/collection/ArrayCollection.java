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
package org.neo4j.kernel.impl.util.collection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.helpers.collection.PrefetchingIterator;

import static java.lang.System.arraycopy;
import static java.lang.reflect.Array.newInstance;

/**
 * Like {@link ArrayList}, but has efficient {@link #clear()}, i.e. just setting the cursor to 0.
 * Only adding and clearing works, not removing.
 */
public class ArrayCollection<T> implements Collection<T>
{
    private final int initialCapacity;
    private Object[] array;
    private int size;
    private final int fastClearThreshold;

    /**
     * Sets fastClearThreshold == initialCapacity.
     *
     * @param initialCapacity initial capacity of the backing array.
     */
    public ArrayCollection( int initialCapacity )
    {
        this( initialCapacity, initialCapacity );
    }

    /**
     * @param initialCapacity initial capacity of the backing array.
     * @param fastClearThreshold if {@link #size()} is {@code <= fastClearThreshold} then the items in
     * the backing array are still referenced by the backing array after a call to {@link #clear()} just
     * the internal cursor is reset, otherwise a new backing array of capacity {@code initialCapacity} is
     * created to take its place.
     */
    public ArrayCollection( int initialCapacity, int fastClearThreshold )
    {
        this.initialCapacity = initialCapacity;
        this.fastClearThreshold = Math.min( fastClearThreshold, initialCapacity );
        this.array = new Object[initialCapacity];
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public boolean isEmpty()
    {
        return size == 0;
    }

    @Override
    public boolean contains( Object o )
    {
        assert o != null;
        for ( int i = 0; i < size; i++ )
        {
            if ( o.equals( array[i] ) )
            {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings( "unchecked" )
    private T item( int at )
    {
        return (T) array[at];
    }

    @Override
    public Iterator<T> iterator()
    {
        return new PrefetchingIterator<T>()
        {
            private int iteratorCursor;

            @Override
            protected T fetchNextOrNull()
            {
                return iteratorCursor < size ? item( iteratorCursor++ ) : null;
            }
        };
    }

    @Override
    public Object[] toArray()
    {
        return Arrays.copyOf( array, size );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <R> R[] toArray( R[] a )
    {
        R[] result = a.length >= size ? a :
                (R[]) newInstance( a.getClass().getComponentType(), size );
        arraycopy( array, 0, result, 0, size );
        return result;
    }

    @Override
    public boolean add( T e )
    {
        ensureCapacity( size+1 );
        add0( e );
        return true;
    }

    private void add0( T e )
    {
        assert e != null;
        array[size++] = e;
    }

    private void ensureCapacity( int capacity )
    {
        if ( capacity > array.length )
        {
            // Double capacity until we reach target
            int newCapacity = array.length;
            while ( newCapacity < capacity )
            {
                newCapacity <<= 1;
            }

            array = Arrays.copyOf( array, newCapacity );
        }
    }

    /**
     * Implemented by moving elements after the removed item one step back, just to keep the simplicity
     * of having an array and a cursor and keeping the items in the array compact so that {@link #clear()}
     * can be done by setting the cursor to 0.
     */
    @Override
    public boolean remove( Object o )
    {
        throw new UnsupportedOperationException( "Please implement if needed" );
    }

    @Override
    public boolean containsAll( Collection<?> c )
    {
        throw new UnsupportedOperationException( "Please implement if needed" );
    }

    @Override
    public boolean addAll( Collection<? extends T> c )
    {
        if ( c.isEmpty() )
        {
            return false;
        }

        ensureCapacity( size + c.size() );

        for ( T item : c )
        {
            add0( item );
        }
        return true;
    }

    @Override
    public boolean removeAll( Collection<?> c )
    {
        throw new UnsupportedOperationException( "Please implement if needed" );
    }

    @Override
    public boolean retainAll( Collection<?> c )
    {
        throw new UnsupportedOperationException( "Please implement if needed" );
    }

    @Override
    public void clear()
    {
        if ( size > fastClearThreshold )
        {
            array = new Object[initialCapacity];
        }
        size = 0;
    }

    @Override
    public String toString()
    {
        return "ArrayCollection{" +
               "array=" + Arrays.toString( array ) +
               '}';
    }
}
