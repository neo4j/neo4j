/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.collection.trackable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;

import org.neo4j.memory.MemoryTracker;

import static org.neo4j.internal.helpers.ArrayUtil.MAX_ARRAY_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;
import static org.neo4j.util.Preconditions.requireNonNegative;

/**
 * A heap tracking array list. It only tracks the internal structure, not the elements within.
 *
 * This is mostly a copy of {@link ArrayList} to expose the {@link #grow(int)} method.
 *
 * @param <E> element type
 */
@SuppressWarnings( "unchecked" )
public class HeapTrackingArrayList<E> implements List<E>, AutoCloseable
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( HeapTrackingArrayList.class );

    private final MemoryTracker memoryTracker;

    private long trackedSize;
    private int size;
    private int modCount;
    private Object[] elementData;

    /**
     * @return a new heap tracking array list with initial size 1
     */
    public static <T> HeapTrackingArrayList<T> newArrayList( MemoryTracker memoryTracker )
    {
        return newArrayList( 1, memoryTracker );
    }

    /**
     * @return a new heap tracking array list with the specified initial size
     */
    public static <T> HeapTrackingArrayList<T> newArrayList( int initialSize, MemoryTracker memoryTracker )
    {
        requireNonNegative( initialSize );
        long trackedSize = shallowSizeOfObjectArray( initialSize );
        memoryTracker.allocateHeap( SHALLOW_SIZE + trackedSize );
        return new HeapTrackingArrayList<>( initialSize, memoryTracker, trackedSize );
    }

    @SuppressWarnings( "CopyConstructorMissesField" )
    private HeapTrackingArrayList( HeapTrackingArrayList<E> other )
    {
        int otherSize = other.size;
        this.size = otherSize;
        this.elementData = new Object[otherSize];
        System.arraycopy( other.elementData, 0, this.elementData, 0, otherSize );
        this.memoryTracker = other.memoryTracker;
        this.trackedSize = shallowSizeOfObjectArray( otherSize );
        memoryTracker.allocateHeap( SHALLOW_SIZE + trackedSize );
    }

    private HeapTrackingArrayList( int initialSize, MemoryTracker memoryTracker, long trackedSize )
    {
        this.elementData = new Object[initialSize];
        this.memoryTracker = memoryTracker;
        this.trackedSize = trackedSize;
    }

    /*
     * Compacts the elementData list of the original array
     */
    @Override
    public HeapTrackingArrayList<E> clone()
    {
        return new HeapTrackingArrayList<>( this );
    }

    @Override
    public boolean add( E item )
    {
        modCount++;
        add( item, elementData, size );
        return true;
    }

    @Override
    public boolean containsAll( Collection<?> c )
    {
        for ( Object e : c )
        {
            if ( !contains( e ) )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll( Collection<? extends E> c )
    {
        Object[] a = c.toArray();
        modCount++;
        int numNew = a.length;
        if ( numNew == 0 )
        {
            return false;
        }
        Object[] elementData;
        final int s;
        if ( numNew > (elementData = this.elementData).length - (s = size) )
        {
            elementData = grow( s + numNew );
        }
        System.arraycopy( a, 0, elementData, s, numNew );
        size = s + numNew;
        return true;
    }

    @Override
    public boolean addAll( int index, Collection<? extends E> c )
    {
        rangeCheckForAdd( index );

        Object[] a = c.toArray();
        modCount++;
        int numNew = a.length;
        if ( numNew == 0 )
        {
            return false;
        }
        Object[] elementData;
        final int s;
        if ( numNew > (elementData = this.elementData).length - (s = size) )
        {
            elementData = grow( s + numNew );
        }

        int numMoved = s - index;
        if ( numMoved > 0 )
        {
            System.arraycopy( elementData, index, elementData, index + numNew, numMoved );
        }
        System.arraycopy( a, 0, elementData, index, numNew );
        size = s + numNew;
        return true;
    }

    @Override
    public boolean removeAll( Collection<?> c )
    {
        return batchRemove( c, false, 0, size );
    }

    @Override
    public boolean retainAll( Collection<?> c )
    {
        return batchRemove( c, true, 0, size );
    }

    @Override
    public E get( int index )
    {
        Objects.checkIndex( index, size );
        return elementData( index );
    }

    @Override
    public E set( int index, E element )
    {
        Objects.checkIndex( index, size );
        E oldValue = elementData( index );
        elementData[index] = element;
        return oldValue;
    }

    @Override
    public void add( int index, E element )
    {
        rangeCheckForAdd( index );
        modCount++;
        final int s;
        Object[] elementData;
        if ( (s = size) == (elementData = this.elementData).length )
        {
            elementData = grow( size + 1 );
        }
        System.arraycopy( elementData, index, elementData, index + 1, s - index );
        elementData[index] = element;
        size = s + 1;
    }

    @Override
    public E remove( int index )
    {
        Objects.checkIndex( index, size );
        final Object[] es = elementData;

        E oldValue = (E) es[index];
        fastRemove( es, index );

        return oldValue;
    }

    @Override
    public int indexOf( Object o )
    {
        Object[] es = elementData;
        int size = this.size;
        if ( o == null )
        {
            for ( int i = 0; i < size; i++ )
            {
                if ( es[i] == null )
                {
                    return i;
                }
            }
        }
        else
        {
            for ( int i = 0; i < size; i++ )
            {
                if ( o.equals( es[i] ) )
                {
                    return i;
                }
            }
        }
        return -1;
    }

    @Override
    public int lastIndexOf( Object o )
    {
        Object[] es = elementData;
        int size = this.size;
        if ( o == null )
        {
            for ( int i = size - 1; i >= 0; i-- )
            {
                if ( es[i] == null )
                {
                    return i;
                }
            }
        }
        else
        {
            for ( int i = size - 1; i >= 0; i-- )
            {
                if ( o.equals( es[i] ) )
                {
                    return i;
                }
            }
        }
        return -1;
    }

    @Override
    public ListIterator<E> listIterator()
    {
        return new ListItr( 0 );
    }

    @Override
    public ListIterator<E> listIterator( int index )
    {
        rangeCheckForAdd( index );
        return new ListItr( index );
    }

    @Override
    public List<E> subList( int fromIndex, int toIndex )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<E> iterator()
    {
        return new Itr();
    }

    @Override
    public Object[] toArray()
    {
        return Arrays.copyOf( elementData, size );
    }

    @Override
    public <T> T[] toArray( T[] a )
    {
        if ( a.length < size )
        // Make a new array of a's runtime type, but my contents:
        {
            return (T[]) Arrays.copyOf( elementData, size, a.getClass() );
        }
        System.arraycopy( elementData, 0, a, 0, size );
        if ( a.length > size )
        {
            a[size] = null;
        }
        return a;
    }

    @Override
    public void close()
    {
        if ( elementData != null )
        {
            memoryTracker.releaseHeap( trackedSize + SHALLOW_SIZE );
            elementData = null;
        }
    }

    public Iterator<E> autoClosingIterator()
    {
        return new Iterator<>()
        {
            int index;

            @Override
            public boolean hasNext()
            {
                if ( index >= size )
                {
                    close();
                    return false;
                }
                return true;
            }

            @Override
            public E next()
            {
                if ( !hasNext() )
                {
                    throw new NoSuchElementException();
                }
                return elementData(index++);
            }
        };
    }

    public void sort( Comparator<? super E> c )
    {
        final int expectedModCount = modCount;
        Arrays.sort( (E[]) elementData, 0, size, c );
        if ( modCount != expectedModCount )
        {
            throw new ConcurrentModificationException();
        }
        modCount++;
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
        return indexOf( o ) >= 0;
    }

    @Override
    public boolean remove( Object o )
    {
        final Object[] es = elementData;
        final int size = this.size;
        int i = 0;
        found:
        {
            if ( o == null )
            {
                for ( ; i < size; i++ )
                {
                    if ( es[i] == null )
                    {
                        break found;
                    }
                }
            }
            else
            {
                for ( ; i < size; i++ )
                {
                    if ( o.equals( es[i] ) )
                    {
                        break found;
                    }
                }
            }
            return false;
        }
        fastRemove( es, i );
        return true;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o == this )
        {
            return true;
        }

        if ( !(o instanceof List) )
        {
            return false;
        }

        final int expectedModCount = modCount;
        boolean equal = (o.getClass() == HeapTrackingArrayList.class)
                ? equalsArrayList( (HeapTrackingArrayList<?>) o )
                : equalsRange( (List<?>) o, 0, size );

        checkForComodification( expectedModCount );
        return equal;
    }

    @Override
    public int hashCode()
    {
        int expectedModCount = modCount;
        int hash = hashCodeRange( 0, size );
        checkForComodification( expectedModCount );
        return hash;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder( "[" );
        forEach( elem -> sb.append( elem ).append( "," ) );
        if ( size() > 0 )
        {
            sb.deleteCharAt( sb.length() - 1 );
        }
        sb.append( "]" );
        return sb.toString();
    }

    @Override
    public void clear()
    {
        modCount++;
        final Object[] es = elementData;
        for ( int to = size, i = size = 0; i < to; i++ )
        {
            es[i] = null;
        }
    }

    @Override
    public void forEach( Consumer<? super E> action )
    {
        Objects.requireNonNull( action );
        final int expectedModCount = modCount;
        final Object[] es = elementData;
        final int size = this.size;
        for ( int i = 0; modCount == expectedModCount && i < size; i++ )
        {
            action.accept( elementAt( es, i ) );
        }
        if ( modCount != expectedModCount )
        {
            throw new ConcurrentModificationException();
        }
    }

    /**
     * Grow and report size change to tracker
     */
    private Object[] grow( int minimumCapacity )
    {
        int newCapacity = newCapacity( minimumCapacity, elementData.length );

        long oldHeapUsage = trackedSize;
        trackedSize = shallowSizeOfObjectArray( newCapacity );
        memoryTracker.allocateHeap( trackedSize );
        Object[] newItems = new Object[newCapacity];
        System.arraycopy( elementData, 0, newItems, 0, Math.min( size, newCapacity ) );
        elementData = newItems;
        memoryTracker.releaseHeap( oldHeapUsage );
        return elementData;
    }

    static int newCapacity( int minimumCapacity, int oldCapacity )
    {
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if ( newCapacity - minimumCapacity <= 0 )
        {
            if ( minimumCapacity < 0 ) // overflow
            {
                throw new OutOfMemoryError();
            }
            return minimumCapacity;
        }
        return newCapacity - MAX_ARRAY_SIZE <= 0 ? newCapacity : hugeCapacity( minimumCapacity );
    }

    private static int hugeCapacity( int minCapacity )
    {
        if ( minCapacity < 0 ) // overflow
        {
            throw new OutOfMemoryError();
        }
        return minCapacity > MAX_ARRAY_SIZE ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
    }

    @SuppressWarnings( "unchecked" )
    private E elementData( int index )
    {
        return (E) elementData[index];
    }

    @SuppressWarnings( "unchecked" )
    private static <E> E elementAt( Object[] es, int index )
    {
        return (E) es[index];
    }

    private void add( E e, Object[] elementData, int s )
    {
        if ( s == elementData.length )
        {
            elementData = grow( size + 1 );
        }
        elementData[s] = e;
        size = s + 1;
    }

    private void fastRemove( Object[] es, int i )
    {
        modCount++;
        final int newSize;
        if ( (newSize = size - 1) > i )
        {
            System.arraycopy( es, i + 1, es, i, newSize - i );
        }
        es[size = newSize] = null;
    }

    private void checkForComodification( final int expectedModCount )
    {
        if ( modCount != expectedModCount )
        {
            throw new ConcurrentModificationException();
        }
    }

    private boolean equalsRange( List<?> other, int from, int to )
    {
        final Object[] es = elementData;
        if ( to > es.length )
        {
            throw new ConcurrentModificationException();
        }
        var oit = other.iterator();
        for ( ; from < to; from++ )
        {
            if ( !oit.hasNext() || !Objects.equals( es[from], oit.next() ) )
            {
                return false;
            }
        }
        return !oit.hasNext();
    }

    private boolean equalsArrayList( HeapTrackingArrayList<?> other )
    {
        final int otherModCount = other.modCount;
        final int s = size;
        boolean equal;
        if ( equal = s == other.size )
        {
            final Object[] otherEs = other.elementData;
            final Object[] es = elementData;
            if ( s > es.length || s > otherEs.length )
            {
                throw new ConcurrentModificationException();
            }
            for ( int i = 0; i < s; i++ )
            {
                if ( !Objects.equals( es[i], otherEs[i] ) )
                {
                    equal = false;
                    break;
                }
            }
        }
        other.checkForComodification( otherModCount );
        return equal;
    }

    private int hashCodeRange( int from, int to )
    {
        final Object[] es = elementData;
        if ( to > es.length )
        {
            throw new ConcurrentModificationException();
        }
        int hashCode = 1;
        for ( int i = from; i < to; i++ )
        {
            Object e = es[i];
            hashCode = 31 * hashCode + (e == null ? 0 : e.hashCode());
        }
        return hashCode;
    }

    private boolean batchRemove( Collection<?> c, boolean complement, final int from, final int end )
    {
        Objects.requireNonNull( c );
        final Object[] es = elementData;
        int r;
        // Optimize for initial run of survivors
        for ( r = from; ; r++ )
        {
            if ( r == end )
            {
                return false;
            }
            if ( c.contains( es[r] ) != complement )
            {
                break;
            }
        }
        int w = r++;
        try
        {
            for ( Object e; r < end; r++ )
            {
                if ( c.contains( e = es[r] ) == complement )
                {
                    es[w++] = e;
                }
            }
        }
        catch ( Throwable ex )
        {
            System.arraycopy( es, r, es, w, end - r );
            w += end - r;
            throw ex;
        }
        finally
        {
            modCount += end - w;
            shiftTailOverGap( es, w, end );
        }
        return true;
    }

    private void shiftTailOverGap( Object[] es, int lo, int hi )
    {
        System.arraycopy( es, hi, es, lo, size - hi );
        for ( int to = size, i = size -= hi - lo; i < to; i++ )
        {
            es[i] = null;
        }
    }

    private void rangeCheckForAdd( int index )
    {
        if ( index > size || index < 0 )
        {
            throw new IndexOutOfBoundsException( "Index: " + index + ", Size: " + size );
        }
    }

    private class Itr implements Iterator<E>
    {
        int cursor;
        int lastRet = -1;
        int expectedModCount = modCount;

        Itr()
        {
        }

        public boolean hasNext()
        {
            return cursor != size;
        }

        @SuppressWarnings( "unchecked" )
        public E next()
        {
            checkForComodification();
            int i = cursor;
            if ( i >= size )
            {
                throw new NoSuchElementException();
            }
            Object[] elementData = HeapTrackingArrayList.this.elementData;
            if ( i >= elementData.length )
            {
                throw new ConcurrentModificationException();
            }
            cursor = i + 1;
            return (E) elementData[lastRet = i];
        }

        public void remove()
        {
            if ( lastRet < 0 )
            {
                throw new IllegalStateException();
            }
            checkForComodification();

            try
            {
                HeapTrackingArrayList.this.remove( lastRet );
                cursor = lastRet;
                lastRet = -1;
                expectedModCount = modCount;
            }
            catch ( IndexOutOfBoundsException ex )
            {
                throw new ConcurrentModificationException();
            }
        }

        @Override
        public void forEachRemaining( Consumer<? super E> action )
        {
            Objects.requireNonNull( action );
            final int size = HeapTrackingArrayList.this.size;
            int i = cursor;
            if ( i < size )
            {
                final Object[] es = elementData;
                if ( i >= es.length )
                {
                    throw new ConcurrentModificationException();
                }
                for ( ; i < size && modCount == expectedModCount; i++ )
                {
                    action.accept( elementAt( es, i ) );
                }
                // update once at end to reduce heap write traffic
                cursor = i;
                lastRet = i - 1;
                checkForComodification();
            }
        }

        final void checkForComodification()
        {
            if ( modCount != expectedModCount )
            {
                throw new ConcurrentModificationException();
            }
        }
    }

    private class ListItr extends Itr implements ListIterator<E>
    {
        ListItr( int index )
        {
            super();
            cursor = index;
        }

        public boolean hasPrevious()
        {
            return cursor != 0;
        }

        public int nextIndex()
        {
            return cursor;
        }

        public int previousIndex()
        {
            return cursor - 1;
        }

        @SuppressWarnings( "unchecked" )
        public E previous()
        {
            checkForComodification();
            int i = cursor - 1;
            if ( i < 0 )
            {
                throw new NoSuchElementException();
            }
            Object[] elementData = HeapTrackingArrayList.this.elementData;
            if ( i >= elementData.length )
            {
                throw new ConcurrentModificationException();
            }
            cursor = i;
            return (E) elementData[lastRet = i];
        }

        public void set( E e )
        {
            if ( lastRet < 0 )
            {
                throw new IllegalStateException();
            }
            checkForComodification();

            try
            {
                HeapTrackingArrayList.this.set( lastRet, e );
            }
            catch ( IndexOutOfBoundsException ex )
            {
                throw new ConcurrentModificationException();
            }
        }

        public void add( E e )
        {
            checkForComodification();

            try
            {
                int i = cursor;
                HeapTrackingArrayList.this.add( i, e );
                cursor = i + 1;
                lastRet = -1;
                expectedModCount = modCount;
            }
            catch ( IndexOutOfBoundsException ex )
            {
                throw new ConcurrentModificationException();
            }
        }
    }
}
