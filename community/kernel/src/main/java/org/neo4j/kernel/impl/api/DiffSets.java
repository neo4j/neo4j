/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import static org.neo4j.helpers.collection.Iterables.concat;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;

/**
 * Given a sequence of add and removal operations, instances of DiffSets track
 * which elements need to actually be added and removed at minimum from some
 * hypothetical target collection such that the result is equivalent to just
 * executing the sequence of additions and removals in order
 *
 * @param <T> type of elements
 */
public class DiffSets<T>
{
    @SuppressWarnings(
            {"rawtypes", "unchecked"})
    private static final DiffSets EMPTY = new DiffSets( Collections.emptySet(), Collections.emptySet() )
    {
        @Override
        public Iterator apply( Iterator source )
        {
            return source;
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> DiffSets<T> emptyDiffSets()
    {
        return EMPTY;
    }

    private Set<T> addedElements;
    private Set<T> removedElements;
    private Predicate<T> filter;

    public static <E> DiffSets<E> fromAdding( E... elems )
    {
        Set<E> addedElements = new HashSet<E>( elems.length );
        Collections.addAll( addedElements, elems );
        return new DiffSets<E>( addedElements, null );
    }

    public DiffSets()
    {
        this( null, null );
    }

    public DiffSets( Set<T> addedElements, Set<T> removedElements )
    {
        this.addedElements = addedElements;
        this.removedElements = removedElements;
    }

    public boolean add( T elem )
    {
        ensureAddedHasBeenCreated();
        boolean result = addedElements.add( elem );
        if ( removedElements != null )
        {
            removedElements.remove( elem );
        }
        return result;
    }

    public boolean remove( T elem )
    {
        ensureRemoveHasBeenCreated();
        boolean removedFromAddedElements = false;
        if ( addedElements != null )
        {
            removedFromAddedElements = addedElements.remove( elem );
        }
        boolean result = removedFromAddedElements || removedElements.add( elem );
        return result;
    }

    public void addAll( Iterator<T> elems )
    {
        while ( elems.hasNext() )
        {
            add( elems.next() );
        }
    }

    public void removeAll( Iterator<T> elems )
    {
        while ( elems.hasNext() )
        {
            remove( elems.next() );
        }
    }

    public boolean isAdded( T elem )
    {
        return addedElements != null && addedElements.contains( elem );
    }

    public boolean isRemoved( T elem )
    {
        return removedElements != null && removedElements.contains( elem );
    }

    public Set<T> getAdded()
    {
        return resultSet( addedElements );
    }

    public Set<T> getRemoved()
    {
        return resultSet( removedElements );
    }

    public boolean isEmpty()
    {
        return getAdded().isEmpty() && getRemoved().isEmpty();
    }

    @SuppressWarnings("unchecked")
    public Iterator<T> apply( Iterator<T> source )
    {
        Iterator<T> result = source;
        if ( removedElements != null && !removedElements.isEmpty() )
        {
            ensureFilterHasBeenCreated();
            result = filter( filter, result );
        }
        if ( addedElements != null && !addedElements.isEmpty() )
        {
            result = concat( result, addedElements.iterator() );
        }
        return result;
    }

    public DiffSets<T> filterAdded( Predicate<T> addedFilter )
    {
        Iterable<T> newAdded = Iterables.filter( addedFilter, getAdded() );
        Set<T> newRemoved = getRemoved();
        return new DiffSets<T>( asSet( newAdded ), asSet( newRemoved ) );
    }

    private void ensureAddedHasBeenCreated()
    {
        if ( addedElements == null )
        {
            addedElements = newSet();
        }
    }

    private void ensureRemoveHasBeenCreated()
    {
        if ( removedElements == null )
        {
            removedElements = newSet();
            ensureFilterHasBeenCreated();
        }
    }

    private void ensureFilterHasBeenCreated()
    {
        if ( filter == null )
        {
            filter = new Predicate<T>()
            {
                @Override
                public boolean accept( T item )
                {
                    return !removedElements.contains( item );
                }
            };
        }
    }

    private Set<T> newSet()
    {
        return new HashSet<T>();
    }

    private Set<T> resultSet( Set<T> coll )
    {
        return coll == null ? Collections.<T>emptySet() : Collections.unmodifiableSet( coll );
    }

}
