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
package org.neo4j.kernel.impl.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;

import static java.lang.String.format;
import static org.neo4j.helpers.collection.Iterables.concat;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

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
    public interface Visitor<T>
    {
        void visitAdded( T element );

        void visitRemoved( T element );
    }

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

    public DiffSets()
    {
        this( null, null );
    }

    public DiffSets( Set<T> addedElements, Set<T> removedElements )
    {
        this.addedElements = addedElements;
        this.removedElements = removedElements;
    }

    public void accept( Visitor<T> visitor )
    {
        for ( T element : added( false ) )
        {
            visitor.visitAdded( element );
        }
        for ( T element : removed( false ) )
        {
            visitor.visitRemoved( element );
        }
    }

    public boolean add( T elem )
    {
        boolean result = added( true ).add( elem );
        removed( false ).remove( elem );
        return result;
    }

    public void replace( T toRemove, T toAdd )
    {
        Set<T> added = added( true ); // we're doing both add and remove on it, so pass in true
        boolean removedFromAdded = added.remove( toRemove );
        removed( false ).remove( toAdd );

        added.add( toAdd );
        if ( !removedFromAdded )
        {
            removed( true ).add( toRemove );
        }
    }

    public boolean remove( T elem )
    {
        boolean removedFromAddedElements = added( false ).remove( elem );
        // Add to the removedElements only if it was removed from the addedElements.
        return removedFromAddedElements || removed( true ).add( elem );
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
        return added( false ).contains( elem );
    }

    public boolean isRemoved( T elem )
    {
        return removed( false ).contains( elem );
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
        return added( false ).isEmpty() && removed( false ).isEmpty();
    }

    public Iterator<T> apply( Iterator<T> source )
    {
        Iterator<T> result = source;
        if ( ( removedElements != null && !removedElements.isEmpty() ) ||
             ( addedElements != null && !addedElements.isEmpty() ) )
        {
            ensureFilterHasBeenCreated();
            result = Iterables.filter( filter, result );
        }
        if ( addedElements != null && !addedElements.isEmpty() )
        {
            result = concat( result, addedElements.iterator() );
        }
        return result;
    }

    public PrimitiveLongIterator applyPrimitiveLongIterator( final PrimitiveLongIterator source )
    {
        return new DiffApplyingPrimitiveLongIterator( source, added( false ), removed( false ) );
    }

    public PrimitiveIntIterator applyPrimitiveIntIterator( final PrimitiveIntIterator source )
    {
        return new DiffApplyingPrimitiveIntIterator( source, added( false ), removed( false ) );
    }

    public DiffSets<T> filterAdded( Predicate<T> addedFilter )
    {
        return new DiffSets<>(
                asSet( Iterables.filter( addedFilter, added( false ) ) ),
                asSet( removed( false ) ) );
    }

    public DiffSets<T> filter( Predicate<T> filter )
    {
        return new DiffSets<>(
                asSet( Iterables.filter( filter, added( false ) ) ),
                asSet( Iterables.filter( filter, removed( false ) ) ) );
    }

    private Set<T> added( boolean create )
    {
        if ( addedElements == null )
        {
            if ( !create )
            {
                return Collections.emptySet();
            }
            addedElements = newSet();
        }
        return addedElements;
    }

    private Set<T> removed( boolean create )
    {
        if ( removedElements == null )
        {
            if ( !create )
            {
                return Collections.emptySet();
            }
            removedElements = newSet();
        }
        return removedElements;
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
                    return !removed( false ).contains( item ) && !added( false ).contains( item );
                }
            };
        }
    }

    public int delta()
    {
        return added( false ).size() - removed( false ).size();
    }

    private Set<T> newSet()
    {
        return new HashSet<>();
    }

    private Set<T> resultSet( Set<T> coll )
    {
        return coll == null ? Collections.<T>emptySet() : Collections.unmodifiableSet( coll );
    }

    public boolean unRemove( T item )
    {
        return removed( false ).remove( item );
    }

    @Override
    public String toString()
    {
        return format( "{+%s, -%s}", added( false ), removed( false ) );
    }
}
