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
package org.neo4j.kernel.impl.util.diffsets;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;

import static java.lang.String.format;

public class MutableDiffSetsImpl<T> implements MutableDiffSets<T>
{
    private Set<T> addedElements;
    private Set<T> removedElements;
    private Predicate<T> filter;

    private MutableDiffSetsImpl( Set<T> addedElements, Set<T> removedElements )
    {
        this.addedElements = addedElements;
        this.removedElements = removedElements;
    }

    public MutableDiffSetsImpl()
    {
        this( null, null );
    }

    @Override
    public boolean add( T elem )
    {
        boolean wasRemoved = removed( false ).remove( elem );
        // Add to the addedElements only if it was not removed from the removedElements
        return wasRemoved || added( true ).add( elem );
    }

    @Override
    public void addAll( Iterator<T> elems )
    {
        while ( elems.hasNext() )
        {
            add( elems.next() );
        }
    }

    @Override
    public boolean remove( T elem )
    {
        boolean removedFromAddedElements = added( false ).remove( elem );
        // Add to the removedElements only if it was not removed from the addedElements.
        return removedFromAddedElements || removed( true ).add( elem );
    }

    @Override
    public void removeAll( Iterator<T> elems )
    {
        while ( elems.hasNext() )
        {
            remove( elems.next() );
        }
    }

    @Override
    public void clear()
    {
        if ( addedElements != null )
        {
            addedElements.clear();
        }
        if ( removedElements != null )
        {
            removedElements.clear();
        }
    }

    @Override
    public boolean unRemove( T item )
    {
        return removed( false ).remove( item );
    }

    protected Set<T> added( boolean create )
    {
        if ( addedElements == null )
        {
            if ( !create )
            {
                return Collections.emptySet();
            }
            addedElements = new HashSet<>();
        }
        return addedElements;
    }

    private void ensureFilterHasBeenCreated()
    {
        if ( filter == null )
        {
            filter = item -> !removed( false ).contains( item ) && !added( false ).contains( item );
        }
    }

    @Override
    public String toString()
    {
        return format( "{+%s, -%s}", added( false ), removed( false ) );
    }

    @Override
    public boolean isAdded( T elem )
    {
        return added( false ).contains( elem );
    }

    @Override
    public boolean isRemoved( T elem )
    {
        return removed( false ).contains( elem );
    }

    @Override
    public Set<T> getAdded()
    {
        return resultSet( addedElements );
    }

    @Override
    public Set<T> getRemoved()
    {
        return resultSet( removedElements );
    }

    @Override
    public boolean isEmpty()
    {
        return added( false ).isEmpty() && removed( false ).isEmpty();
    }

    @Override
    public Iterator<T> apply( Iterator<? extends T> source )
    {
        Iterator<T> result = (Iterator) source;
        if ( (removedElements != null && !removedElements.isEmpty()) ||
                (addedElements != null && !addedElements.isEmpty()) )
        {
            ensureFilterHasBeenCreated();
            result = Iterators.filter( filter, result );
        }
        if ( addedElements != null && !addedElements.isEmpty() )
        {
            result = Iterators.concat( result, addedElements.iterator() );
        }
        return result;
    }

    @Override
    public int delta()
    {
        return added( false ).size() - removed( false ).size();
    }

    @Override
    public MutableDiffSetsImpl<T> filterAdded( Predicate<T> addedFilter )
    {
        return new MutableDiffSetsImpl<>(
                Iterables.asSet( Iterables.filter( addedFilter, added( false ) ) ),
                Iterables.asSet( removed( false ) ) );
    }

    protected Set<T> removed( boolean create )
    {
        if ( removedElements == null )
        {
            if ( !create )
            {
                return Collections.emptySet();
            }
            removedElements = new HashSet<>();
        }
        return removedElements;
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

    private Set<T> resultSet( Set<T> coll )
    {
        return coll == null ? Collections.emptySet() : Collections.unmodifiableSet( coll );
    }
}
