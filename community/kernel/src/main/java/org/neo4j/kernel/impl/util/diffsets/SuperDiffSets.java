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
package org.neo4j.kernel.impl.util.diffsets;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.impl.util.VersionedHashMap;

import static java.lang.String.format;
import static java.util.Collections.newSetFromMap;
import static org.neo4j.helpers.collection.Iterables.concat;

/**
 * Super class of readable diffsets where use of {@link PrimitiveLongIterator} can be parameterized
 * to a specific subclass instead.
 */
abstract class SuperDiffSets<T,LONGITERATOR extends PrimitiveLongIterator>
        implements SuperReadableDiffSets<T,LONGITERATOR>
{
    private Set<T> addedElements;
    private Set<T> removedElements;
    private Predicate<T> filter;

    public SuperDiffSets()
    {
        this( null, null );
    }

    public SuperDiffSets( Set<T> addedElements, Set<T> removedElements )
    {
        this.addedElements = addedElements;
        this.removedElements = removedElements;
    }

    @Override
    public void accept( DiffSetsVisitor<T> visitor )
            throws ConstraintValidationKernelException, CreateConstraintFailureException
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
        boolean wasRemoved = removed( false ).remove( elem );
        // Add to the addedElements only if it was not removed from the removedElements
        return wasRemoved || added( true ).add( elem );
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
        // Add to the removedElements only if it was not removed from the addedElements.
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

    protected Set<T> added( boolean create )
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

    protected Set<T> removed( boolean create )
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
                public boolean test( T item )
                {
                    return !removed( false ).contains( item ) && !added( false ).contains( item );
                }
            };
        }
    }

    @Override
    public int delta()
    {
        return added( false ).size() - removed( false ).size();
    }

    private Set<T> newSet()
    {
        return newSetFromMap( new VersionedHashMap<T, Boolean>() );
    }

    private Set<T> resultSet( Set<T> coll )
    {
        return coll == null ? Collections.<T>emptySet() : Collections.unmodifiableSet( coll );
    }

    public boolean unRemove( T item )
    {
        return removed( false ).remove( item );
    }

    public void clear()
    {
        if(addedElements != null)
        {
            addedElements.clear();
        }
        if(removedElements != null)
        {
            removedElements.clear();
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        SuperDiffSets diffSets = (SuperDiffSets) o;

        if ( addedElements != null ? !addedElements.equals( diffSets.addedElements ) : diffSets.addedElements != null )
        {
            return false;
        }
        if ( filter != null ? !filter.equals( diffSets.filter ) : diffSets.filter != null )
        {
            return false;
        }
        if ( removedElements != null ? !removedElements.equals( diffSets.removedElements ) : diffSets.removedElements
                != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = addedElements != null ? addedElements.hashCode() : 0;
        result = 31 * result + (removedElements != null ? removedElements.hashCode() : 0);
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return format( "{+%s, -%s}", added( false ), removed( false ) );
    }
}
