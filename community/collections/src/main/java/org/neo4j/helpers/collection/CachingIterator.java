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
package org.neo4j.helpers.collection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * An {@link Iterator} which lazily fetches and caches items from the
 * underlying iterator when items are requested. This enables positioning
 * as well as going backwards through the iteration.
 * @author Mattias Persson
 *
 * @param <T> the type of items in the iterator.
 */
public class CachingIterator<T> implements ListIterator<T>
{
    private final Iterator<T> source;
    private final List<T> visited = new ArrayList<T>();
    private int position;
    private T current;

    /**
     * Creates a new caching iterator using {@code source} as its underlying
     * {@link Iterator} to get items lazily from.
     * @param source the underlying {@link Iterator} to lazily get items from.
     */
    public CachingIterator( Iterator<T> source )
    {
        this.source = source;
    }

    /**
     * Returns whether a call to {@link #next()} will be able to return
     * an item or not. If the current {@link #position()} is beyond the size
     * of the cache (as will be the case if only calls to
     * {@link #hasNext()}/{@link #next()} has been made up to this point)
     * the underlying iterator is asked, else {@code true} since it can be
     * returned from the cache.
     * 
     * @return whether or not there are more items in this iteration given the
     * current {@link #position()}.
     */
    public boolean hasNext()
    {
        return visited.size() > position ? true : source.hasNext();
    }
    
    /**
     * Returns the next item given the current {@link #position()}.
     * If the current {@link #position()} is beyond the size
     * of the cache (as will be the case if only calls to
     * {@link #hasNext()}/{@link #next()} has been made up to this point) the
     * underlying iterator is asked for the next item (and cached if there
     * was one), else the item is returned from the cache.
     * 
     * @return the next item given the current {@link #position()}.
     */
    public T next()
    {
        if ( visited.size() > position )
        {
            current = visited.get( position );
        }
        else
        {
            if ( !source.hasNext() )
            {
                throw new NoSuchElementException();
            }
            current = source.next();
            visited.add( current );
        }
        position++;
        return current;
    }
    
    /**
     * Returns the current position of the iterator, initially 0. The position
     * represents the index of the item which will be returned by the next call
     * to {@link #next()} and also the index of the next item returned by
     * {@link #previous()} plus one. An example:
     * 
     * <ul>
     * <li>Instantiate an iterator which would iterate over the strings "first", "second" and "third".</li>
     * <li>Get the two first items ("first" and "second") from it by using {@link #next()},
     * {@link #position()} will now return 2.</li>
     * <li>Call {@link #previous()} (which will return "second") and {@link #position()} will now be 1</li>
     * </ul>
     * 
     * @return the position of the iterator.
     */
    public int position()
    {
        return position;
    }
    
    /**
     * Sets the position of the iterator. {@code 0} means all the way back to
     * the beginning. It is also possible to set the position to one higher
     * than the last item, so that the next call to {@link #previous()} would
     * return the last item. Items will be cached along the way if necessary.
     * 
     * @param newPosition the position to set for the iterator, must be
     * non-negative.
     * @return the position before changing to the new position.
     */
    public int position( int newPosition )
    {
        if ( newPosition < 0 )
        {
            throw new IllegalArgumentException( "Position must be non-negative, was " + newPosition );
        }
        
        int previousPosition = position;
        boolean overTheEdge = false;
        while ( visited.size() < newPosition )
        {
            T next = source.hasNext() ? source.next() : null;
            if ( next != null )
            {
                visited.add( next );
            }
            else
            {
                if ( !overTheEdge )
                {
                    overTheEdge = true;
                }
                else
                {
                    throw new NoSuchElementException( "Requested position " + newPosition +
                            ", but didn't get further than to " + visited.size() );
                }
            }
        }
        current = null;
        position = newPosition;
        return previousPosition;
    }
    
    /**
     * Returns whether or not a call to {@link #previous()} will be able to
     * return an item or not. So it will return {@code true} if
     * {@link #position()} is bigger than 0.
     * 
     * {@inheritDoc}
     */
    public boolean hasPrevious()
    {
        return position > 0;
    }
    
    /**
     * {@inheritDoc}
     */
    public T previous()
    {
        if ( !hasPrevious() )
        {
            throw new NoSuchElementException( "Position is " + position );
        }
        current = visited.get( --position );
        return current;
    }

    /**
     * Returns the last item returned by {@link #next()}/{@link #previous()}.
     * If no call has been made to {@link #next()} or {@link #previous()} since
     * this iterator was created or since a call to {@link #position(int)} has
     * been made a {@link NoSuchElementException} will be thrown.
     * 
     * @return the last item returned by {@link #next()}/{@link #previous()}.
     * @throws NoSuchElementException if no call has been made to {@link #next()}
     * or {@link #previous()} since this iterator was created or since a call to
     * {@link #position(int)} has been made.
     */
    public T current()
    {
        if ( current == null )
        {
            throw new NoSuchElementException();
        }
        return current;
    }

    /**
     * {@inheritDoc}
     */
    public int nextIndex()
    {
        return position;
    }
    
    /**
     * {@inheritDoc}
     */
    public int previousIndex()
    {
        return position-1;
    }

    /**
     * Not supported by this implement.
     * 
     * {@inheritDoc}
     */
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Not supported by this implement.
     * 
     * {@inheritDoc}
     */
    public void set( T e )
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Not supported by this implement.
     * 
     * {@inheritDoc}
     */
    public void add( T e )
    {
        throw new UnsupportedOperationException();
    }
}
