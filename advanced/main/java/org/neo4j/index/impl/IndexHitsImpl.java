package org.neo4j.index.impl;
/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */


import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.index.IndexHits;

/**
 * A simple implementation of an {@link IndexHits} where the size is known at
 * construction time.
 *
 * @param <T> the type of items.
 */
public class IndexHitsImpl<T> implements IndexHits<T>
{
    private final Iterator<T> hits;
    private final int size;
    
    /**
     * Wraps an Iterable<T> with a known size.
     * 
     * @param hits the hits to iterate through.
     * @param size the size of the iteration.
     */
    public IndexHitsImpl( Iterable<T> hits, int size )
    {
        this( hits.iterator(), size );
    }
    
    /**
     * Wraps an Iterator<T> with a known size.
     * 
     * @param hits the hits to iterate through.
     * @param size the size of the iteration.
     */
    public IndexHitsImpl( Iterator<T> hits, int size )
    {
        this.hits = hits;
        this.size = size;
    }
    
    public Iterator<T> iterator()
    {
        return this.hits;
    }

    public int size()
    {
        return this.size;
    }

    public void close()
    {
        // Do nothing
    }

    public boolean hasNext()
    {
        return this.hits.hasNext();
    }

    public T next()
    {
        return this.hits.next();
    }

    public void remove()
    {
        this.hits.remove();
    }
    
    public T getSingle()
    {
        T result = this.hits.hasNext() ? this.hits.next() : null;
        if ( this.hits.hasNext() )
        {
            throw new NoSuchElementException();
        }
        return result;
    }
}
