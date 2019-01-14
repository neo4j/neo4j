/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.helpers.collection;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.ResourceUtils;

public abstract class ResourceClosingIterator<T, V> implements ResourceIterator<V>
{
    /**
     * @deprecated use {@link #newResourceIterator(Iterator, Resource...)}
     */
    @Deprecated
    public static <R> ResourceIterator<R> newResourceIterator( Resource resource, Iterator<R> iterator )
    {
        return newResourceIterator( iterator, resource );
    }

    public static <R> ResourceIterator<R> newResourceIterator( Iterator<R> iterator, Resource... resources )
    {
        return new ResourceClosingIterator<R,R>( iterator, resources )
        {
            @Override
            public R map( R elem )
            {
                return elem;
            }
        };
    }

    private Resource[] resources;
    private final Iterator<T> iterator;

    ResourceClosingIterator( Iterator<T> iterator, Resource... resources )
    {
        this.resources = resources;
        this.iterator = iterator;
    }

    @Override
    public void close()
    {
        if ( resources != null )
        {
            ResourceUtils.closeAll( resources );
            resources = null;
        }
    }

    @Override
    public boolean hasNext()
    {
        boolean hasNext = iterator.hasNext();
        if ( !hasNext )
        {
            close();
        }
        return hasNext;
    }

    public abstract V map( T elem );

    @Override
    public V next()
    {
        try
        {
            return map( iterator.next() );
        }
        catch ( NoSuchElementException e )
        {
            close();
            throw e;
        }
    }

    @Override
    public void remove()
    {
        iterator.remove();
    }
}
