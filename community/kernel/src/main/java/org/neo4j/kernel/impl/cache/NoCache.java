/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.cache;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public class NoCache<E extends EntityWithSize> implements Cache<E>
{
    private final String name;
    private volatile long misses;
    
    private static final AtomicLong MISSES = new AtomicLong( 0 );

    public NoCache( String name )
    {
        this.name = name;
    }

    public void put( E value )
    {
    }

    public void putAll( Collection<E> values )
    {
    }

    public E get( long key )
    {
        MISSES.incrementAndGet();
        return null;
    }

    public E remove( long key )
    {
        return null;
    }

    @Override
    public long hitCount()
    {
        return 0;
    }

    @Override
    public long missCount()
    {
        return misses;
    }

    public long size()
    {
        return 0;
    }

    public void clear()
    {
    }

    public String getName()
    {
        return name;
    }

    @Override
    public void updateSize( E entity, int newSize )
    {
        // do nothing
    }
}
