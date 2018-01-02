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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MultiSet<T>
{
    private final Map<T, Long> inner;
    private int size = 0;

    public MultiSet()
    {
        inner = new HashMap<>();
    }

    public MultiSet( int initialCapacity )
    {
        inner = new HashMap<>( initialCapacity );
    }

    public boolean contains( T value )
    {
        return inner.containsKey( value );
    }

    public long count( T value )
    {
        return unbox( inner.get( value ) );
    }

    public long add( T value )
    {
        return increment( value, +1 );
    }

    public long remove( T value )
    {
       return increment( value, -1 );
    }

    public long replace( T value, long newCount )
    {
        if ( newCount <= 0 )
        {
            long previous = unbox( inner.remove( value ) );
            size -= previous;
            return previous;
        }
        else
        {
            long previous = unbox( inner.put( value, newCount ) );
            size += newCount - previous;
            return previous;
        }
    }

    public long increment( T value, long amount )
    {
        long previous = count( value );
        if ( amount == 0 )
        {
            return previous;
        }

        long newCount = previous + amount;
        if ( newCount <= 0 )
        {
            inner.remove( value );
            size -= previous;
            return 0;
        }
        else
        {
            inner.put( value, newCount );
            size += amount;
            return newCount;
        }
    }

    public boolean isEmpty()
    {
        return inner.isEmpty();
    }

    public int size()
    {
        return size;
    }

    public int uniqueSize()
    {
        return inner.size();
    }

    public void clear()
    {
        inner.clear();
        size = 0;
    }

    @Override
    public boolean equals( Object other )
    {
        return this == other ||
                !(other == null || getClass() != other.getClass()) &&
                inner.equals( ((MultiSet) other).inner );

    }

    @Override
    public int hashCode()
    {
        return inner.hashCode();
    }

    private long unbox( Long value )
    {
        return value == null ? 0 : value;
    }

    public Set<Map.Entry<T,Long>> entrySet()
    {
        return inner.entrySet();
    }
}
