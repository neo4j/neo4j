/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

public class MultiSet<T>
{
    private final Map<T, Integer> inner;
    private int size = 0;

    public MultiSet()
    {
        inner = new HashMap<>();
    }

    public MultiSet( int batchSize )
    {
        inner = new HashMap<>( batchSize );
    }

    public int add( T value )
    {
        size++;
        Integer count = inner.get( value );
        int newCount = count == null ? 1 : count + 1;
        inner.put( value, newCount );
        return newCount;
    }

    public int remove( T value )
    {
        Integer count = inner.get( value );
        if ( count == null )
        {
            return 0;
        }

        size--;
        int newCount = count - 1;
        if ( newCount <= 0 )
        {
            inner.remove( value );
        }
        else
        {
            inner.put( value, newCount );
        }
        return newCount;
    }

    public boolean contains( T value )
    {
        Integer count = inner.get( value );
        return count != null && count > 0;
    }

    public int count( T value )
    {
        Integer count = inner.get( value );
        return count == null ? 0 : count;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public int size()
    {
        return size;
    }

    public int uniqueValueSize()
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
}
