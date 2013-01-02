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
package org.neo4j.cypher.pycompat;

import java.util.Collection;

public class WrappedCollection<T> extends WrappedIterable<T> implements Collection<T>
{
    private final Collection<T> inner;

    public WrappedCollection( Collection<T> inner )
    {
        super(inner);
        this.inner = inner;
    }

    @Override
    public int size()
    {
        return inner.size();
    }

    @Override
    public boolean isEmpty()
    {
        return inner.isEmpty();
    }

    @Override
    public boolean contains( Object o )
    {
        return inner.contains( o );
    }

    @Override
    public Object[] toArray()
    {
        return inner.toArray();
    }

    @Override
    public <T> T[] toArray( T[] ts )
    {
        return inner.toArray( ts );
    }

    @Override
    public boolean add( T o )
    {
        return inner.add( o );
    }

    @Override
    public boolean remove( Object o )
    {
        return inner.remove( o );
    }

    @Override
    public boolean containsAll( Collection<?> objects )
    {
        return inner.containsAll( objects );
    }

    @Override
    public boolean addAll( Collection<? extends T> objects )
    {
        return inner.addAll( objects );
    }

    @Override
    public boolean removeAll( Collection<?> objects )
    {
        return inner.removeAll( objects );
    }

    @Override
    public boolean retainAll( Collection<?> objects )
    {
        return inner.retainAll( objects );
    }

    @Override
    public void clear()
    {
        inner.clear();
    }
}
