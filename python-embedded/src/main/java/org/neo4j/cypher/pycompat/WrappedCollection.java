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
package org.neo4j.cypher.pycompat;

import java.util.Collection;
import java.util.Iterator;

public class WrappedCollection implements Collection<Object>
{
    private Collection<Object> inner;

    public WrappedCollection( Collection<Object> inner )
    {
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
    public Iterator<Object> iterator()
    {
        return new WrappedIterator(inner.iterator());
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
    public boolean add( Object o )
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
    public boolean addAll( Collection<? extends Object> objects )
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
