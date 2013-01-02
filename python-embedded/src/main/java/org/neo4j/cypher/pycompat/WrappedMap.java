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
import java.util.Map;
import java.util.Set;

class WrappedMap implements Map<String, Object>
{

    private Map<String, Object> inner;

    public WrappedMap( Map<String, Object> inner )
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
    public boolean containsKey( Object o )
    {
        return inner.containsKey( o );
    }

    @Override
    public boolean containsValue( Object o )
    {
        return inner.containsValue( o );
    }

    @Override
    public Object get( Object o )
    {
        return ScalaToPythonWrapper.wrap( inner.get( o ) );
    }

    @Override
    public Object put( String s, Object o )
    {
        return inner.put( s, o );
    }

    @Override
    public Object remove( Object o )
    {
        return inner.remove( o );
    }

    @Override
    public void putAll( Map<? extends String, ? extends Object> map )
    {
        inner.putAll( map );
    }

    @Override
    public void clear()
    {
        inner.clear();
    }

    @Override
    public Set<String> keySet()
    {
        return inner.keySet();
    }

    @Override
    public Collection<Object> values()
    {
        return new WrappedCollection(inner.values());
    }

    @Override
    public Set<Entry<String, Object>> entrySet()
    {
        return inner.entrySet();
    }
}
