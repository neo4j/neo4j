/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.helpers.collection.IteratorUtil;

/**
 * Set implementation that provides stable snapshot iteration. It does so by marking the current underlying map as read
 * only whenever an iterator is requested. The first write made after the map has been marked as read only will create
 * a copy of the map. The new map will not be marked as read only until a new iterator is requested.
 */
class CopyOnWriteAfterIteratorHashSet<E> implements Set<E>
{
    private transient HashMap<E, Object> map;
    private boolean readOnly = false;

    /** Marker to signal that the value is present in the backing map. */
    private static final Object PRESENT = new Object();

    @Override
    public Iterator<E> iterator()
    {
        if ( map == null || map.isEmpty() )
        {
            return IteratorUtil.emptyIterator();
        }
        readOnly = true;
        return map.keySet().iterator();
    }

    @Override
    public String toString()
    {
        return map == null ? "[]" : map.keySet().toString();
    }

    @Override
    public Object[] toArray()
    {
        return map == null ? new Object[0] : map.keySet().toArray();
    }

    @Override
    @SuppressWarnings("SuspiciousToArrayCall")
    public <T> T[] toArray( T[] a )
    {
        return map == null ? a : map.keySet().toArray( a );
    }

    @Override
    public int size()
    {
        return map == null ? 0 : map.size();
    }

    @Override
    public boolean isEmpty()
    {
        return map == null || map.isEmpty();
    }

    @Override
    @SuppressWarnings("SuspiciousMethodCalls")
    public boolean contains( Object o )
    {
        return map != null && map.containsKey( o );
    }

    @Override
    public boolean add( E e )
    {
        if ( map == null )
        {
            map = new HashMap<>();
        }
        if ( readOnly )
        {
            if ( map.containsKey( e ) )
            {
                return false;
            }
            map = new HashMap<>( map );
            readOnly = false;
        }
        return map.put( e, PRESENT ) == null;
    }

    @Override
    @SuppressWarnings("SuspiciousMethodCalls")
    public boolean remove( Object o )
    {
        if ( map == null )
        {
            return false;
        }
        if ( readOnly )
        {
            if ( !map.containsKey( o ) )
            {
                return false;
            }
            map = new HashMap<>( map );
            readOnly = false;
        }
        return map.remove( o ) == PRESENT;
    }

    @Override
    public boolean containsAll( Collection<?> c )
    {
        return map == null ? c.isEmpty() : map.keySet().containsAll( c );
    }

    @Override
    public boolean addAll( Collection<? extends E> c )
    {
        boolean modified = false;
        for ( E e : c )
        {
            if ( add( e ) )
            {
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public void clear()
    {
        readOnly = false;
        map = null;
    }

    @Override
    public boolean removeAll( Collection<?> c )
    {
        return conditionalRemove( c, true );
    }

    @Override
    public boolean retainAll( Collection<?> c )
    {
        return conditionalRemove( c, false );
    }

    private boolean conditionalRemove( Collection<?> c, boolean remove )
    {
        if ( map == null || map.isEmpty() )
        {
            return false;
        }
        boolean modified = false;
        Iterator<E> it = map.keySet().iterator();
        HashMap<E, Object> target = readOnly ? null : map;
        while ( it.hasNext() )
        {
            E item = it.next();
            if ( c.contains( item ) == remove )
            {
                if ( readOnly )
                {
                    if ( target == null )
                    {
                        target = new HashMap<>( map );
                    }
                    target.remove( item );
                }
                else
                {
                    it.remove();
                }
                modified = true;
            }
        }
        if ( target != null && target != map )
        {
            map = target;
            readOnly = false;
        }
        return modified;
    }
}
