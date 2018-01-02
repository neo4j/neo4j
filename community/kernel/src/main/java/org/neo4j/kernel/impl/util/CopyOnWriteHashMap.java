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
package org.neo4j.kernel.impl.util;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Optimized for rare writes and very frequent reads in a thread safe way.
 * Reads impose no synchronization or locking.
 * 
 * {@link #keySet()}, {@link #values()} and {@link #entrySet()} wraps the
 * returned iterators since they provide the {@link Iterator#remove() remove}
 * method which isn't supported by this implementation. These iterators are also
 * views of the map at that point in time so they don't change during their
 * life time.
 * 
 * @author Mattias Persson
 *
 * @param <K> key type
 * @param <V> value type
 */
public class CopyOnWriteHashMap<K, V> implements Map<K, V>
{
    private volatile Map<K, V> actual = new HashMap<K, V>();
    
    @Override
    public int size()
    {
        return actual.size();
    }

    @Override
    public boolean isEmpty()
    {
        return actual.isEmpty();
    }

    @Override
    public boolean containsKey( Object key )
    {
        return actual.containsKey( key );
    }

    @Override
    public boolean containsValue( Object value )
    {
        return actual.containsValue( value );
    }

    @Override
    public V get( Object key )
    {
        return actual.get( key );
    }

    private Map<K, V> copy()
    {
        return new HashMap<K, V>( actual );
    }
    
    @Override
    public synchronized V put( K key, V value )
    {
        Map<K, V> copy = copy();
        V previous = copy.put( key, value );
        actual = copy;
        return previous;
    }

    @Override
    public synchronized V remove( Object key )
    {
        Map<K, V> copy = copy();
        V previous = copy.remove( key );
        actual = copy;
        return previous;
    }

    @Override
    public synchronized void putAll( Map<? extends K, ? extends V> m )
    {
        Map<K, V> copy = copy();
        copy.putAll( m );
        actual = copy;
    }

    @Override
    public synchronized void clear()
    {
        actual = new HashMap<K, V>();
    }
    
    private static class UnsupportedRemoveIterator<T> implements Iterator<T>
    {
        private final Iterator<T> actual;
        
        UnsupportedRemoveIterator( Iterator<T> actual )
        {
            this.actual = actual;
        }
        
        @Override
        public boolean hasNext()
        {
            return actual.hasNext();
        }

        @Override
        public T next()
        {
            return actual.next();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Set<K> keySet()
    {
        return new AbstractSet<K>()
        {
            @Override
            public boolean remove( Object o )
            {
                return CopyOnWriteHashMap.this.remove( o ) != null;
            }
            
            @Override
            public Iterator<K> iterator()
            {
                return new UnsupportedRemoveIterator<K>( actual.keySet().iterator() );
            }

            @Override
            public int size()
            {
                return actual.size();
            }
        };
    }

    @Override
    public Collection<V> values()
    {
        return new AbstractCollection<V>()
        {
            @Override
            public Iterator<V> iterator()
            {
                return new UnsupportedRemoveIterator<V>( actual.values().iterator() );
            }

            @Override
            public int size()
            {
                return actual.size();
            }
        };
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        return new AbstractSet<Entry<K,V>>()
        {
            @Override
            public boolean remove( Object o )
            {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public Iterator<Entry<K, V>> iterator()
            {
                return new UnsupportedRemoveIterator<Entry<K,V>>( actual.entrySet().iterator() )
                {
                    @Override
                    public Entry<K, V> next()
                    {
                        final Entry<K, V> actualNext = super.next();
                        return new Entry<K,V>()
                        {
                            @Override
                            public K getKey()
                            {
                                return actualNext.getKey();
                            }

                            @Override
                            public V getValue()
                            {
                                return actualNext.getValue();
                            }

                            @Override
                            public V setValue( V value )
                            {
                                throw new UnsupportedOperationException();
                            }
                            
                            @Override
                            public boolean equals( Object obj )
                            {
                                return actualNext.equals( obj );
                            }
                            
                            @Override
                            public int hashCode()
                            {
                                return actualNext.hashCode();
                            }
                        };
                    }
                };
            }

            @Override
            public int size()
            {
                return actual.size();
            }
        };
    }
}
