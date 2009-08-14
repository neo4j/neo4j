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
package org.neo4j.impl.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class ArrayMap<K,V>
{
    private ArrayEntry<K,V>[] arrayEntries;

    private volatile int arrayCount = 0;
    private int toMapThreshold = 5;
    private Map<K,V> propertyMap = null;
    private final boolean useThreadSafeMap;
    private boolean switchBackToArray = false;

    public ArrayMap()
    {
        useThreadSafeMap = false;
        arrayEntries = new ArrayEntry[toMapThreshold];
    }

    public ArrayMap( int mapThreshold, boolean threadSafe, boolean shrinkToArray )
    {
        this.toMapThreshold = mapThreshold;
        this.useThreadSafeMap = threadSafe;
        this.switchBackToArray = shrinkToArray;
        arrayEntries = new ArrayEntry[toMapThreshold];
    }

    public void put( K key, V value )
    {
        if ( useThreadSafeMap )
        {
            synchronizedPut( key, value );
            return;
        }
        for ( int i = 0; i < arrayCount; i++ )
        {
            if ( arrayEntries[i].getKey().equals( key ) )
            {
                arrayEntries[i].setNewValue( value );
                return;
            }
        }
        if ( arrayCount != -1 )
        {
            if ( arrayCount < arrayEntries.length )
            {
                arrayEntries[arrayCount++] = new ArrayEntry<K,V>( key, value );
            }
            else
            {
                propertyMap = new HashMap<K,V>();
                for ( int i = 0; i < arrayCount; i++ )
                {
                    propertyMap.put( arrayEntries[i].getKey(), arrayEntries[i]
                        .getValue() );
                }
                arrayCount = -1;
                propertyMap.put( key, value );
            }
        }
        else
        {
            propertyMap.put( key, value );
        }
    }

    private synchronized void synchronizedPut( K key, V value )
    {
        for ( int i = 0; i < arrayCount; i++ )
        {
            if ( arrayEntries[i].getKey().equals( key ) )
            {
                arrayEntries[i].setNewValue( value );
                return;
            }
        }
        if ( arrayCount != -1 )
        {
            if ( arrayCount < arrayEntries.length )
            {
                arrayEntries[arrayCount++] = new ArrayEntry<K,V>( key, value );
            }
            else
            {
                propertyMap = new ConcurrentHashMap<K,V>();
                for ( int i = 0; i < arrayCount; i++ )
                {
                    propertyMap.put( arrayEntries[i].getKey(), arrayEntries[i]
                        .getValue() );
                }
                arrayEntries = null;
                arrayCount = -1;
                propertyMap.put( key, value );
            }
        }
        else
        {
            propertyMap.put( key, value );
        }
    }

    public V get( K key )
    {
        if ( key == null )
        {
            return null;
        }
        if ( useThreadSafeMap )
        {
            return synchronizedGet( key );
        }
        int count = arrayCount;
        for ( int i = 0; i < count; i++ )
        {
            if ( key.equals( arrayEntries[i].getKey() ) )
            {
                return arrayEntries[i].getValue();
            }
        }
        if ( arrayCount == -1 )
        {
            return propertyMap.get( key );
        }
        return null;
    }

    private synchronized V synchronizedGet( K key )
    {
        for ( int i = 0; i < arrayCount; i++ )
        {
            if ( key.equals( arrayEntries[i].getKey() ) )
            {
                return arrayEntries[i].getValue();
            }
        }
        if ( arrayCount == -1 )
        {
            return propertyMap.get( key );
        }
        return null;
    }

    private synchronized V synchronizedRemove( K key )
    {
        for ( int i = 0; i < arrayCount; i++ )
        {
            if ( arrayEntries[i].getKey().equals( key ) )
            {
                V removedProperty = arrayEntries[i].getValue();
                arrayCount--;
                System.arraycopy( arrayEntries, i + 1, arrayEntries, i,
                    arrayCount - i );
                return removedProperty;
            }
        }
        if ( arrayCount == -1 )
        {
            V value = propertyMap.remove( key );
            if ( switchBackToArray && propertyMap.size() < toMapThreshold )
            {
                arrayEntries = new ArrayEntry[toMapThreshold];
                int tmpCount = 0;
                for ( Entry<K,V> entry : propertyMap.entrySet() )
                {
                    arrayEntries[tmpCount++] = new ArrayEntry<K,V>( entry
                        .getKey(), entry.getValue() );
                }
                arrayCount = tmpCount;
            }
            return value;
        }
        return null;
    }

    public V remove( K key )
    {
        if ( useThreadSafeMap )
        {
            return synchronizedRemove( key );
        }
        for ( int i = 0; i < arrayCount; i++ )
        {
            if ( arrayEntries[i].getKey().equals( key ) )
            {
                V removedProperty = arrayEntries[i].getValue();
                arrayCount--;
                System.arraycopy( arrayEntries, i + 1, arrayEntries, i,
                    arrayCount - i );
                return removedProperty;
            }
        }
        if ( arrayCount == -1 )
        {
            V value = propertyMap.remove( key );
            if ( switchBackToArray && propertyMap.size() < toMapThreshold )
            {
                arrayEntries = new ArrayEntry[toMapThreshold];
                int tmpCount = 0;
                for ( Entry<K,V> entry : propertyMap.entrySet() )
                {
                    arrayEntries[tmpCount++] = new ArrayEntry<K,V>( entry
                        .getKey(), entry.getValue() );
                }
                arrayCount = tmpCount;
            }
            return value;
        }
        return null;
    }

    static class ArrayEntry<K,V> implements Entry<K,V>
    {
        private K key;
        private V value;

        ArrayEntry( K key, V value )
        {
            this.key = key;
            this.value = value;
        }

        public K getKey()
        {
            return key;
        }

        public V getValue()
        {
            return value;
        }

        void setNewValue( V value )
        {
            this.value = value;
        }

        public V setValue( V value )
        {
            V oldValue = value;
            this.value = value;
            return oldValue;
        }
    }

    public Iterable<K> keySet()
    {
        if ( arrayCount == -1 )
        {
            return propertyMap.keySet();
        }
        List<K> keys = new LinkedList<K>();
        for ( int i = 0; i < arrayCount; i++ )
        {
            keys.add( arrayEntries[i].getKey() );
        }
        return keys;
    }

    public Iterable<V> values()
    {
        if ( arrayCount == -1 )
        {
            return propertyMap.values();
        }
        List<V> values = new LinkedList<V>();
        for ( int i = 0; i < arrayCount; i++ )
        {
            values.add( arrayEntries[i].getValue() );
        }
        return values;
    }
    
    public Set<Entry<K,V>> entrySet()
    {
        if ( arrayCount == -1 )
        {
            return propertyMap.entrySet();
        }
        Set<Entry<K,V>> entries = new HashSet<Entry<K,V>>();
        for ( int i = 0; i < arrayCount; i++ )
        {
            entries.add( arrayEntries[i] );
        }
        return entries;
    }

    public int size()
    {
        if ( arrayCount != -1 )
        {
            return arrayCount;
        }
        return propertyMap.size();
    }

    public void clear()
    {
        if ( arrayCount != -1 )
        {
            arrayCount = 0;
        }
        else
        {
            propertyMap.clear();
        }
    }
}