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

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ArrayMap<K,V>
{
    private Object data;
    private volatile byte arrayCount;
    private byte toMapThreshold = 5;
    private final boolean useThreadSafeMap;
    private final boolean switchBackToArray;

    public ArrayMap()
    {
        switchBackToArray = false;
        useThreadSafeMap = false;
        data = new ArrayEntry[toMapThreshold];
    }

    public ArrayMap( byte mapThreshold, boolean threadSafe, boolean shrinkToArray )
    {
        this.toMapThreshold = mapThreshold;
        this.useThreadSafeMap = threadSafe;
        this.switchBackToArray = shrinkToArray;
        data = new ArrayEntry[toMapThreshold];
    }

    @Override
    public String toString()
    {
        final byte size;
        final Object snapshot;
        if ( useThreadSafeMap )
        {
            synchronized ( this )
            {
                size = arrayCount;
                snapshot = this.data;
            }
        }
        else
        {
            size = arrayCount;
            snapshot = this.data;
        }
        if ( size != -1 )
        {
            StringBuilder result = new StringBuilder();
            String sep = "[";
            for ( int i = 0; i < size; i++ )
            {
                result.append( sep ).append( ( (ArrayEntry[]) snapshot )[i] );
                sep = ", ";
            }
            return result.append( "]" ).toString();
        }
        return snapshot.toString();
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
            if ( ((ArrayEntry[])data)[i].getKey().equals( key ) )
            {
                ((ArrayEntry[])data)[i].setNewValue( value );
                return;
            }
        }
        if ( arrayCount != -1 )
        {
            if ( arrayCount < ((ArrayEntry[])data).length )
            {
                ((ArrayEntry[])data)[arrayCount++] = new ArrayEntry<K,V>( key, value );
            }
            else
            {
                Map propertyMap = new HashMap<K,V>( ((ArrayEntry[])data).length * 2 );
                for ( int i = 0; i < arrayCount; i++ )
                {
                    propertyMap.put( ((ArrayEntry[])data)[i].getKey(), ((ArrayEntry[])data)[i].getValue() );
                }
                data = propertyMap;
                arrayCount = -1;
                propertyMap.put( key, value );
            }
        }
        else
        {
            ((Map)data).put( key, value );
        }
    }

    private synchronized void synchronizedPut( K key, V value )
    {
        for ( int i = 0; i < arrayCount; i++ )
        {
            if ( ((ArrayEntry[])data)[i].getKey().equals( key ) )
            {
                ((ArrayEntry[])data)[i].setNewValue( value );
                return;
            }
        }
        if ( arrayCount != -1 )
        {
            if ( arrayCount < ((ArrayEntry[])data).length )
            {
                ((ArrayEntry[])data)[arrayCount++] = new ArrayEntry<K,V>( key, value );
            }
            else
            {
                Map propertyMap = new HashMap<K,V>( ((ArrayEntry[])data).length * 2 );
                for ( int i = 0; i < arrayCount; i++ )
                {
                    propertyMap.put( ((ArrayEntry[])data)[i].getKey(), ((ArrayEntry[])data)[i].getValue() );
                }
                data = propertyMap;
                arrayCount = -1;
                propertyMap.put( key, value );
            }
        }
        else
        {
            ((Map)data).put( key, value );
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
            ArrayEntry<K, V> entry = ((ArrayEntry[])data)[i];
            if ( entry != null && key.equals( entry.getKey() ) )
            {
                return entry.getValue();
            }
        }
        if ( arrayCount == -1 )
        {
            return (V) ((Map)data).get( key );
        }
        return null;
    }

    private synchronized V synchronizedGet( K key )
    {
        int count = arrayCount;
        for ( int i = 0; i < count; i++ )
        {
            ArrayEntry<K, V> entry = ((ArrayEntry[])data)[i];
            if ( entry != null && key.equals( entry.getKey() ) )
            {
                return entry.getValue();
            }
        }
        if ( arrayCount == -1 )
        {
            return (V) ((Map)data).get( key );
        }
        return null;
    }

    private synchronized V synchronizedRemove( K key )
    {
        for ( int i = 0; i < arrayCount; i++ )
        {
            if ( ((ArrayEntry[])data)[i].getKey().equals( key ) )
            {
                V removedProperty = (V) ((ArrayEntry[])data)[i].getValue();
                arrayCount--;
                System.arraycopy( data, i + 1, data, i, arrayCount - i );
                ((ArrayEntry[])data)[arrayCount] = null;
                return removedProperty;
            }
        }
        if ( arrayCount == -1 )
        {
            V value = (V) ((Map)data).remove( key );
            if ( switchBackToArray && ((Map)data).size() < toMapThreshold )
            {
                ArrayEntry[] arrayEntries = new ArrayEntry[toMapThreshold];
                int tmpCount = 0;
                for ( Object entryObject : ((Map)data).entrySet() )
                {
                    Entry entry = (Entry) entryObject;
                    arrayEntries[tmpCount++] = new ArrayEntry( entry.getKey(), entry.getValue() );
                }
                data = arrayEntries;
                arrayCount = (byte) tmpCount;
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
            if ( ((ArrayEntry[])data)[i].getKey().equals( key ) )
            {
                V removedProperty = (V) ((ArrayEntry[])data)[i].getValue();
                arrayCount--;
                System.arraycopy( data, i + 1, data, i, arrayCount - i );
                ((ArrayEntry[])data)[arrayCount] = null;
                return removedProperty;
            }
        }
        if ( arrayCount == -1 )
        {
            V value = (V) ((Map)data).remove( key );
            if ( switchBackToArray && ((Map)data).size() < toMapThreshold )
            {
                ArrayEntry[] arrayEntries = new ArrayEntry[toMapThreshold];
                int tmpCount = 0;
                for ( Object entryObject : ((Map)data).entrySet() )
                {
                    Entry entry = (Entry) entryObject;
                    arrayEntries[tmpCount++] = new ArrayEntry( entry.getKey(), entry.getValue() );
                }
                data = arrayEntries;
                arrayCount = (byte) tmpCount;
            }
            return value;
        }
        return null;
    }

    static class ArrayEntry<K,V> implements Entry<K,V>
    {
        private final K key;
        private V value;

        ArrayEntry( K key, V value )
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey()
        {
            return key;
        }

        @Override
        public V getValue()
        {
            return value;
        }

        void setNewValue( V value )
        {
            this.value = value;
        }

        @Override
        public V setValue( V value )
        {
            V oldValue = value;
            this.value = value;
            return oldValue;
        }

        @Override
        public String toString()
        {
            return key + "=" + value;
        }
    }

    public Iterable<K> keySet()
    {
        if ( arrayCount == -1 )
        {
            return ((Map)data).keySet();
        }
        List<K> keys = new LinkedList<K>();
        for ( int i = 0; i < arrayCount; i++ )
        {
            keys.add( (K) ((ArrayEntry[])data)[i].getKey() );
        }
        return keys;
    }

    public Iterable<V> values()
    {
        if ( arrayCount == -1 )
        {
            return ((Map)data).values();
        }
        List<V> values = new LinkedList<V>();
        for ( int i = 0; i < arrayCount; i++ )
        {
            values.add( (V) ((ArrayEntry[])data)[i].getValue() );
        }
        return values;
    }

    public int size()
    {
        if ( useThreadSafeMap )
        {
            return synchronizedSize();
        }
        if ( arrayCount != -1 )
        {
            return arrayCount;
        }
        return ((Map)data).size();
    }

    private synchronized int synchronizedSize()
    {
        if ( arrayCount != -1 )
        {
            return arrayCount;
        }
        return ((Map)data).size();
    }

    public void clear()
    {
        if ( useThreadSafeMap )
        {
            synchronizedClear();
            return;
        }
        if ( arrayCount != -1 )
        {
            Arrays.fill( ((ArrayEntry[])data), null );
            arrayCount = 0;
        }
        else
        {
            ((Map)data).clear();
        }
    }

    private synchronized void synchronizedClear()
    {
        if ( arrayCount != -1 )
        {
            Arrays.fill( ((ArrayEntry[])data), null );
            arrayCount = 0;
        }
        else
        {
            ((Map)data).clear();
        }
    }
}