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

import static java.lang.System.arraycopy;

public class DirectArrayMap<V>
{
    private volatile V[] array;
    
    public DirectArrayMap( int maxSize )
    {
        array = (V[])new Object[maxSize];
    }
    
    public void put( int key, V value )
    {
        V[] newArray = copyArray();
        newArray[key] = value;
        array = newArray;
    }
    
    private synchronized V[] copyArray()
    {
        V[] newArray = (V[])new Object[array.length];
        arraycopy( array, 0, newArray, 0, array.length );
        return newArray;
    }

    public void remove( int key )
    {
        V[] newArray = copyArray();
        newArray[key] = null;
        array = newArray;
    }
    
    public V get( int key )
    {
        return array[key];
    }
}
