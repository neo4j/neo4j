/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.messaging.util;

import java.util.Arrays;

import static java.lang.System.arraycopy;

/**
 * This collection class implements a minimal map-like interface
 * for an ordered, primitive-based key-value array. The array both
 * maintains insertion order and ensures key values are unique.
 */
public class PrimitiveLongIntKeyValueArray
{
    static final int DEFAULT_INITIAL_CAPACITY = 100;
    private static final double DEFAULT_GROWTH_FACTOR = 1.2;

    private long[] naturalKeys;
    private int[] naturalValues;
    private long[] sortedKeys;
    private int[] sortedValues;
    private double growthFactor;
    private int size;

    private PrimitiveLongIntKeyValueArray( int initialCapacity, double growthFactor )
    {
        if ( initialCapacity <= 0 )
        {
            throw new IllegalArgumentException( "Illegal initial capacity: " + initialCapacity );
        }
        if ( growthFactor <= 1 )
        {
            throw new IllegalArgumentException( "Illegal growth factor: " + growthFactor );
        }
        naturalKeys = new long[DEFAULT_INITIAL_CAPACITY];
        naturalValues = new int[DEFAULT_INITIAL_CAPACITY];
        sortedKeys = new long[DEFAULT_INITIAL_CAPACITY];
        sortedValues = new int[DEFAULT_INITIAL_CAPACITY];
        this.growthFactor = growthFactor;
    }

    public PrimitiveLongIntKeyValueArray( int initialCapacity )
    {
        this( initialCapacity, DEFAULT_GROWTH_FACTOR );
    }

    PrimitiveLongIntKeyValueArray()
    {
        this( DEFAULT_INITIAL_CAPACITY, DEFAULT_GROWTH_FACTOR );
    }

    /**
     * The current capacity.
     *
     * @return current capacity
     */
    public int capacity()
    {
        return naturalKeys.length;
    }

    /**
     * Ensure the array has at least the capacity requested. The
     * capacity will only ever increase or stay the same.
     *
     * @param newCapacity the new capacity requirement
     */
    void ensureCapacity( int newCapacity )
    {
        int capacity = naturalKeys.length;
        if ( newCapacity > capacity )
        {
            long[] newNaturalKeys = new long[newCapacity];
            int[] newNaturalValues = new int[newCapacity];
            long[] newSortedKeys = new long[newCapacity];
            int[] newSortedValues = new int[newCapacity];
            arraycopy( naturalKeys, 0, newNaturalKeys, 0, capacity );
            arraycopy( naturalValues, 0, newNaturalValues, 0, capacity );
            arraycopy( sortedKeys, 0, newSortedKeys, 0, capacity );
            arraycopy( sortedValues, 0, newSortedValues, 0, capacity );
            naturalKeys = newNaturalKeys;
            naturalValues = newNaturalValues;
            sortedKeys = newSortedKeys;
            sortedValues = newSortedValues;
        }
    }

    /**
     * The number of items in this array.
     *
     * @return number of items in the array
     */
    public int size()
    {
        return size;
    }

    /**
     * Fetch the integer mapped to the given key or defaultValue if
     * that key does not exist.
     *
     * @param key the handle for the required value
     * @param defaultValue value to return if the key is not found
     * @return the integer value mapped to the key provided
     */
    public int getOrDefault( long key, int defaultValue )
    {
        int index = Arrays.binarySearch( sortedKeys, 0, size, key );
        if ( index >= 0 )
        {
            return sortedValues[index];
        }
        else
        {
            return defaultValue;
        }
    }

    /**
     * Set the value for a given key if that key is not already in use.
     *
     * @param key the key against which to put the value
     * @param value the value to include
     * @return true if the value was successfully included, false otherwise
     */
    public boolean putIfAbsent( long key, int value )
    {
        int capacity = naturalKeys.length;
        if ( size == capacity )
        {
            ensureCapacity( (int) Math.floor( growthFactor * capacity ) );
        }

        int index = Arrays.binarySearch( sortedKeys, 0, size, key );
        if ( index >= 0 )
        {
            return false;
        }
        else
        {
            index = -index - 1;
            for ( int i = size; i > index; i-- )
            {
                int j = i - 1;
                sortedKeys[i] = sortedKeys[j];
                sortedValues[i] = sortedValues[j];
            }
            naturalKeys[size] = key;
            naturalValues[size] = value;
            sortedKeys[index] = key;
            sortedValues[index] = value;

            size += 1;
            return true;
        }
    }

    /**
     * Clear the array and set a new capacity if not already large enough.
     *
     * @param newCapacity the new capacity requirement
     */
    public void reset( int newCapacity )
    {
        size = 0;
        ensureCapacity( newCapacity );
    }

    /**
     * Return an array of all key values, in order of insertion.
     *
     * @return array of key values
     */
    public long[] keys()
    {
        return Arrays.copyOfRange( naturalKeys, 0, size );
    }
}
