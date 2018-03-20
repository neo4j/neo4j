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
package org.neo4j.collection.primitive;

import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import org.neo4j.collection.primitive.hopscotch.LongKeyObjectValueTable;
import org.neo4j.collection.primitive.hopscotch.PrimitiveLongObjectHashMap;
import org.neo4j.memory.MemoryAllocationTracker;

import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.NO_MONITOR;

/**
 * Convenient factory for common primitive sets and maps.
 *
 * @see PrimitiveIntCollections
 * @see PrimitiveLongCollections
 */
public class Primitive
{
    /**
     * Used as value marker for sets, where values aren't applicable. The hop scotch algorithm still
     * deals with values so having this will have no-value collections, like sets communicate
     * the correct semantics to the algorithm.
     */
    public static final Object VALUE_MARKER = new Object();
    public static final int DEFAULT_HEAP_CAPACITY = 1 << 4;
    public static final int DEFAULT_OFFHEAP_CAPACITY = 1 << 20;

    private Primitive()
    {
    }

    public static MutableLongSet offHeapLongSet( MemoryAllocationTracker allocationTracker )
    {
//        return offHeapLongSet( DEFAULT_OFFHEAP_CAPACITY, allocationTracker );

        return new LongHashSet();
    }

    public static <VALUE> PrimitiveLongObjectMap<VALUE> longObjectMap()
    {
        return longObjectMap( DEFAULT_HEAP_CAPACITY );
    }

    public static <VALUE> PrimitiveLongObjectMap<VALUE> longObjectMap( int initialCapacity )
    {
        return new PrimitiveLongObjectHashMap<>( new LongKeyObjectValueTable<VALUE>( initialCapacity ), NO_MONITOR );
    }

    public static MutableIntSet offHeapIntSet( MemoryAllocationTracker allocationTracker )
    {
        // todo ak
        return new IntHashSet();
    }

    public static LongIterator iterator( final long... longs )
    {
        return new LongIterator()
        {
            int i;

            @Override
            public boolean hasNext()
            {
                return i < longs.length;
            }

            @Override
            public long next()
            {
                return longs[i++];
            }
        };
    }

    public static IntIterator iterator( final int... ints )
    {
        return new IntIterator()
        {
            int i;

            @Override
            public boolean hasNext()
            {
                return i < ints.length;
            }

            @Override
            public int next()
            {
                return ints[i++];
            }
        };
    }
}
