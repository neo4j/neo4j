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

import org.neo4j.collection.primitive.hopscotch.IntKeyLongValueTable;
import org.neo4j.collection.primitive.hopscotch.IntKeyObjectValueTable;
import org.neo4j.collection.primitive.hopscotch.IntKeyTable;
import org.neo4j.collection.primitive.hopscotch.IntKeyUnsafeTable;
import org.neo4j.collection.primitive.hopscotch.LongKeyIntValueTable;
import org.neo4j.collection.primitive.hopscotch.LongKeyLongValueUnsafeTable;
import org.neo4j.collection.primitive.hopscotch.LongKeyObjectValueTable;
import org.neo4j.collection.primitive.hopscotch.LongKeyTable;
import org.neo4j.collection.primitive.hopscotch.LongKeyUnsafeTable;
import org.neo4j.collection.primitive.hopscotch.PrimitiveIntHashSet;
import org.neo4j.collection.primitive.hopscotch.PrimitiveIntLongHashMap;
import org.neo4j.collection.primitive.hopscotch.PrimitiveIntObjectHashMap;
import org.neo4j.collection.primitive.hopscotch.PrimitiveLongHashSet;
import org.neo4j.collection.primitive.hopscotch.PrimitiveLongIntHashMap;
import org.neo4j.collection.primitive.hopscotch.PrimitiveLongLongHashMap;
import org.neo4j.collection.primitive.hopscotch.PrimitiveLongObjectHashMap;

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

    // Some example would be...
    public static PrimitiveLongSet longSet()
    {
        return longSet( DEFAULT_HEAP_CAPACITY );
    }

    public static PrimitiveLongSet longSet( int initialCapacity )
    {
        return new PrimitiveLongHashSet( new LongKeyTable<>( initialCapacity, VALUE_MARKER ),
                VALUE_MARKER, NO_MONITOR );
    }

    public static PrimitiveLongSet offHeapLongSet()
    {
        return offHeapLongSet( DEFAULT_OFFHEAP_CAPACITY );
    }

    public static PrimitiveLongSet offHeapLongSet( int initialCapacity )
    {
        return new PrimitiveLongHashSet( new LongKeyUnsafeTable<>( initialCapacity, VALUE_MARKER ),
                VALUE_MARKER, NO_MONITOR );
    }

    public static PrimitiveLongIntMap longIntMap()
    {
        return longIntMap( DEFAULT_HEAP_CAPACITY );
    }

    public static PrimitiveLongIntMap longIntMap( int initialCapacity )
    {
        return new PrimitiveLongIntHashMap( new LongKeyIntValueTable( initialCapacity ), NO_MONITOR );
    }

    public static PrimitiveLongLongMap offHeapLongLongMap()
    {
        return offHeapLongLongMap( DEFAULT_OFFHEAP_CAPACITY );
    }

    public static PrimitiveLongLongMap offHeapLongLongMap( int initialCapacity )
    {
        return new PrimitiveLongLongHashMap( new LongKeyLongValueUnsafeTable( initialCapacity ), NO_MONITOR );
    }

    public static <VALUE> PrimitiveLongObjectMap<VALUE> longObjectMap()
    {
        return longObjectMap( DEFAULT_HEAP_CAPACITY );
    }

    public static <VALUE> PrimitiveLongObjectMap<VALUE> longObjectMap( int initialCapacity )
    {
        return new PrimitiveLongObjectHashMap<>( new LongKeyObjectValueTable<VALUE>( initialCapacity ), NO_MONITOR );
    }

    public static PrimitiveIntSet intSet()
    {
        return intSet( DEFAULT_HEAP_CAPACITY );
    }

    public static PrimitiveIntSet intSet( int initialCapacity )
    {
        return new PrimitiveIntHashSet( new IntKeyTable<>( initialCapacity, VALUE_MARKER ),
                VALUE_MARKER, NO_MONITOR );
    }

    public static PrimitiveIntSet offHeapIntSet()
    {
        return new PrimitiveIntHashSet( new IntKeyUnsafeTable<>( 1 << 20, VALUE_MARKER ),
                VALUE_MARKER, NO_MONITOR );
    }

    public static PrimitiveIntSet offHeapIntSet( int initialCapacity )
    {
        return new PrimitiveIntHashSet( new IntKeyUnsafeTable<>( initialCapacity, VALUE_MARKER ),
                VALUE_MARKER, NO_MONITOR );
    }

    public static <VALUE> PrimitiveIntObjectMap<VALUE> intObjectMap()
    {
        return intObjectMap( DEFAULT_HEAP_CAPACITY );
    }

    public static <VALUE> PrimitiveIntObjectMap<VALUE> intObjectMap( int initialCapacity )
    {
        return new PrimitiveIntObjectHashMap<>( new IntKeyObjectValueTable<VALUE>( initialCapacity ), NO_MONITOR );
    }

    public static PrimitiveIntLongMap intLongMap()
    {
        return intLongMap( DEFAULT_HEAP_CAPACITY );
    }

    public static PrimitiveIntLongMap intLongMap( int initialCapacity )
    {
        return new PrimitiveIntLongHashMap( new IntKeyLongValueTable( initialCapacity ), NO_MONITOR );
    }

    public static PrimitiveLongIterator iterator( final long... longs )
    {
        return new PrimitiveLongIterator()
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
}
