/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.collection.primitive.hopscotch.LongKeyLongValueUnsafeTable;
import org.neo4j.collection.primitive.hopscotch.PrimitiveLongLongHashMap;
import org.neo4j.collection.primitive.koloboke.PrimitiveIntObjectMapImpl;
import org.neo4j.collection.primitive.koloboke.PrimitiveIntSetImpl;
import org.neo4j.collection.primitive.koloboke.PrimitiveLongIntMapImpl;
import org.neo4j.collection.primitive.koloboke.PrimitiveLongObjectMapImpl;
import org.neo4j.collection.primitive.koloboke.PrimitiveLongSetImpl;

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
//        return new PrimitiveLongHashSet( new LongKeyTable<>( initialCapacity, VALUE_MARKER ),
//                VALUE_MARKER, NO_MONITOR );
        return PrimitiveLongSetImpl.withExpectedSize( initialCapacity );
    }

    public static PrimitiveLongIntMap longIntMap()
    {
        return longIntMap( DEFAULT_HEAP_CAPACITY );
    }

    public static PrimitiveLongIntMap longIntMap( int initialCapacity )
    {
        return PrimitiveLongIntMapImpl.withExceptedSize( initialCapacity );
//        return new PrimitiveLongIntHashMap( new LongKeyIntValueTable( initialCapacity ), NO_MONITOR );
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
        return PrimitiveLongObjectMapImpl.withExceptedSize( initialCapacity );
//        return new PrimitiveLongObjectHashMap<>( new LongKeyObjectValueTable<VALUE>( initialCapacity ), NO_MONITOR );
    }

    public static PrimitiveIntSet intSet()
    {
        return intSet( DEFAULT_HEAP_CAPACITY );
    }

    public static PrimitiveIntSet intSet( int initialCapacity )
    {
        return PrimitiveIntSetImpl.withExpectedSize( initialCapacity );
//        return new PrimitiveIntHashSet( new IntKeyTable<>( initialCapacity, VALUE_MARKER ),
//                VALUE_MARKER, NO_MONITOR );
    }

    public static <VALUE> PrimitiveIntObjectMap<VALUE> intObjectMap()
    {
        return intObjectMap( DEFAULT_HEAP_CAPACITY );
    }

    public static <VALUE> PrimitiveIntObjectMap<VALUE> intObjectMap( int initialCapacity )
    {
        return PrimitiveIntObjectMapImpl.withExceptedSize( initialCapacity );
//        return new PrimitiveIntObjectHashMap<>( new IntKeyObjectValueTable<VALUE>( initialCapacity ), NO_MONITOR );
    }
}
