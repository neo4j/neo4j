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
package org.neo4j.kernel.impl.util.collection;

import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;

import org.neo4j.kernel.impl.util.diffsets.MutableDiffSets;
import org.neo4j.kernel.impl.util.diffsets.MutableDiffSetsImpl;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSets;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSetsImpl;
import org.neo4j.kernel.impl.util.diffsets.RemovalsCountingDiffSets;
import org.neo4j.memory.MemoryTracker;

public final class HeapTrackingCollections
{
    private HeapTrackingCollections()
    {
    }

    public static <V> MutableIntObjectMap<V> newIntObjectHashMap( MemoryTracker memoryTracker )
    {
        return HeapTrackingIntObjectHashMap.createIntObjectHashMap( memoryTracker );
    }

    public static MutableLongSet newLongSet( MemoryTracker memoryTracker )
    {
        return HeapTrackingLongHashSet.createLongHashSet( memoryTracker );
    }

    public static <V> MutableLongObjectMap<V> newLongObjectMap( MemoryTracker memoryTracker )
    {
        return HeapTrackingLongObjectHashMap.createLongObjectHashMap( memoryTracker );
    }

    public static <K,V> MutableMap<K,V> newMap( MemoryTracker memoryTracker )
    {
        return HeapTrackingUnifiedMap.createUnifiedMap( memoryTracker );
    }

    public static <T> MutableSet<T> newSet( MemoryTracker memoryTracker )
    {
        return HeapTrackingUnifiedSet.createUnifiedSet( memoryTracker );
    }

    public static MutableLongDiffSets newMutableLongDiffSets( CollectionsFactory collectionsFactory, MemoryTracker memoryTracker )
    {
        return MutableLongDiffSetsImpl.createMutableLongDiffSetsImpl( collectionsFactory, memoryTracker );
    }

    public static RemovalsCountingDiffSets newRemovalsCountingDiffSets( CollectionsFactory collectionsFactory, MemoryTracker memoryTracker )
    {
        return RemovalsCountingDiffSets.newRemovalsCountingDiffSets( collectionsFactory, memoryTracker );
    }

    public static <T> MutableDiffSets<T> newMutableDiffSets( MemoryTracker memoryTracker )
    {
        return MutableDiffSetsImpl.newMutableDiffSets( memoryTracker );
    }
}
