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

import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.memory.Measurable;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ScopedMemoryTracker;

import static java.util.Collections.emptyIterator;
import static org.neo4j.kernel.impl.util.collection.LongProbeTable.SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

/**
 * A specialized table used during hash joins.
 * @param <K> key type
 * @param <V> value type
 */
public class ProbeTable<K extends Measurable,V extends Measurable> implements AutoCloseable
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( ProbeTable.class );
    private final ScopedMemoryTracker scopedMemoryTracker;
    private final UnifiedMap<K,HeapTrackingAppendList<V>> map;

    public static <K extends Measurable,V extends Measurable> ProbeTable<K,V> createProbeTable( MemoryTracker memoryTracker )
    {
        ScopedMemoryTracker scopedMemoryTracker = new ScopedMemoryTracker( memoryTracker );
        scopedMemoryTracker.allocateHeap( SHALLOW_SIZE + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE );
        return new ProbeTable<>( scopedMemoryTracker );
    }

    private ProbeTable( ScopedMemoryTracker scopedMemoryTracker )
    {
        this.scopedMemoryTracker = scopedMemoryTracker;
        this.map = HeapTrackingUnifiedMap.createUnifiedMap( scopedMemoryTracker );
    }

    public void put( K key, V value )
    {
        map.getIfAbsentPutWith( key, p ->
        {
            p.allocateHeap( key.estimatedHeapUsage() );
            return HeapTrackingAppendList.newAppendList( p );
        }, scopedMemoryTracker ).add( value );
        scopedMemoryTracker.allocateHeap( value.estimatedHeapUsage() );
    }

    public Iterator<V> get( K key )
    {
        var entry = map.get( key );
        if ( entry == null )
        {
            return emptyIterator();
        }
        return entry.iterator();
    }

    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    public Set<K> keySet()
    {
        return map.keySet();
    }

    @Override
    public void close()
    {
        scopedMemoryTracker.close();
    }
}
