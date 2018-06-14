/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.eclipse.collections.api.set.primitive.MutableLongSet;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.collection.offheap.MemoryAllocator;
import org.neo4j.collection.offheap.MutableLinearProbeLongHashSet;
import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSetsImpl;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.memory.MemoryTracker;

public class OffHeapCollectionsFactory implements CollectionsFactory
{
    private final MemoryAllocationTracker memoryTracker = new LocalMemoryTracker();
    private final MemoryAllocator allocator;

    private final Collection<Resource> resources = new ArrayList<>();

    public OffHeapCollectionsFactory( OffHeapBlockAllocator blockAllocator )
    {
        this.allocator = new OffHeapMemoryAllocator( memoryTracker, blockAllocator );
    }

    @Override
    public MutableLongSet newLongSet()
    {
        final MutableLinearProbeLongHashSet set = new MutableLinearProbeLongHashSet( allocator );
        resources.add( set );
        return set;
    }

    @Override
    public MutableLongDiffSetsImpl newLongDiffSets()
    {
        return new MutableLongDiffSetsImpl( this );
    }

    @Override
    public MemoryTracker getMemoryTracker()
    {
        return memoryTracker;
    }

    @Override
    public void release()
    {
        resources.forEach( Resource::close );
        resources.clear();
    }
}
