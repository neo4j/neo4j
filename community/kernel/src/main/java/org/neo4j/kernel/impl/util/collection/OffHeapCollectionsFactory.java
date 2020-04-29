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

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.impl.api.state.AppendOnlyValuesContainer;
import org.neo4j.kernel.impl.api.state.ValuesContainer;
import org.neo4j.kernel.impl.api.state.ValuesMap;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSets;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.util.diffsets.TrackableDiffSets.newMutableLongDiffSets;

public class OffHeapCollectionsFactory implements CollectionsFactory
{
    private final MemoryAllocator allocator;

    private final Collection<Resource> resources = new ArrayList<>();
    private ValuesContainer valuesContainer;

    public OffHeapCollectionsFactory( OffHeapBlockAllocator blockAllocator )
    {
        this.allocator = new OffHeapMemoryAllocator( blockAllocator );
    }

    @Override
    public MutableLongSet newLongSet( MemoryTracker memoryTracker )
    {
        final MutableLinearProbeLongHashSet set = new MutableLinearProbeLongHashSet( allocator, memoryTracker );
        resources.add( set );
        return set;
    }

    @Override
    public MutableLongDiffSets newLongDiffSets( MemoryTracker memoryTracker )
    {
        return newMutableLongDiffSets( this, memoryTracker );
    }

    @Override
    public MutableLongObjectMap<Value> newValuesMap( MemoryTracker memoryTracker )
    {
        if ( valuesContainer == null )
        {
            valuesContainer = new AppendOnlyValuesContainer( allocator, memoryTracker );
        }
        final LinearProbeLongLongHashMap refs = new LinearProbeLongLongHashMap( allocator, memoryTracker );
        resources.add( refs );
        return new ValuesMap( refs, valuesContainer );
    }

    @Override
    public void release()
    {
        resources.forEach( Resource::close );
        resources.clear();
        if ( valuesContainer != null )
        {
            valuesContainer.close();
            valuesContainer = null;
        }
    }
}
