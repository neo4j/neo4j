/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.impl.util.diffsets.PrimitiveLongDiffSets;
import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.memory.MemoryTracker;

import static java.util.Objects.requireNonNull;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptySet;

class OffHeapCollectionsFactory implements CollectionsFactory
{
    private final MemoryAllocationTracker memoryTracker;

    OffHeapCollectionsFactory( MemoryAllocationTracker memoryTracker )
    {
        this.memoryTracker = requireNonNull( memoryTracker );
    }

    @Override
    public PrimitiveLongSet newLongSet()
    {
        return Primitive.offHeapLongSet( memoryTracker );
    }

    @Override
    public <V> PrimitiveLongObjectMap<V> newLongObjectMap()
    {
        return Primitive.longObjectMap();
    }

    @Override
    public <V> PrimitiveIntObjectMap<V> newIntObjectMap()
    {
        return Primitive.intObjectMap();
    }

    @Override
    public PrimitiveLongDiffSets newLongDiffSets()
    {
        return new PrimitiveLongDiffSets( emptySet(), emptySet(), this );
    }

    @Override
    public MemoryTracker getMemoryTracker()
    {
        return memoryTracker;
    }

    @Override
    public boolean collectionsMustBeReleased()
    {
        return true;
    }
}
