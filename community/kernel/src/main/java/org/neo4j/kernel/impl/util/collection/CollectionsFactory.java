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

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;

import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSetsImpl;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.Value;

/**
 * The purpose of this factory is the ability to switch between multiple collection implementations used in {@link TxState} (e.g. on- or off-heap),
 * keeping track of underlying memory allocations.
 */
public interface CollectionsFactory
{
    MutableLongSet newLongSet();

    MutableLongDiffSetsImpl newLongDiffSets();

    MutableLongObjectMap<Value> newValuesMap();

    MemoryTracker getMemoryTracker();

    /**
     * Release previously created collections. This method does not invalidate the factory.
     */
    void release();
}
