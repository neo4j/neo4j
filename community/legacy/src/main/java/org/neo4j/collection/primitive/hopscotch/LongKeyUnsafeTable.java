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
package org.neo4j.collection.primitive.hopscotch;

import org.neo4j.memory.MemoryAllocationTracker;

public class LongKeyUnsafeTable<VALUE> extends UnsafeTable<VALUE>
{
    public LongKeyUnsafeTable( int capacity, VALUE valueMarker, MemoryAllocationTracker allocationTracker )
    {
        super( capacity, 8, valueMarker, allocationTracker );
    }

    @Override
    protected long internalKey( long keyAddress )
    {
        return alignmentSafeGetLongAsTwoInts( keyAddress );
    }

    @Override
    protected void internalPut( long keyAddress, long key, VALUE value )
    {
        alignmentSafePutLongAsTwoInts( keyAddress, key );
    }

    @Override
    protected Table<VALUE> newInstance( int newCapacity )
    {
        return new LongKeyUnsafeTable<>( newCapacity, valueMarker, allocationTracker );
    }
}
