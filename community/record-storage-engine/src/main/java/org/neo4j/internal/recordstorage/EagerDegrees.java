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
package org.neo4j.internal.recordstorage;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

import org.neo4j.graphdb.Direction;
import org.neo4j.storageengine.api.Degrees;

class EagerDegrees implements Degrees
{
    private static final int[] UNKNOWNS = new int[3];

    private final MutableIntObjectMap<int[]> degrees = IntObjectMaps.mutable.empty();

    void add( int type, int outgoing, int incoming, int total )
    {
        degrees.put( type, new int[]{outgoing, incoming, total} );
    }

    @Override
    public int[] types()
    {
        return degrees.keySet().toArray();
    }

    @Override
    public int degree( int type, Direction direction )
    {
        return degrees.getIfAbsent( type, () -> UNKNOWNS )[direction.ordinal()];
    }
}
