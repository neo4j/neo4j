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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.neo4j.unsafe.impl.batchimport.cache.LongArray;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

/**
 * Stores longs in a {@link LongArray} provided by {@link NumberArrayFactory}.
 */
public class LongCollisionValues implements CollisionValues
{
    private final LongArray cache;
    private long nextOffset;

    public LongCollisionValues( NumberArrayFactory factory, long length )
    {
        cache = factory.newLongArray( length, 0 );
    }

    @Override
    public long add( Object id )
    {
        long collisionIndex = nextOffset++;
        cache.set( collisionIndex, ((Number)id).longValue() );
        return collisionIndex;
    }

    @Override
    public Object get( long offset )
    {
        return cache.get( offset );
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        cache.acceptMemoryStatsVisitor( visitor );
    }

    @Override
    public void close()
    {
        cache.close();
    }
}
