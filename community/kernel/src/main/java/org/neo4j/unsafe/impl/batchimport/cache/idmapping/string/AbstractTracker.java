/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArray;

/**
 * Base implementation of {@link Tracker} over a {@link NumberArray}.
 *
 * @param <ARRAY> type of {@link NumberArray} in this implementation.
 */
abstract class AbstractTracker<ARRAY extends NumberArray> implements Tracker
{
    static final int DEFAULT_VALUE = -1;

    protected ARRAY array;

    protected AbstractTracker( ARRAY array )
    {
        this.array = array;
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        array.acceptMemoryStatsVisitor( visitor );
    }

    @Override
    public void swap( long fromIndex, long toIndex, int count )
    {
        array.swap( fromIndex, toIndex, count );
    }
}
