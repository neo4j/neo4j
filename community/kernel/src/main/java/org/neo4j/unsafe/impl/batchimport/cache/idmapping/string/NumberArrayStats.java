/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.unsafe.impl.batchimport.cache.NumberArray;

/**
 * Keeps simple stats about f.ex a {@link NumberArray}. Keeps {@link #size()} and {@link #highestIndex()},
 * either {@link #register(long) registered per value} or {@link #set(long, long)} specifically.
 */
public class NumberArrayStats
{
    private long size;
    private long highestIndex = -1;

    public void register( long index )
    {
        size++;
        if ( index > highestIndex )
        {
            highestIndex = index;
        }
    }

    public void set( long size, long highestIndex )
    {
        this.size = size;
        this.highestIndex = highestIndex;
    }

    public long size()
    {
        return size;
    }

    public long highestIndex()
    {
        return highestIndex;
    }
}
