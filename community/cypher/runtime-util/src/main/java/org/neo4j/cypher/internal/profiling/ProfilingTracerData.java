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
package org.neo4j.cypher.internal.profiling;

import java.util.Arrays;

import org.neo4j.cypher.result.OperatorProfile;

public class ProfilingTracerData implements OperatorProfile
{
    private long time;
    private long dbHits;
    private long rows;
    private long pageCacheHits;
    private long pageCacheMisses;
    private long maxAllocatedMemory;

    public void update( long time, long dbHits, long rows, long pageCacheHits, long pageCacheMisses, long maxAllocatedMemory )
    {
        this.time += time;
        this.dbHits += dbHits;
        this.rows += rows;
        this.pageCacheHits += pageCacheHits;
        this.pageCacheMisses += pageCacheMisses;
        this.maxAllocatedMemory += maxAllocatedMemory;
    }

    @Override
    public long time()
    {
        return time;
    }

    @Override
    public long dbHits()
    {
        return dbHits;
    }

    @Override
    public long rows()
    {
        return rows;
    }

    @Override
    public long pageCacheHits()
    {
        return pageCacheHits;
    }

    @Override
    public long pageCacheMisses()
    {
        return pageCacheMisses;
    }

    @Override
    public long maxAllocatedMemory()
    {
        return maxAllocatedMemory;
    }

    public void sanitize()
    {
        if ( time < OperatorProfile.NO_DATA )
        {
            time = OperatorProfile.NO_DATA;
        }
        if ( dbHits < OperatorProfile.NO_DATA )
        {
            dbHits = OperatorProfile.NO_DATA;
        }
        if ( rows < OperatorProfile.NO_DATA )
        {
            rows = OperatorProfile.NO_DATA;
        }
        if ( pageCacheHits < OperatorProfile.NO_DATA )
        {
            pageCacheHits = OperatorProfile.NO_DATA;
        }
        if ( pageCacheMisses < OperatorProfile.NO_DATA )
        {
            pageCacheMisses = OperatorProfile.NO_DATA;
        }
        if ( maxAllocatedMemory < OperatorProfile.NO_DATA )
        {
            maxAllocatedMemory = OperatorProfile.NO_DATA;
        }
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( new long[]{this.time(), this.dbHits(), this.rows(), this.pageCacheHits(), this.pageCacheMisses(), this.maxAllocatedMemory()} );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( !(o instanceof OperatorProfile) )
        {
            return false;
        }
        OperatorProfile that = (OperatorProfile) o;
        return this.time() == that.time() &&
               this.dbHits() == that.dbHits() &&
               this.rows() == that.rows() &&
               this.pageCacheHits() == that.pageCacheHits() &&
               this.pageCacheMisses() == that.pageCacheMisses() &&
               this.maxAllocatedMemory() == that.maxAllocatedMemory();
    }

    @Override
    public String toString()
    {
        return String.format( "Operator Profile { time: %d, dbHits: %d, rows: %d, page cache hits: %d, page cache misses: %d, max allocated: %d }",
                              this.time(),
                              this.dbHits(),
                              this.rows(),
                              this.pageCacheHits(),
                              this.pageCacheMisses(),
                              this.maxAllocatedMemory() );
    }
}
