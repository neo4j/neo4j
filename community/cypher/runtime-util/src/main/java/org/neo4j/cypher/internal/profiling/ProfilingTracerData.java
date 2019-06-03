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
package org.neo4j.cypher.internal.profiling;

import org.neo4j.cypher.result.OperatorProfile;

public class ProfilingTracerData implements OperatorProfile
{
    private long time;
    private long dbHits;
    private long rows;
    private long pageCacheHits;
    private long pageCacheMisses;

    public void update( long time, long dbHits, long rows, long pageCacheHits, long pageCacheMisses )
    {
        this.time += time;
        this.dbHits += dbHits;
        this.rows += rows;
        this.pageCacheHits += pageCacheHits;
        this.pageCacheMisses += pageCacheMisses;
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
}
