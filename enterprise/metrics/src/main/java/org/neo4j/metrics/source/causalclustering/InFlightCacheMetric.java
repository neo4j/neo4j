/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.metrics.source.causalclustering;

import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCacheMonitor;

public class InFlightCacheMetric implements InFlightCacheMonitor
{
    private volatile long misses;
    private volatile long hits;
    private volatile long totalBytes;
    private volatile long maxBytes;
    private volatile int elementCount;
    private volatile int maxElements;

    @Override
    public void miss()
    {
        misses++;
    }

    @Override
    public void hit()
    {
        hits++;
    }

    public long getMisses()
    {
        return misses;
    }

    public long getHits()
    {
        return hits;
    }

    public long getMaxBytes()
    {
        return maxBytes;
    }

    public long getTotalBytes()
    {
        return totalBytes;
    }

    public long getMaxElements()
    {
        return maxElements;
    }

    public long getElementCount()
    {
        return elementCount;
    }

    @Override
    public void setMaxBytes( long maxBytes )
    {
        this.maxBytes = maxBytes;
    }

    @Override
    public void setTotalBytes( long totalBytes )
    {
        this.totalBytes = totalBytes;
    }

    @Override
    public void setMaxElements( int maxElements )
    {
        this.maxElements = maxElements;
    }

    @Override
    public void setElementCount( int elementCount )
    {
        this.elementCount = elementCount;
    }
}
