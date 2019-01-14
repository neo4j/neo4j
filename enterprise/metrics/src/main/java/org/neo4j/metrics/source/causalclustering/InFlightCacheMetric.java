/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
