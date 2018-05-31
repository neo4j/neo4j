/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.javacompat;

import org.neo4j.cypher.internal.CacheTracer;
import org.neo4j.cypher.internal.StringCacheMonitor;

/**
 * Adapter for passing CacheTraces into the Monitoring infrastructure.
 */
public class MonitoringCacheTracer implements CacheTracer<String>
{
    private final StringCacheMonitor monitor;

    public MonitoringCacheTracer( StringCacheMonitor monitor )
    {
        this.monitor = monitor;
    }

    @Override
    public void queryCacheHit( String queryKey, String metaData )
    {
        monitor.cacheHit( queryKey );
    }

    @Override
    public void queryCacheMiss( String queryKey, String metaData )
    {
        monitor.cacheMiss( queryKey );
    }

    @Override
    public void queryCacheStale( String queryKey, int secondsSincePlan, String metaData )
    {
        monitor.cacheDiscard( queryKey, metaData, secondsSincePlan );
    }

    @Override
    public void queryCacheFlush( long sizeOfCacheBeforeFlush )
    {
        monitor.cacheFlushDetected( sizeOfCacheBeforeFlush );
    }
}
