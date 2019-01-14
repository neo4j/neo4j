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
package org.neo4j.cypher.internal.javacompat;

import scala.collection.immutable.Map;

import org.neo4j.cypher.internal.CacheTracer;
import org.neo4j.cypher.internal.StringCacheMonitor;
import org.neo4j.helpers.collection.Pair;

/**
 * Adapter for passing CacheTraces into the Monitoring infrastructure.
 */
public class MonitoringCacheTracer implements CacheTracer<Pair<String,scala.collection.immutable.Map<String, Class<?>>>>
{
    private final StringCacheMonitor monitor;

    public MonitoringCacheTracer( StringCacheMonitor monitor )
    {
        this.monitor = monitor;
    }

    @Override
    public void queryCacheHit( Pair<String,scala.collection.immutable.Map<String, Class<?>>> queryKey, String metaData )
    {
        monitor.cacheHit( queryKey );
    }

    @Override
    public void queryCacheMiss( Pair<String,scala.collection.immutable.Map<String, Class<?>>> queryKey, String metaData )
    {
        monitor.cacheMiss( queryKey );
    }

    @Override
    public void queryCacheRecompile( Pair<String,Map<String,Class<?>>> queryKey, String metaData )
    {
        monitor.cacheRecompile( queryKey );
    }

    @Override
    public void queryCacheStale( Pair<String,scala.collection.immutable.Map<String, Class<?>>> queryKey, int secondsSincePlan, String metaData )
    {
        monitor.cacheDiscard( queryKey, metaData, secondsSincePlan );
    }

    @Override
    public void queryCacheFlush( long sizeOfCacheBeforeFlush )
    {
        monitor.cacheFlushDetected( sizeOfCacheBeforeFlush );
    }
}
