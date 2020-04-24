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
package org.neo4j.cypher.internal.javacompat;

import scala.Option;

import org.neo4j.cypher.internal.CacheTracer;
import org.neo4j.cypher.internal.ExecutionEngineQueryCacheMonitor;
import org.neo4j.cypher.internal.QueryCache;
import org.neo4j.internal.helpers.collection.Pair;

/**
 * Adapter from ExecutionEngineQueryCacheMonitor to CacheTracer.
 *
 * The reason why we
 * a) need an adapter and
 * b) tracers for the ExpressionEngine query cache and the CypherPlanner query cache need to implement two different
 *    interfaces (ExecutionEngineQueryCacheMonitor and CacheTracer respectively)
 *
 *  is that kernel monitors work by interface methods. If tracers for those two caches shared the abstract methods
 *  in the same superclass, the monitor callbacks would always be invoked from both caches. So we need this
 *  awful mumbo-jumbo in order to monitor specifically one of the two caches only.
 */
public class MonitoringCacheTracer implements CacheTracer<Pair<String,QueryCache.ParameterTypeMap>>
{
    private final ExecutionEngineQueryCacheMonitor monitor;

    public MonitoringCacheTracer( ExecutionEngineQueryCacheMonitor monitor )
    {
        this.monitor = monitor;
    }

    @Override
    public void queryCacheHit( Pair<String,QueryCache.ParameterTypeMap> queryKey, String metaData )
    {
        monitor.cacheHit( queryKey );
    }

    @Override
    public void queryCacheMiss( Pair<String,QueryCache.ParameterTypeMap> queryKey, String metaData )
    {
        monitor.cacheMiss( queryKey );
    }

    @Override
    public void queryCompile( Pair<String,QueryCache.ParameterTypeMap> queryKey, String metaData )
    {
        monitor.cacheCompile( queryKey );
    }

    @Override
    public void queryCompileWithExpressionCodeGen( Pair<String,QueryCache.ParameterTypeMap> queryKey, String metaData )
    {
        monitor.cacheCompileWithExpressionCodeGen( queryKey );
    }

    @Override
    public void queryCacheStale( Pair<String,QueryCache.ParameterTypeMap> queryKey, int secondsSincePlan, String metaData,
                                 Option<String> maybeReason )
    {
        monitor.cacheDiscard( queryKey, metaData, secondsSincePlan, maybeReason );
    }

    @Override
    public void queryCacheFlush( long sizeOfCacheBeforeFlush )
    {
        monitor.cacheFlushDetected( sizeOfCacheBeforeFlush );
    }
}
