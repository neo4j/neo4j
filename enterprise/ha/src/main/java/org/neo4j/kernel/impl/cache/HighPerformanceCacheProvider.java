/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.cache;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;

@Service.Implementation(CacheProvider.class)
public class HighPerformanceCacheProvider extends CacheProvider
{
    public static final String NAME = "hpc";

    public HighPerformanceCacheProvider()
    {
        super( NAME, "High-Performance Cache" );
    }

    @Override
    public Cache<NodeImpl> newNodeCache( StringLogger logger, Config config, Monitors monitors )
    {
        Long node = config.get( HighPerformanceCacheSettings.node_cache_size );
        if ( node == null )
        {
            node = Runtime.getRuntime().maxMemory() / 4;
        }

        Long rel = config.get( HighPerformanceCacheSettings.relationship_cache_size );
        if ( rel == null )
        {
            rel = Runtime.getRuntime().maxMemory() / 4;
        }

        checkMemToUse( logger, node, rel, Runtime.getRuntime().maxMemory() );
        return new HighPerformanceCache<>( node, config.get( HighPerformanceCacheSettings.node_cache_array_fraction ),
                config.get( HighPerformanceCacheSettings.log_interval ),
                NODE_CACHE_NAME, logger, monitors.newMonitor( HighPerformanceCache.Monitor.class ) );
    }

    @Override
    public Cache<RelationshipImpl> newRelationshipCache( StringLogger logger, Config config, Monitors monitors )
    {
        Long node = config.get( HighPerformanceCacheSettings.node_cache_size );
        if ( node == null )
        {
            node = Runtime.getRuntime().maxMemory() / 4;
        }

        Long rel = config.get( HighPerformanceCacheSettings.relationship_cache_size );
        if ( rel == null )
        {
            rel = Runtime.getRuntime().maxMemory() / 4;
        }

        checkMemToUse( logger, node, rel, Runtime.getRuntime().maxMemory() );
        return new HighPerformanceCache<>( rel, config.get( HighPerformanceCacheSettings
                .relationship_cache_array_fraction ), config.get( HighPerformanceCacheSettings.log_interval ),
                RELATIONSHIP_CACHE_NAME, logger, monitors.newMonitor( HighPerformanceCache.Monitor.class ) );
    }

    // TODO: Move into validation method of config setting?
    @SuppressWarnings("boxing")
    private void checkMemToUse( StringLogger logger, long node, long rel, long available )
    {
        long advicedMax = available / 2;
        long total = 0;
        node = Math.max( HighPerformanceCache.MIN_SIZE, node );
        total += node;
        rel = Math.max( HighPerformanceCache.MIN_SIZE, rel );
        total += rel;
        if ( total > available )
        {
            throw new IllegalArgumentException(
                    String.format( "Configured cache memory limits (node=%s, relationship=%s, " +
                            "total=%s) exceeds available heap space (%s)",
                            node, rel, total, available ) );
        }
        if ( total > advicedMax )
        {
            logger.logMessage( String.format( "Configured cache memory limits(node=%s, relationship=%s, " +
                    "total=%s) exceeds recommended limit (%s)",
                    node, rel, total, advicedMax ) );
        }
    }

    @Override
    public Class getSettingsClass()
    {
        return HighPerformanceCacheSettings.class;
    }
}
