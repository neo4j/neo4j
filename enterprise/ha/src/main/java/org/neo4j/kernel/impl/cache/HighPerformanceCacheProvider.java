/*
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
        HPCMemoryConfig mem = config.get( HighPerformanceCacheSettings.cache_memory );
        if(mem.source() == HPCMemoryConfig.Source.SPECIFIC_OVERRIDING_RATIO)
        {
            logger.warn( "Explicit cache memory ratio configuration is ignored, because advanced cache memory " +
                    "configuration has been specified. Please specify either only the ratio configuration, or only " +
                    "the advanced configuration." );
        }
        return new HighPerformanceCache<>( mem.nodeCacheSize(), mem.nodeLookupTableFraction(),
                config.get( HighPerformanceCacheSettings.log_interval ),
                NODE_CACHE_NAME, logger, monitors.newMonitor( HighPerformanceCache.Monitor.class ) );
    }

    @Override
    public Cache<RelationshipImpl> newRelationshipCache( StringLogger logger, Config config, Monitors monitors )
    {
        HPCMemoryConfig mem = config.get( HighPerformanceCacheSettings.cache_memory );
        return new HighPerformanceCache<>( mem.relCacheSize(), mem.relLookupTableFraction(),
                config.get( HighPerformanceCacheSettings.log_interval ),
                RELATIONSHIP_CACHE_NAME, logger, monitors.newMonitor( HighPerformanceCache.Monitor.class ) );
    }

    @Override
    public Class getSettingsClass()
    {
        return HighPerformanceCacheSettings.class;
    }
}
