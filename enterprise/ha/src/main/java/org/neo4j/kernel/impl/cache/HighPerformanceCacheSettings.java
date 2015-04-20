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

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.kernel.configuration.Internal;

import static org.neo4j.helpers.Settings.BYTES;
import static org.neo4j.helpers.Settings.DURATION;
import static org.neo4j.helpers.Settings.FLOAT;
import static org.neo4j.helpers.Settings.NO_DEFAULT;
import static org.neo4j.helpers.Settings.range;
import static org.neo4j.helpers.Settings.setting;

/**
 * Settings for the High-Performance Cache
 *
 */
@Description( "High Performance Cache configuration settings" )
public class HighPerformanceCacheSettings
{
    @Description( "Maximum size of the heap memory to dedicate to the cached nodes. "
            + "Right before the maximum size is reached a purge is performed. "
            + "The purge will evict objects from the cache until the cache size gets below 90% of the maximum size. "
            + "Optimal settings for the maximum size depends on the size of your graph. "
            + "The configured maximum size should leave enough room for other objects to coexist in the same JVM. "
            + "At the same time it should be large enough to keep loading from the low level cache at a minimum. "
            + "Predicted load on the JVM as well as layout of domain level objects should also be taken into consideration." )
    public static final Setting<Long> node_cache_size = setting( "node_cache_size", BYTES, NO_DEFAULT );

    @Description( "Maximum size of the heap memory to dedicate to the cached relationships. "
                  + "See node_cache_size for more information." )
    public static final Setting<Long> relationship_cache_size =
            setting( "relationship_cache_size", BYTES,NO_DEFAULT );

    @Description( "Fraction of the heap to dedicate to the array holding the nodes in the cache. "
            + "Specifying `5` will let that array itself take up 5% out of the entire heap. "
            + "Increasing this figure will reduce the chance of hash collisions at the expense of more heap used for it. "
            + "More collisions means more redundant loading of objects from the low level cache." )
    @SuppressWarnings("unchecked")
    public static final Setting<Float> node_cache_array_fraction =
            setting( "node_cache_array_fraction", FLOAT, "1.0",range( 1.0f, 10.0f ) );

    @Description( "Fraction of the heap to dedicate to the array holding the relationships in the cache. "
                  + "See node_cache_array_fraction for more information." )
    @SuppressWarnings("unchecked")
    public static final Setting<Float> relationship_cache_array_fraction =
            setting("relationship_cache_array_fraction", FLOAT, "1.0", range( 1.0f, 10.0f ) );

    @Description( "Set how much of the memory available for caching to use for caching. "
                  + "It is recommended to not have this value exceed 70 percent." )
    @SuppressWarnings("unchecked")
    public static final Setting<HPCMemoryConfig> cache_memory =
            setting( "cache.memory_ratio", HPCSettingFunctions.CACHE_MEMORY_RATIO, HPCSettingFunctions.DEFAULT,
                    HPCSettingFunctions.OTHER_CACHE_SETTINGS_OVERRIDE, HPCSettingFunctions.TOTAL_NOT_ALLOWED_ABOVE_HEAP );

    @Internal
    public static final Setting<Long> log_interval = setting( "high_performance_cache_min_log_interval", DURATION, "60s" );



}
