/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
public class HighPerformanceCacheSettings
{

    public static final Setting<Long> node_cache_size = setting( "node_cache_size", BYTES, NO_DEFAULT );
    public static final Setting<Long> relationship_cache_size = setting( "relationship_cache_size", BYTES,NO_DEFAULT );

    @SuppressWarnings("unchecked")
    public static final Setting<Float> node_cache_array_fraction =
            setting( "node_cache_array_fraction", FLOAT, "1.0",range( 1.0f, 10.0f ) );

    @SuppressWarnings("unchecked")
    public static final Setting<Float> relationship_cache_array_fraction =
            setting("relationship_cache_array_fraction", FLOAT, "1.0", range( 1.0f, 10.0f ) );

    @Description("Set the amount of usable memory to allocate to the high-level object cache, a value between " +
            "0 and 100. It is recommended to not have this value exceed 70. This cannot be used in conjunction with " +
            "other object cache size settings.")
    @SuppressWarnings("unchecked")
    public static final Setting<HPCMemoryConfig> cache_memory =
            setting( "cache.memory_ratio", HPCSettingFunctions.CACHE_MEMORY_RATIO, HPCSettingFunctions.DEFAULT,
                    HPCSettingFunctions.OTHER_CACHE_SETTINGS_OVERRIDE, HPCSettingFunctions.TOTAL_NOT_ALLOWED_ABOVE_HEAP );

    public static final Setting<Long> log_interval = setting( "high_performance_cache_min_log_interval", DURATION, "60s" );



}
