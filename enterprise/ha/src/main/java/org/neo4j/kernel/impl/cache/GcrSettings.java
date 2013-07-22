/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.helpers.Settings;

import static org.neo4j.helpers.Settings.DURATION;
import static org.neo4j.helpers.Settings.FLOAT;
import static org.neo4j.helpers.Settings.NO_DEFAULT;
import static org.neo4j.helpers.Settings.range;
import static org.neo4j.helpers.Settings.setting;

/**
 * Settings for the GCR cache
 *
 */
public class GcrSettings
{
    public static final Setting<Long> node_cache_size = setting( "node_cache_size", Settings.BYTES, NO_DEFAULT );
    public static final Setting<Long> relationship_cache_size =
            setting( "relationship_cache_size", Settings.BYTES,NO_DEFAULT );

    @SuppressWarnings("unchecked")
    public static final Setting<Float> node_cache_array_fraction =
            setting( "node_cache_array_fraction", FLOAT, "1.0",range( 1.0f, 10.0f ) );

    @SuppressWarnings("unchecked")
    public static final Setting<Float> relationship_cache_array_fraction =
            setting("relationship_cache_array_fraction", FLOAT, "1.0", range( 1.0f, 10.0f ) );

    public static final Setting<Long> log_interval = setting( "gcr_cache_min_log_interval", DURATION, "60s" );
}
