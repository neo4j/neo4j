package org.neo4j.kernel.impl.cache;

import static org.neo4j.helpers.Settings.DURATION;
import static org.neo4j.helpers.Settings.FLOAT;
import static org.neo4j.helpers.Settings.NO_DEFAULT;
import static org.neo4j.helpers.Settings.range;
import static org.neo4j.helpers.Settings.setting;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.Settings;

/**
 * Settings for the GCR cache
 *
 */
public class GcrSettings
{
    public static Setting<Long> node_cache_size = setting( "node_cache_size", Settings.BYTES, NO_DEFAULT );
    public static Setting<Long> relationship_cache_size =
            setting( "relationship_cache_size", Settings.BYTES,NO_DEFAULT );

    public static Setting<Float> node_cache_array_fraction =
            setting( "node_cache_array_fraction", FLOAT, "1.0",range( 1.0f, 10.0f ) );

    public static Setting<Float> relationship_cache_array_fraction =
            setting("relationship_cache_array_fraction", FLOAT, "1.0", range( 1.0f, 10.0f ) );

    public static Setting<Long> log_interval = setting( "gcr_cache_min_log_interval", DURATION, "60s" );
}
