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
package org.neo4j.kernel.impl.index.schema;

import java.util.Map;

import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Utility class with static method for extracting relevant spatial index configurations from {@link CoordinateReferenceSystem} and
 * {@link SpaceFillingCurveSettings}. Configurations will be put into a map, prefixed by {@link #SPATIAL_CONFIG_PREFIX} and
 * {@link CoordinateReferenceSystem#getName()}.
 * By using this class when extracting configurations we make sure that the same name and format is used for the same configuration.
 */
final class SpatialIndexConfig
{
    private static final String SPATIAL_CONFIG_PREFIX = "spatial";

    private SpatialIndexConfig()
    {
    }

    /**
     * Extract spatial index configuration and put into provided map.
     *
     * @param map {@link Map} into which extracted configurations should be inserted.
     * @param crs {@link CoordinateReferenceSystem} from which to extract configurations.
     * @param settings {@link SpaceFillingCurveSettings} from which to extract configurations.
     */
    static void addSpatialConfig( Map<String,Value> map, CoordinateReferenceSystem crs, SpaceFillingCurveSettings settings )
    {
        String crsName = crs.getName();
        int tableId = crs.getTable().getTableId();
        int code = crs.getCode();
        int dimensions = settings.getDimensions();
        int maxLevels = settings.getMaxLevels();
        double[] min = settings.indexExtents().getMin();
        double[] max = settings.indexExtents().getMax();

        String prefix = prefix( crsName );
        map.put( prefix + ".tableId", Values.intValue( tableId ) );
        map.put( prefix + ".code", Values.intValue( code ) );
        map.put( prefix + ".dimensions", Values.intValue( dimensions ) );
        map.put( prefix + ".maxLevels", Values.intValue( maxLevels ) );
        map.put( prefix + ".min", Values.doubleArray( min ) );
        map.put( prefix + ".max", Values.doubleArray( max ) );
    }

    private static String prefix( String crsName )
    {
        return SPATIAL_CONFIG_PREFIX + "." + crsName;
    }
}
