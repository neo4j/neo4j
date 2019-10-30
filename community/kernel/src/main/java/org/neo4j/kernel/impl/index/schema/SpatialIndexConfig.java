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
package org.neo4j.kernel.impl.index.schema;

import org.eclipse.collections.api.tuple.Pair;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Utility class with static method for extracting relevant spatial index configurations from {@link CoordinateReferenceSystem} and
 * {@link SpaceFillingCurveSettings}. Configurations will be put into a map, prefixed by {@link #SPATIAL_CONFIG_PREFIX} and
 * {@link CoordinateReferenceSystem#getName()}.
 * By using this class when extracting configurations we make sure that the same name and format is used for the same configuration.
 */
public final class SpatialIndexConfig
{
    static final String MIN = "min";
    static final String MAX = "max";
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
        double[] min = settings.indexExtents().getMin();
        double[] max = settings.indexExtents().getMax();
        addSpatialConfig( map, crs, min, max );
    }

    public static void addSpatialConfig( Map<String,Value> map, CoordinateReferenceSystem crs, double[] min, double[] max )
    {
        String crsName = crs.getName();
        map.put( key( crsName, MIN ), Values.doubleArray( min ) );
        map.put( key( crsName, MAX ), Values.doubleArray( max ) );
    }

    /**
     * Throws an {@link IllegalArgumentException} if the spatial settings in the given {@link IndexConfig} are invalid.
     */
    static void validateSpatialConfig( IndexConfig indexConfig )
    {
       extractSpatialConfig( indexConfig );
    }

    static IndexConfig addSpatialConfig( IndexConfig indexConfig, CoordinateReferenceSystem crs, SpaceFillingCurveSettings settings )
    {
        Map<String,Value> spatialConfig = new HashMap<>();
        addSpatialConfig( spatialConfig, crs, settings );
        for ( String key : spatialConfig.keySet() )
        {
            indexConfig = indexConfig.withIfAbsent( key, spatialConfig.get( key ) );
        }
        return indexConfig;
    }

    static Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> extractSpatialConfig( IndexConfig indexConfig )
    {
        HashMap<String,Map<String,Value>> configByCrsNames = groupConfigByCrsNames( indexConfig );
        return buildSettingsFromConfig( configByCrsNames );
    }

    private static HashMap<String,Map<String,Value>> groupConfigByCrsNames( IndexConfig indexConfig )
    {
        HashMap<String,Map<String,Value>> configByCrsNames = new HashMap<>();
        for ( Pair<String,Value> entry : indexConfig.entries() )
        {
            String key = entry.getOne();
            if ( key.startsWith( SPATIAL_CONFIG_PREFIX ) )
            {
                String[] split = key.split( "\\." );
                String prefix = split[1];
                String valueKey = split[2];
                configByCrsNames.compute( prefix, ( string, map ) -> {
                    if ( map == null )
                    {
                        map = new HashMap<>();
                    }
                    map.put( valueKey, entry.getTwo() );
                    return map;
                } );
            }
        }
        return configByCrsNames;
    }

    private static Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> buildSettingsFromConfig( HashMap<String,Map<String,Value>> configByCrsNames )
    {
        Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> settings = new HashMap<>();
        for ( String key : configByCrsNames.keySet() )
        {
            Map<String,Value> configByCrsName = configByCrsNames.get( key );
            double[] min = getDoubleArrayValue( configByCrsName, MIN ).asObjectCopy();
            double[] max = getDoubleArrayValue( configByCrsName, MAX ).asObjectCopy();

            final CoordinateReferenceSystem crs = CoordinateReferenceSystem.byName( key );
            Envelope extents = new Envelope( min, max );
            settings.put( crs, new SpaceFillingCurveSettings( crs.getDimension(), extents ) );
        }
        return settings;
    }

    static String key( String crsName, String key )
    {
        return SPATIAL_CONFIG_PREFIX + "." + crsName + "." + key;
    }

    private static DoubleArray getDoubleArrayValue( Map<String,Value> configByCrsName, String key )
    {
        Value value = configByCrsName.get( key );
        if ( value instanceof DoubleArray )
        {
            return (DoubleArray) value;
        }
        throw new IllegalStateException( "Expected key " + key + " to be mapped to a DoubleArray but was " + value );
    }
}
