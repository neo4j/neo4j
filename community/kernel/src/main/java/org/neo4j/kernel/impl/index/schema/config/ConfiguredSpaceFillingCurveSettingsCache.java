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
package org.neo4j.kernel.impl.index.schema.config;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * These settings affect the creation of the 2D (or 3D) to 1D mapper.
 * Changing these will change the values of the 1D mapping, but this will not invalidate existing indexes. They store the settings used to create
 * them, and will not use these settings at all. Changes will only affect future indexes made. In order to change existing indexes, you will need
 * to drop and recreate any indexes you wish to affect.
 */
public class ConfiguredSpaceFillingCurveSettingsCache
{
    private final int maxBits;
    private final HashMap<CoordinateReferenceSystem,SpaceFillingCurveSettings> settings = new HashMap<>();

    public ConfiguredSpaceFillingCurveSettingsCache( Config config )
    {
        this.maxBits = config.get( SpatialIndexSettings.space_filling_curve_max_bits );
        HashMap<CoordinateReferenceSystem,EnvelopeSettings> env = EnvelopeSettings.envelopeSettingsFromConfig( config );
        for ( Map.Entry<CoordinateReferenceSystem,EnvelopeSettings> entry : env.entrySet() )
        {
            CoordinateReferenceSystem crs = entry.getKey();
            settings.put( crs, SpaceFillingCurveSettingsFactory.fromConfig( this.maxBits, entry.getValue() ) );
        }
    }

    /**
     * The space filling curve is configured up front to cover a specific region of 2D (or 3D) space,
     * and the mapping tree is configured up front to have a specific maximum depth. These settings
     * are stored in an instance of SpaceFillingCurveSettings and are determined by the Coordinate
     * Reference System, and any neo4j.conf settings to override the CRS defaults.
     *
     * @return The default settings for the specified coordinate reference system
     */
    public SpaceFillingCurveSettings forCRS( CoordinateReferenceSystem crs )
    {
        if ( settings.containsKey( crs ) )
        {
            return settings.get( crs );
        }
        else
        {
            return SpaceFillingCurveSettingsFactory.fromConfig( maxBits, new EnvelopeSettings( crs ) );
        }
    }
}
