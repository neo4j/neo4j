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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.ConfigValue;
import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * These settings affect the creation of the 2D (or 3D) to 1D mapper.
 * Changing these will change the values of the 1D mapping, but this will not invalidate existing indexes. They store the settings used to create
 * them, and will not use these settings at all. Changes will only affect future indexes made. In order to change existing indexes, you will need
 * to drop and recreate any indexes you wish to affect.
 */
public class SpaceFillingCurveSettingsFactory
{
    private int maxBits;
    private HashMap<CoordinateReferenceSystem,SpaceFillingCurveSettings> settings = new HashMap<>();
    private static final double DEFAULT_MIN_EXTENT = -1000000;
    private static final double DEFAULT_MAX_EXTENT = 1000000;
    private static final double DEFAULT_MIN_LATITUDE = -90;
    private static final double DEFAULT_MAX_LATITUDE = 90;
    private static final double DEFAULT_MIN_LONGITUDE = -180;
    private static final double DEFAULT_MAX_LONGITUDE = 180;
    private static final String SPATIAL_SETTING_PREFIX = "unsupported.dbms.db.spatial.crs.";

    private class EnvelopeSettings
    {
        CoordinateReferenceSystem crs;
        double[] min;
        double[] max;

        EnvelopeSettings( CoordinateReferenceSystem crs )
        {
            this.crs = crs;
            this.min = new double[crs.getDimension()];
            this.max = new double[crs.getDimension()];
            Arrays.fill( this.min, Double.NaN );
            Arrays.fill( this.max, Double.NaN );
        }

        Envelope asEnvelope()
        {
            return envelopeFromCRS( crs.getDimension(), crs.isGeographic(), this );
        }
    }

    public SpaceFillingCurveSettingsFactory( Config config )
    {
        this.maxBits = config.get( SpatialIndexSettings.space_filling_curve_max_bits );
        HashMap<CoordinateReferenceSystem,EnvelopeSettings> env = new HashMap<>();
        for ( Map.Entry<String,ConfigValue> entry : config.getConfigValues().entrySet() )
        {
            String key = entry.getKey();
            String value = entry.getValue().toString();
            if ( key.startsWith( SPATIAL_SETTING_PREFIX ) )
            {
                String[] fields = key.replace( SPATIAL_SETTING_PREFIX, "" ).split( "\\." );
                if ( fields.length != 3 )
                {
                    throw new IllegalArgumentException(
                            "Invalid spatial config settings, expected three fields after '" + SPATIAL_SETTING_PREFIX + "': " + key );
                }
                else
                {
                    CoordinateReferenceSystem crs = CoordinateReferenceSystem.byName( fields[0] );
                    EnvelopeSettings envelopeSettings = env.computeIfAbsent( crs, EnvelopeSettings::new );
                    int index = "xyz".indexOf( fields[1].toLowerCase() );
                    if ( index < 0 )
                    {
                        throw new IllegalArgumentException( "Invalid spatial coordinate key (should be one of 'x', 'y' or 'z'): " + fields[1] );
                    }
                    if ( index >= crs.getDimension() )
                    {
                        throw new IllegalArgumentException( "Invalid spatial coordinate key for " + crs.getDimension() + "D: " + fields[1] );
                    }
                    switch ( fields[2].toLowerCase() )
                    {
                    case "min":
                        envelopeSettings.min[index] = Double.parseDouble( value );
                        break;
                    case "max":
                        envelopeSettings.max[index] = Double.parseDouble( value );
                        break;
                    default:
                        throw new IllegalArgumentException( "Invalid spatial coordinate range key (should be one of 'max' or 'min'): " + fields[2] );
                    }
                }
            }
        }
        for ( Map.Entry<CoordinateReferenceSystem,EnvelopeSettings> entry : env.entrySet() )
        {
            CoordinateReferenceSystem crs = entry.getKey();
            settings.put( crs, SpaceFillingCurveSettings.fromConfig( crs.getDimension(), this.maxBits, entry.getValue().asEnvelope() ) );
        }
    }

    /**
     * The space filling curve is configured up front to cover a specific region of 2D (or 3D) space,
     * and the mapping tree is configured up front to have a specific maximum depth. These settings
     * are stored in an instance of SpaceFillingCurveSettings and are determined by the Coordinate
     * Reference System, and any neo4j.conf settings to override the CRS defaults.
     *
     * @return The settings for the specified coordinate reference system
     */
    public SpaceFillingCurveSettings settingsFor( CoordinateReferenceSystem crs )
    {
        if ( settings.containsKey( crs ) )
        {
            return settings.get( crs );
        }
        else
        {
            return SpaceFillingCurveSettings.fromConfig( crs.getDimension(), maxBits,
                    envelopeFromCRS( crs.getDimension(), crs.isGeographic(), new EnvelopeSettings( crs ) ) );
        }
    }

    private static Envelope envelopeFromCRS( int dimension, boolean geographic, EnvelopeSettings envelopeSettings )
    {
        assert dimension >= 2;
        double[] min = new double[dimension];
        double[] max = new double[dimension];
        int cartesianStartIndex = 0;
        if ( geographic )
        {
            // Geographic CRS default to extent of the earth in degrees
            min[0] = valOrDefault( envelopeSettings.min[0], DEFAULT_MIN_LONGITUDE );
            max[0] = valOrDefault( envelopeSettings.max[0], DEFAULT_MAX_LONGITUDE );
            min[1] = valOrDefault( envelopeSettings.min[1], DEFAULT_MIN_LATITUDE );
            max[1] = valOrDefault( envelopeSettings.max[1], DEFAULT_MAX_LATITUDE );
            cartesianStartIndex = 2;    // if geographic index has higher than 2D, then other dimensions are cartesian
        }
        for ( int i = cartesianStartIndex; i < dimension; i++ )
        {
            min[i] = valOrDefault( envelopeSettings.min[i], DEFAULT_MIN_EXTENT );
            max[i] = valOrDefault( envelopeSettings.max[i], DEFAULT_MAX_EXTENT );
        }
        return new Envelope( min, max );
    }

    private static double valOrDefault( double val, double def )
    {
        return Double.isNaN( val ) ? def : val;
    }
}
