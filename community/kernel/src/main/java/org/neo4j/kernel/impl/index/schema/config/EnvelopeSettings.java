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
package org.neo4j.kernel.impl.index.schema.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.ConfigValue;
import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.values.storable.CoordinateReferenceSystem;

class EnvelopeSettings
{
    private static final String SPATIAL_SETTING_PREFIX = "unsupported.dbms.db.spatial.crs.";
    private static final double DEFAULT_MIN_EXTENT = -1000000;
    private static final double DEFAULT_MAX_EXTENT = 1000000;
    private static final double DEFAULT_MIN_LATITUDE = -90;
    private static final double DEFAULT_MAX_LATITUDE = 90;
    private static final double DEFAULT_MIN_LONGITUDE = -180;
    private static final double DEFAULT_MAX_LONGITUDE = 180;

    private CoordinateReferenceSystem crs;
    private double[] min;
    private double[] max;

    EnvelopeSettings( CoordinateReferenceSystem crs )
    {
        this.crs = crs;
        this.min = new double[crs.getDimension()];
        this.max = new double[crs.getDimension()];
        Arrays.fill( this.min, Double.NaN );
        Arrays.fill( this.max, Double.NaN );
    }

    static HashMap<CoordinateReferenceSystem,EnvelopeSettings> envelopeSettingsFromConfig( Config config )
    {
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
        return env;
    }

    Envelope asEnvelope()
    {
        int dimension = crs.getDimension();
        assert dimension >= 2;
        double[] min = new double[dimension];
        double[] max = new double[dimension];
        int cartesianStartIndex = 0;
        if ( crs.isGeographic() )
        {
            // Geographic CRS default to extent of the earth in degrees
            min[0] = minOrDefault( 0, DEFAULT_MIN_LONGITUDE );
            max[0] = maxOrDefault( 0, DEFAULT_MAX_LONGITUDE );
            min[1] = minOrDefault( 1, DEFAULT_MIN_LATITUDE );
            max[1] = maxOrDefault( 1, DEFAULT_MAX_LATITUDE );
            cartesianStartIndex = 2;    // if geographic index has higher than 2D, then other dimensions are cartesian
        }
        for ( int i = cartesianStartIndex; i < dimension; i++ )
        {
            min[i] = minOrDefault( i, DEFAULT_MIN_EXTENT );
            max[i] = maxOrDefault( i, DEFAULT_MAX_EXTENT );
        }
        return new Envelope( min, max );
    }

    CoordinateReferenceSystem getCrs()
    {
        return crs;
    }

    private double minOrDefault( int i, double defVal )
    {
        return valOrDefault( min[i], defVal );
    }

    private double maxOrDefault( int i, double defVal )
    {
        return valOrDefault( max[i], defVal );
    }

    private static double valOrDefault( double val, double def )
    {
        return Double.isNaN( val ) ? def : val;
    }
}
