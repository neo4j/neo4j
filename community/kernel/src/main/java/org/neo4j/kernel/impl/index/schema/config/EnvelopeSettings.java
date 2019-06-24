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

import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.Envelope;
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
    private Double[] min;
    private Double[] max;

    EnvelopeSettings( CoordinateReferenceSystem crs )
    {
        this.crs = crs;
        this.min = new Double[crs.getDimension()];
        this.max = new Double[crs.getDimension()];
        Arrays.fill( this.min, Double.NaN );
        Arrays.fill( this.max, Double.NaN );
    }

    static HashMap<CoordinateReferenceSystem,EnvelopeSettings> envelopeSettingsFromConfig( Config config )
    {
        HashMap<CoordinateReferenceSystem,EnvelopeSettings> env = new HashMap<>();
        config.getGroups( CrsConfig.class ).forEach( ( id, crsConfig ) ->
        {
            EnvelopeSettings envelopeSettings = new EnvelopeSettings( crsConfig.crs );
            envelopeSettings.min = config.get( crsConfig.min ).toArray( Double[]::new );
            envelopeSettings.max = config.get( crsConfig.max ).toArray( Double[]::new );
            env.put( crsConfig.crs, envelopeSettings );
        } );

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
