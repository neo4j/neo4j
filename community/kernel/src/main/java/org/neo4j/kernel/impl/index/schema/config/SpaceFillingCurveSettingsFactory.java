/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Function;

import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * <p>
 * This factory can be used to create new space filling curve settings for use in configuring the curves.
 * These settings can be created either by defaults from the neo4j.conf file (see ConfiguredSpaceFullCurveSettingsCache)
 * or from reading the header of an existing GBPTree based index.
 */
public abstract class SpaceFillingCurveSettingsFactory
{
    private static final double DEFAULT_MIN_EXTENT = -1000000;
    private static final double DEFAULT_MAX_EXTENT = 1000000;
    private static final double DEFAULT_MIN_LATITUDE = -90;
    private static final double DEFAULT_MAX_LATITUDE = 90;
    private static final double DEFAULT_MIN_LONGITUDE = -180;
    private static final double DEFAULT_MAX_LONGITUDE = 180;

    /**
     * This method builds the default index configuration object for the specified CRS and other config options.
     * Currently we only support a SingleSpaceFillingCurveSettings which is the best option for cartesian, but
     * not necessarily the best for geographic coordinate systems.
     */
    static SpaceFillingCurveSettings fromConfig( int maxBits, EnvelopeSettings envelopeSettings )
    {
        // Currently we support only one type of index, but in future we could support different types for different CRS
        return new SpaceFillingCurveSettings.SettingsFromConfig( envelopeSettings.crs.getDimension(), maxBits, envelopeSettings.asEnvelope() );
    }

    public static SpaceFillingCurveSettings fromGBPTree( File indexFile, PageCache pageCache, Function<ByteBuffer,String> onError ) throws IOException
    {
        SpaceFillingCurveSettings.SettingsFromIndexHeader settings = new SpaceFillingCurveSettings.SettingsFromIndexHeader();
        GBPTree.readHeader( pageCache, indexFile, settings.headerReader( onError ) );
        if ( settings.isFailed() )
        {
            throw new IOException( settings.getFailureMessage() );
        }
        return settings;
    }

    static class EnvelopeSettings
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

        private double minOrDefault( int i, double defVal )
        {
            return valOrDefault( min[i], defVal );
        }

        private double maxOrDefault( int i, double defVal )
        {
            return valOrDefault( max[i], defVal );
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

        private static double valOrDefault( double val, double def )
        {
            return Double.isNaN( val ) ? def : val;
        }
    }

    enum SpatialIndexType
    {
        SingleSpaceFillingCurve( 1 )
                {
                    @Override
                    public void writeHeader( SpaceFillingCurveSettings settings, PageCursor cursor )
                    {
                        cursor.putInt( settings.maxLevels );
                        cursor.putInt( settings.dimensions );
                        double[] min = settings.extents.getMin();
                        double[] max = settings.extents.getMax();
                        for ( int i = 0; i < settings.dimensions; i++ )
                        {
                            cursor.putLong( Double.doubleToLongBits( min[i] ) );
                            cursor.putLong( Double.doubleToLongBits( max[i] ) );
                        }
                    }

                    @Override
                    public void readHeader( SpaceFillingCurveSettings.SettingsFromIndexHeader settings, ByteBuffer headerBytes )
                    {
                        try
                        {
                            settings.maxLevels = headerBytes.getInt();
                            settings.dimensions = headerBytes.getInt();
                            double[] min = new double[settings.dimensions];
                            double[] max = new double[settings.dimensions];
                            for ( int i = 0; i < settings.dimensions; i++ )
                            {
                                min[i] = headerBytes.getDouble();
                                max[i] = headerBytes.getDouble();
                            }
                            settings.extents = new Envelope( min, max );
                        }
                        catch ( BufferUnderflowException e )
                        {
                            settings.markAsFailed( "Failed to read settings from GBPTree header: " + e.getMessage() );
                        }
                    }
                };
        int id;

        public abstract void writeHeader( SpaceFillingCurveSettings settings, PageCursor cursor );

        public abstract void readHeader( SpaceFillingCurveSettings.SettingsFromIndexHeader settingsFromIndexHeader, ByteBuffer headerBytes );

        SpatialIndexType( int id )
        {
            this.id = id;
        }

        static SpatialIndexType get( int id )
        {
            for ( SpatialIndexType type : values() )
            {
                if ( type.id == id )
                {
                    return type;
                }
            }
            return null;
        }
    }
}
