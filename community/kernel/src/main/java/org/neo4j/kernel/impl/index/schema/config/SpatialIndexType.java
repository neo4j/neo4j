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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.io.pagecache.PageCursor;

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
