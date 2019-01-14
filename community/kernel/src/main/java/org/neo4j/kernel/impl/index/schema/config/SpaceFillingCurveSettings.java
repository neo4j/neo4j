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

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve2D;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Header;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_FAILED;

/**
 * <p>
 * These settings affect the creation of the 2D (or 3D) to 1D mapper.
 * Changing these will change the values of the 1D mapping, and require re-indexing, so
 * once data has been indexed, do not change these without recreating the index.
 * </p>
 * <p>
 * Key data maintained by this class include:
 * <dl>
 *     <dt>dimensions</dt>
 *         <dd>either 2 or 3 for 2D or 3D</dd>
 *     <dt>maxLevels<dt>
 *         <dd>the number of levels in the tree that models the 2D to 1D mapper calculated as maxBits / dimensions</dd>
 *     <dt>extents</dt>
 *         <dd>The space filling curve is configured up front to cover a specific region of 2D (or 3D) space.
 * Any points outside this space will be mapped as if on the edges. This means that if these extents
 * do not match the real extents of the data being indexed, the index will be less efficient. Making
 * the extents too big means than only a small area is used causing more points to map to fewer 1D
 * values and requiring more post filtering. If the extents are too small, many points will lie on
 * the edges, and also cause additional post-index filtering costs.</dd>
 *     <dt>failureMessage</dt>
 *         <dd>The settings are read from the GBPTree header structure, but when this is a FAILED index, there are no settings, but instead an error message
 * describing the failure. If that happens, code that triggered the read should check this field and react accordingly. If the the value is null, there
 * was no failure.</dd>
 * </dl>
 * </p>
 */
public abstract class SpaceFillingCurveSettings
{
    protected int dimensions;
    int maxLevels;
    Envelope extents;

    static SpaceFillingCurveSettings fromConfig( int dimensions, int maxBits, Envelope extents )
    {
        return new SettingsFromConfig( dimensions, maxBits, extents );
    }

    public static SpaceFillingCurveSettings fromGBPTree( File indexFile, PageCache pageCache, Function<ByteBuffer,String> onError ) throws IOException
    {
        SettingsFromIndexHeader settings = new SettingsFromIndexHeader();
        GBPTree.readHeader( pageCache, indexFile, settings.headerReader( onError ) );
        if ( settings.isFailed() )
        {
            throw new IOException( settings.getFailureMessage() );
        }
        return settings;
    }

    private static class SettingsFromConfig extends SpaceFillingCurveSettings
    {
        private SettingsFromConfig( int dimensions, int maxBits, Envelope extents )
        {
            super( dimensions, extents, calcMaxLevels( dimensions, maxBits ) );
        }

        private static int calcMaxLevels( int dimensions, int maxBits )
        {
            int maxConfigured = maxBits / dimensions;
            int maxSupported = (dimensions == 2) ? HilbertSpaceFillingCurve2D.MAX_LEVEL : HilbertSpaceFillingCurve3D.MAX_LEVEL;
            return Math.min( maxConfigured, maxSupported );
        }
    }

    private static class SettingsFromIndexHeader extends SpaceFillingCurveSettings
    {
        private String failureMessage;

        private SettingsFromIndexHeader()
        {
            super( 0, null, 0 );
        }

        private void markAsFailed( String failureMessage )
        {
            this.failureMessage = failureMessage;
        }

        private void markAsSucceeded()
        {
            this.failureMessage = null;
        }

        /**
         * The settings are read from the GBPTree header structure, but when this is a FAILED index, there are no settings, but instead an error message
         * describing the failure. If that happens, code that triggered the read should check this field and react accordingly. If the the value is null, there
         * was no failure.
         */
        private String getFailureMessage()
        {
            return failureMessage;
        }

        /**
         * The settings are read from the GBPTree header structure, but when this is a FAILED index, there are no settings, but instead an error message
         * describing the failure. If that happens, code that triggered the read should check this. If the value is true, calling getFailureMessage() will
         * provide an error message describing the failure.
         */
        private boolean isFailed()
        {
            return failureMessage != null;
        }

        private Header.Reader headerReader( Function<ByteBuffer,String> onError )
        {
            return headerBytes ->
            {
                byte state = headerBytes.get();
                if ( state == BYTE_FAILED )
                {
                    this.failureMessage = "Unexpectedly trying to read the header of a failed index: " + onError.apply( headerBytes );
                }
                else
                {
                    int typeId = headerBytes.getInt();
                    SpatialIndexType indexType = SpatialIndexType.get( typeId );
                    if ( indexType == null )
                    {
                        markAsFailed( "Unknown spatial index type in index header: " + typeId );
                    }
                    else
                    {
                        markAsSucceeded();
                        indexType.readHeader( this, headerBytes );
                    }
                }
            };
        }
    }

    public Consumer<PageCursor> headerWriter( byte initialIndexState )
    {
        return cursor ->
        {
            cursor.putByte( initialIndexState );
            cursor.putInt( SpatialIndexType.SingleSpaceFillingCurve.id );
            SpatialIndexType.SingleSpaceFillingCurve.writeHeader( this, cursor );
        };
    }

    private SpaceFillingCurveSettings( int dimensions, Envelope extents, int maxLevels )
    {
        this.dimensions = dimensions;
        this.extents = extents;
        this.maxLevels = maxLevels;
    }

    /**
     * @return The number of dimensions (2D or 3D)
     */
    public int getDimensions()
    {
        return dimensions;
    }

    /**
     * @return The number of levels in the 2D (or 3D) to 1D mapping tree.
     */
    public int getMaxLevels()
    {
        return maxLevels;
    }

    /**
     * The space filling curve is configured up front to cover a specific region of 2D (or 3D) space.
     * Any points outside this space will be mapped as if on the edges. This means that if these extents
     * do not match the real extents of the data being indexed, the index will be less efficient. Making
     * the extents too big means than only a small area is used causing more points to map to fewer 1D
     * values and requiring more post filtering. If the extents are too small, many points will lie on
     * the edges, and also cause additional post-index filtering costs.
     *
     * @return the extents of the 2D (or 3D) region that is covered by the space filling curve.
     */
    public Envelope indexExtents()
    {
        return extents;
    }

    /**
     * Make an instance of the SpaceFillingCurve that can perform the 2D (or 3D) to 1D mapping based on these settings.
     *
     * @return a configured instance of SpaceFillingCurve
     */
    public SpaceFillingCurve curve()
    {
        if ( dimensions == 2 )
        {
            return new HilbertSpaceFillingCurve2D( extents, maxLevels );
        }
        else if ( dimensions == 3 )
        {
            return new HilbertSpaceFillingCurve3D( extents, maxLevels );
        }
        else
        {
            throw new IllegalArgumentException( "Cannot create spatial index with other than 2D or 3D coordinate reference system: " + dimensions + "D" );
        }
    }

    @Override
    public int hashCode()
    {
        // dimension is also represented in the extents and so not explicitly included here
        return 31 * extents.hashCode() + maxLevels;
    }

    public boolean equals( SpaceFillingCurveSettings other )
    {
        return this.dimensions == other.dimensions && this.maxLevels == other.maxLevels && this.extents.equals( other.extents );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof SpaceFillingCurveSettings )
        {
            return equals( (SpaceFillingCurveSettings) obj );
        }
        else
        {
            return false;
        }
    }

    @Override
    public String toString()
    {
        return String.format( "Space filling curves settings: dimensions=%d, maxLevels=%d, min=%s, max=%s", dimensions, maxLevels,
                Arrays.toString( extents.getMin() ), Arrays.toString( extents.getMax() ) );
    }

    private enum SpatialIndexType
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
                    public void readHeader( SettingsFromIndexHeader settings, ByteBuffer headerBytes )
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
        private int id;

        public abstract void writeHeader( SpaceFillingCurveSettings settings, PageCursor cursor );

        public abstract void readHeader( SettingsFromIndexHeader settingsFromIndexHeader, ByteBuffer headerBytes );

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
