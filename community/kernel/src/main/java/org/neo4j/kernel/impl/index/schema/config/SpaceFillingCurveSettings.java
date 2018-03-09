/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve2D;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;

/**
 * These settings affect the creation of the 2D (or 3D) to 1D mapper.
 * Changing these will change the values of the 1D mapping, and require re-indexing, so
 * once data has been indexed, do not change these without recreating the index.
 */
public class SpaceFillingCurveSettings
{
    private static final String SPATIAL_INDEX_TYPE = "SingleSpaceFillingCurve";
    private int dimensions;
    private int maxLevels;
    private Envelope extents;

    public SpaceFillingCurveSettings( int dimensions, int maxBits, Envelope extents )
    {
        this.dimensions = dimensions;
        this.extents = extents;
        int maxConfigured = maxBits / dimensions;
        int maxSupported = (dimensions == 2) ? HilbertSpaceFillingCurve2D.MAX_LEVEL : HilbertSpaceFillingCurve3D.MAX_LEVEL;
        this.maxLevels = Math.min( maxConfigured, maxSupported );
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
    public int maxLevels()
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
    public String toString()
    {
        return String.format( "Space filling curves settings: dimensions=%d, maxLevels=%d, min=%s, max=%s", dimensions, maxLevels,
                Arrays.toString( extents.getMin() ), Arrays.toString( extents.getMax() ) );
    }

    public void read( BufferedReader in ) throws IOException
    {
        this.dimensions = -1;
        this.maxLevels = -1;
        double[] min = null;
        double[] max = null;

        String line;
        while ( (line = in.readLine()) != null )
        {
            String[] fields = line.trim().split( "\\t", 2 );
            switch ( fields[0] )
            {
            case "indexType":
                if ( !fields[1].equals( SPATIAL_INDEX_TYPE ) )
                {
                    throw new IllegalArgumentException( "Unknown spatial index type: " + fields[1] );
                }
                break;
            case "dimensions":
                this.dimensions = Integer.parseInt( fields[1].trim() );
                break;
            case "maxLevels":
                this.maxLevels = Integer.parseInt( fields[1].trim() );
                break;
            case "min":
                min = splitDoubleArray( fields[1].trim() );
                break;
            case "max":
                max = splitDoubleArray( fields[1].trim() );
                break;
            default:
                break;
            }
        }

        if ( min == null || max == null || dimensions < 2 || maxLevels < 0 || min.length != this.dimensions || max.length != this.dimensions )
        {
            throw new IllegalArgumentException(
                    String.format( "Invalid space filling curves settings: dimensions=%d, maxLevels=%d, min=%s, max=%s", dimensions, maxLevels,
                            Arrays.toString( min ), Arrays.toString( max ) ) );
        }
        this.extents = new Envelope( min, max );
    }

    private double[] splitDoubleArray( String field )
    {
        return Arrays.stream( field.substring( 1, field.length() - 1 ).split( "," ) ).mapToDouble( Double::parseDouble ).toArray();
    }

    public void write( PrintWriter out ) throws IOException
    {
        out.println( "indexType\t" + SPATIAL_INDEX_TYPE );
        out.println( "dimensions\t" + dimensions );
        out.println( "maxLevels\t" + maxLevels );
        out.println( "min\t" + Arrays.toString( extents.getMin() ) );
        out.println( "max\t" + Arrays.toString( extents.getMax() ) );
    }
}
