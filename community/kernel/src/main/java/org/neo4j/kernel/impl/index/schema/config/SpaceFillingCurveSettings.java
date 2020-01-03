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
import java.util.Objects;

import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve2D;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;

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
 * </dl>
 * </p>
 */
public class SpaceFillingCurveSettings
{
    /**
     * Number of bits to use for the space filling curve value in 1D.
     * This number affects the physical structure of indexed spatial values
     * and thus changing it is a format change for btree indexes.
     * 60 because it needs to be divisible by both 2 and 3 to be usable by both
     * 2D and 3D mapping.
     */
    private static final int NBR_OF_BITS = 60;
    private final int dimensions;
    private final int maxLevels;
    private final Envelope extents;

    public SpaceFillingCurveSettings( int dimensions, Envelope extents )
    {
        this.dimensions = dimensions;
        this.extents = extents;
        this.maxLevels = NBR_OF_BITS / dimensions;
    }

    /**
     * @return The number of dimensions (2D or 3D)
     */
    public int getDimensions()
    {
        return dimensions;
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

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        SpaceFillingCurveSettings that = (SpaceFillingCurveSettings) o;
        return dimensions == that.dimensions && maxLevels == that.maxLevels && Objects.equals( extents, that.extents ); }

    @Override
    public String toString()
    {
        return String.format( "Space filling curves settings: dimensions=%d, maxLevels=%d, min=%s, max=%s", dimensions, maxLevels,
                Arrays.toString( extents.getMin() ), Arrays.toString( extents.getMax() ) );
    }
}
