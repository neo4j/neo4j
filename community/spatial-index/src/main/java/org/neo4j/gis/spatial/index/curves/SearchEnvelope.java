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
package org.neo4j.gis.spatial.index.curves;

import org.neo4j.gis.spatial.index.Envelope;

/**
 * N-dimensional searchEnvelope
 */
class SearchEnvelope
{
    private long[] min; // inclusive lower bounds
    private long[] max; // exclusive upper bounds
    private int nbrDim;

    SearchEnvelope( SpaceFillingCurve curve, Envelope referenceEnvelope )
    {
        this.min = curve.getNormalizedCoord( referenceEnvelope.getMin() );
        this.max = curve.getNormalizedCoord( referenceEnvelope.getMax() );
        this.nbrDim = referenceEnvelope.getDimension();
        for ( int i = 0; i < nbrDim; i++ )
        {
            // getNormalizedCoord gives inclusive bounds. Need to increment to make the upper exclusive.
            this.max[i] += 1;
        }
    }

    private SearchEnvelope( long[] min, long[] max )
    {
        this.min = min;
        this.max = max;
        this.nbrDim = min.length;
    }

    SearchEnvelope( long min, long max, int nbrDim )
    {
        this.nbrDim = nbrDim;
        this.min = new long[nbrDim];
        this.max = new long[nbrDim];

        for ( int dim = 0; dim < nbrDim; dim++ )
        {
            this.min[dim] = min;
            this.max[dim] = max;
        }
    }

    SearchEnvelope quadrant( int[] quadNbrs )
    {
        long[] newMin = new long[nbrDim];
        long[] newMax = new long[nbrDim];

        for ( int dim = 0; dim < nbrDim; dim++ )
        {
            long extent = (max[dim] - min[dim]) / 2;
            newMin[dim] = this.min[dim] + quadNbrs[dim] * extent;
            newMax[dim] = this.min[dim] + (quadNbrs[dim] + 1) * extent;
        }
        return new SearchEnvelope( newMin, newMax );
    }

    boolean contains( long[] coord )
    {
        for ( int dim = 0; dim < nbrDim; dim++ )
        {
            if ( coord[dim] < min[dim] || coord[dim] >= max[dim] )
            {
                return false;
            }
        }
        return true;
    }

    boolean intersects( SearchEnvelope other )
    {
        for ( int dim = 0; dim < nbrDim; dim++ )
        {
            if ( this.max[dim] <= other.min[dim] || other.max[dim] <= this.min[dim] )
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Calculates the faction of the overlapping area between {@code this} and {@code} other compared
     * to the area of {@code this}.
     *
     * Must only be called for intersecting envelopes
     */
    double fractionOf( SearchEnvelope other )
    {
        double fraction = 1.0;
        for ( int i = 0; i < nbrDim; i++ )
        {
            long min = Math.max( this.min[i], other.min[i] );
            long max = Math.min( this.max[i], other.max[i] );
            final double innerFraction = (double) (max - min) / (double) (other.max[i] - other.min[i]);
            fraction *= innerFraction;
        }
        return fraction;
    }

    /**
     * The smallest possible envelope has unit area 1
     */
    public long getArea()
    {
        long area = 1;
        for ( int i = 0; i < nbrDim; i++ )
        {
            area *= max[i] - min[i];
        }
        return area;
    }
}
