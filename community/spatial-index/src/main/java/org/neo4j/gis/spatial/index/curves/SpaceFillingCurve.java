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
package org.neo4j.gis.spatial.index.curves;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.gis.spatial.index.Envelope;

/**
 * This class is also used by Neo4j Spatial
 */

public abstract class SpaceFillingCurve
{
    /**
     * Description of the space filling curve structure
     */
    abstract static class CurveRule
    {
        final int dimension;
        final int[] npointValues;

        CurveRule( int dimension, int[] npointValues )
        {
            this.dimension = dimension;
            this.npointValues = npointValues;
            assert npointValues.length == length();
        }

        final int length()
        {
            return 1 << dimension;
        }

        int npointForIndex( int derivedIndex )
        {
            return npointValues[derivedIndex];
        }

        int indexForNPoint( int npoint )
        {
            for ( int index = 0; index < npointValues.length; index++ )
            {
                if ( npointValues[index] == npoint )
                {
                    return index;
                }
            }
            return -1;
        }

        abstract CurveRule childAt( int npoint );
    }

    private final Envelope range;
    private final int nbrDim;
    private final int maxLevel;
    private final long width;
    private final long valueWidth;
    private final int quadFactor;
    private final long initialNormMask;

    private double[] scalingFactor;

    SpaceFillingCurve( Envelope range, int maxLevel )
    {
        this.range = range;
        this.nbrDim = range.getDimension();
        this.maxLevel = maxLevel;
        if ( maxLevel < 1 )
        {
            throw new IllegalArgumentException( "Hilbert index needs at least one level" );
        }
        if ( range.getDimension() > 3 )
        {
            throw new IllegalArgumentException( "Hilbert index does not yet support more than 3 dimensions" );
        }
        this.width = 1L << maxLevel;
        this.scalingFactor = new double[nbrDim];
        for ( int dim = 0; dim < nbrDim; dim++ )
        {
            scalingFactor[dim] = this.width / range.getWidth( dim );
        }
        this.valueWidth = 1L << maxLevel * nbrDim;
        this.initialNormMask = ((1L << nbrDim) - 1) << (maxLevel - 1) * nbrDim;
        this.quadFactor = 1 << nbrDim;
    }

    public int getMaxLevel()
    {
        return maxLevel;
    }

    public long getWidth()
    {
        return width;
    }

    public long getValueWidth()
    {
        return valueWidth;
    }

    public double getTileWidth( int dimension, int level )
    {
        return range.getWidth( dimension ) / ( 1 << level );
    }

    public Envelope getRange()
    {
        return range;
    }

    protected abstract CurveRule rootCurve();

    /**
     * Given a coordinate in multiple dimensions, calculate its derived key for maxLevel
     * Needs to be public due to dependency from Neo4j Spatial
     */
    public Long derivedValueFor( double[] coord )
    {
        return derivedValueFor( coord, maxLevel );
    }

    /**
     * Given a coordinate in multiple dimensions, calculate its derived key for given level
     */
    private Long derivedValueFor( double[] coord, int level )
    {
        assertValidLevel( level );
        long[] normalizedValues = getNormalizedCoord( coord );
        return derivedValueFor( normalizedValues, level );
    }

    /**
     * Given a normalized coordinate in multiple dimensions, calculate its derived key for maxLevel
     */
    public Long derivedValueFor( long[] normalizedValues )
    {
        return derivedValueFor( normalizedValues, maxLevel );
    }

    /**
     * Given a normalized coordinate in multiple dimensions, calculate its derived key for given level
     */
    private Long derivedValueFor( long[] normalizedValues, int level )
    {
        assertValidLevel( level );
        long derivedValue = 0;
        long mask = 1L << (maxLevel - 1);

        // The starting curve depends on the dimensions
        CurveRule currentCurve = rootCurve();

        for ( int i = 1; i <= maxLevel; i++ )
        {
            int bitIndex = maxLevel - i;
            int npoint = 0;

            for ( long val : normalizedValues )
            {
                npoint = npoint << 1 | (int) ((val & mask) >> bitIndex);
            }

            int derivedIndex = currentCurve.indexForNPoint( npoint );
            derivedValue = (derivedValue << nbrDim) | derivedIndex;
            mask = mask >> 1;
            currentCurve = currentCurve.childAt( derivedIndex );
        }

        if ( level < maxLevel )
        {
            derivedValue = derivedValue << (nbrDim * maxLevel - level);
        }
        return derivedValue;
    }

    /**
     * Given a derived key, find the center coordinate of the corresponding tile at maxLevel
     */
    public double[] centerPointFor( long derivedValue )
    {
        return centerPointFor( derivedValue, maxLevel );
    }

    /**
     * Given a derived key, find the center coordinate of the corresponding tile at given level
     */
    private double[] centerPointFor( long derivedValue, int level )
    {
        long[] normalizedCoord = normalizedCoordinateFor( derivedValue, level );
        return getDoubleCoord( normalizedCoord, level );
    }

    /**
     * Given a derived key, find the normalized coordinate it corresponds to on a specific level
     */
    long[] normalizedCoordinateFor( long derivedValue, int level )
    {
        assertValidLevel( level );
        long mask = initialNormMask;
        long[] coordinate = new long[nbrDim];

        // First level is a single curveUp
        CurveRule currentCurve = rootCurve();

        for ( int i = 1; i <= level; i++ )
        {

            int bitIndex = maxLevel - i;

            int derivedIndex = (int) ((derivedValue & mask) >> bitIndex * nbrDim);
            int npoint = currentCurve.npointForIndex( derivedIndex );

            for ( int dim = 0; dim < nbrDim; dim++ )
            {
                coordinate[dim] = coordinate[dim] << 1 | (npoint >> nbrDim - dim - 1) & 1;
            }

            mask = mask >> nbrDim;
            currentCurve = currentCurve.childAt( derivedIndex );
        }

        if ( level < maxLevel )
        {
            for ( int dim = 0; dim < nbrDim; dim++ )
            {
                coordinate[dim] = coordinate[dim] << maxLevel - level;
            }
        }

        return coordinate;
    }

    /**
     * Given an envelope, find a collection of LongRange of tiles intersecting it on maxLevel and merge adjacent ones
     */
    List<LongRange> getTilesIntersectingEnvelope( Envelope referenceEnvelope )
    {
        return getTilesIntersectingEnvelope( referenceEnvelope.getMin(), referenceEnvelope.getMax(), new StandardConfiguration() );
    }

    public List<LongRange> getTilesIntersectingEnvelope( double[] fromOrNull, double[] toOrNull, SpaceFillingCurveConfiguration config )
    {
        double[] from = fromOrNull == null ? range.getMin() : Arrays.copyOf( fromOrNull, fromOrNull.length );
        double[] to = toOrNull == null ? range.getMax() : Arrays.copyOf( toOrNull, toOrNull.length );

        for ( int i = 0; i < from.length; i++ )
        {
            if ( from[i] > to[i] )
            {
                if ( fromOrNull == null )
                {
                    to[i] = from[i];
                }
                else if ( toOrNull == null )
                {
                    from[i] = to[i];
                }
                else
                {
                    throw new IllegalArgumentException( "Invalid range, min greater than max: " + from[i] + " > " + to[i] );
                }
            }
        }
        Envelope referenceEnvelope = new Envelope( from, to );
        return getTilesIntersectingEnvelope( referenceEnvelope, config, null );
    }

    List<LongRange> getTilesIntersectingEnvelope( Envelope referenceEnvelope, SpaceFillingCurveConfiguration config, SpaceFillingCurveMonitor monitor )
    {
        SearchEnvelope search = new SearchEnvelope( this, referenceEnvelope );
        SearchEnvelope wholeExtent = new SearchEnvelope( 0, this.getWidth(), nbrDim );
        List<LongRange> results = new ArrayList<>( config.initialRangesListCapacity() );

        if ( monitor != null )
        {
            monitor.registerSearchArea( search.getArea() );
        }

        addTilesIntersectingEnvelopeAt( config, monitor,
                0, config.maxDepth( referenceEnvelope, this.range, nbrDim, maxLevel ), search,
                wholeExtent, rootCurve(), 0, this.getValueWidth(), results );
        return results;
    }

    private void addTilesIntersectingEnvelopeAt( SpaceFillingCurveConfiguration config, SpaceFillingCurveMonitor monitor, int depth, int maxDepth,
            SearchEnvelope search, SearchEnvelope currentExtent, CurveRule curve, long left, long right, List<LongRange> results )
    {
        assert search.intersects( currentExtent );

        if ( depth > 0 && config.stopAtThisDepth( search.fractionOf( currentExtent ), depth, maxDepth ) )
        {
            computeTilesIntersectionEnvelopeAt( monitor, depth, currentExtent, left, right - 1, results );
        }
        else
        {
            long width = (right - left) / quadFactor;
            for ( int i = 0; i < quadFactor; i++ )
            {
                SearchEnvelope quadrant = currentExtent.quadrant( curve.npointForIndex( i ) );
                if ( width == 1L )
                {
                    long[] coord = normalizedCoordinateFor( left + i, maxLevel );
                    if ( search.contains( coord ) )
                    {
                        computeTilesIntersectionEnvelopeAt( monitor, depth, quadrant, left + i, left + i, results );
                    }
                }
                else if ( search.intersects( quadrant ) )
                {
                    addTilesIntersectingEnvelopeAt( config, monitor, depth + 1, maxDepth, search, quadrant,
                            curve.childAt( i ), left + i * width,
                            left + (i + 1) * width, results );
                }
            }
        }
    }

    private void computeTilesIntersectionEnvelopeAt( SpaceFillingCurveMonitor monitor, int depth,
            SearchEnvelope currentExtent, long left, long newMax, List<LongRange> results )
    {
        // Note that LongRange upper bound is inclusive, hence the '-1' in several places
        LongRange current = results.isEmpty() ? null : results.get( results.size() - 1 );
        if ( current != null && current.max == left - 1 )
        {
            current.expandToMax( newMax );
        }
        else
        {
            current = new LongRange( left, newMax );
            results.add( current );
        }
        if ( monitor != null )
        {
            monitor.addRangeAtDepth( depth );
            monitor.addToCoveredArea( currentExtent.getArea() );
        }
    }

    /**
     * Given a coordinate, find the corresponding normalized coordinate
     */
    long[] getNormalizedCoord( double[] coord )
    {
        long[] normalizedCoord = new long[nbrDim];

        for ( int dim = 0; dim < nbrDim; dim++ )
        {
            double value = clamp( coord[dim], range.getMin( dim ), range.getMax( dim ) );
            // Avoiding awkward rounding errors
            if ( value - range.getMin( dim ) == range.getMax( dim ) - range.getMin( dim ) )
            {
                normalizedCoord[dim] = width - 1;
            }
            else
            {
                /*
                 * We are converting a world coordinate in range [min,max) to a long-int coordinate in range [0,width).
                 * The fact that the origins are not aligned means we can get numerical rounding errors of points near the world origin, but far from
                 * the normalized origin, due to very high precision in doubles near 0.0, and much lower precision of doubles of values far from 0.0.
                 * The symptom of this is points very close to tile edges end up in the adjacent tiles instead.
                 * We fix this by first converting to normalized coordinates, and then using the new tile as a new origin,
                 * and re-converting based on that origin.
                 * This should lead to a number of 0, which means we're in the origin tile (no numerical rounding errors),
                 * but when an error occurs, we could have a tile offset of +1 or -1, and we move to the adjacent tile instead.
                 */
                normalizedCoord[dim] = (long) ((value - range.getMin(dim)) * scalingFactor[dim]);
                // Calculating with an origin at the min can lead to numerical rouding errors, which can be corrected by recalculating using a closer origin
                double tileCenter = ((double) normalizedCoord[dim]) / scalingFactor[dim] + range.getMin(dim) + getTileWidth(dim, maxLevel) / 2.0;
                // The 1E-16 is to create the behavior of the [min,max) bounds without an expensive if...else if...else check
                long normalizedOffset = (long) ((value - tileCenter) * scalingFactor[dim] - 0.5 + 1E-16);
                // normalizedOffset is almost always 0, but can be +1 or -1 if there were rounding errors we need to correct for
                normalizedCoord[dim] += normalizedOffset;
            }
        }
        return normalizedCoord;
    }

    /**
     * Given a normalized coordinate, find the center coordinate of that tile  on the given level
     */
    private double[] getDoubleCoord( long[] normalizedCoord, int level )
    {
        double[] coord = new double[nbrDim];

        for ( int dim = 0; dim < nbrDim; dim++ )
        {
            double coordinate = ((double) normalizedCoord[dim]) / scalingFactor[dim] + range.getMin( dim ) + getTileWidth( dim, level ) / 2.0;
            coord[dim] = clamp( coordinate, range.getMin( dim ), range.getMax( dim ) );
        }
        return coord;
    }

    private static double clamp( double val, double min, double max )
    {
        if ( val <= min )
        {
            return min;
        }
        if ( val >= max )
        {
            return max;
        }
        return val;
    }

    /**
     * Assert that a given level is valid
     */
    private void assertValidLevel( int level )
    {
        if ( level > maxLevel )
        {
            throw new IllegalArgumentException( "Level " + level + " greater than max-level " + maxLevel );
        }
    }

    /**
     * Class for ranges of tiles
     */
    public static class LongRange
    {
        public final long min;
        public long max;

        LongRange( long value )
        {
            this( value, value );
        }

        LongRange( long min, long max )
        {
            this.min = min;
            this.max = max;
        }

        void expandToMax( long other )
        {
            this.max = other;
        }

        @Override
        public boolean equals( Object other )
        {
            return (other instanceof LongRange) && this.equals( (LongRange) other );
        }

        public boolean equals( LongRange other )
        {
            return this.min == other.min && this.max == other.max;
        }

        @Override
        public int hashCode()
        {
            return (int) (this.min << 16 + this.max);
        }

        @Override
        public String toString()
        {
            return "LongRange(" + min + "," + max + ")";
        }
    }
}
