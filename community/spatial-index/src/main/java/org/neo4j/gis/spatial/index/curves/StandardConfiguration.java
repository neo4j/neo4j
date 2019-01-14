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

public class StandardConfiguration implements SpaceFillingCurveConfiguration
{
    public static final int DEFAULT_EXTRA_LEVELS = 1;

    /**
     * After estimating the search ratio, we know the level at which tiles have approximately the same size as
     * our search area. This number dictates the amount of levels we go deeper than that, to trim down the amount
     * of false positives.
     */
    protected int extraLevels;

    public StandardConfiguration()
    {
        this( DEFAULT_EXTRA_LEVELS );
    }

    public StandardConfiguration( int extraLevels )
    {
        this.extraLevels = extraLevels;
    }

    /**
     * This simply stops at the maxDepth calculated in the maxDepth() function, or
     * if the overlap is over 99%.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public boolean stopAtThisDepth( double overlap, int depth, int maxDepth )
    {
        return overlap >= 0.99 || depth >= maxDepth;
    }

    /**
     * If the search area is exactly one of the finest grained tiles (tile at maxLevel), then
     * we want the search to traverse to maxLevel, however, for each area that is 4x larger, we would
     * traverse one level shallower. This is achieved by a log (base 4 for 2D, base 8 for 3D) of the ratio of areas.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public int maxDepth( Envelope referenceEnvelope, Envelope range, int nbrDim, int maxLevel )
    {
        Envelope paddedEnvelope = referenceEnvelope.withSideRatioNotTooSmall();
        double searchRatio = range.getArea() / paddedEnvelope.getArea();
        if ( Double.isInfinite( searchRatio ) )
        {
            return maxLevel;
        }
        return Math.min( maxLevel, (int) (Math.log( searchRatio ) / Math.log( Math.pow( 2, nbrDim ) )) + extraLevels );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" + extraLevels + ")";
    }

    @Override
    public int initialRangesListCapacity()
    {
        // Probably big enough to for the majority of index queries.
        return 1000;
    }
}
