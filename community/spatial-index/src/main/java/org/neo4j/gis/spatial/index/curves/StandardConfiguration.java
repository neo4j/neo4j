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
package org.neo4j.gis.spatial.index.curves;

import org.neo4j.gis.spatial.index.Envelope;

public class StandardConfiguration implements SpaceFillingCurveConfiguration
{
    public static final int DEFAULT_EXTRA_LEVELS = 1;
    protected int extraLevels;

    StandardConfiguration()
    {
        this( DEFAULT_EXTRA_LEVELS );
    }

    StandardConfiguration( int extraLevels )
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
     * traverse one level shallower. This is achieved by a log of the ration of areas,
     * to the base of 4 (for 2D).
     * <p>
     * {@inheritDoc}
     */
    @Override
    public int maxDepth( Envelope referenceEnvelope, Envelope range, int nbrDim, int maxLevel )
    {
        double searchRatio = range.getArea() / referenceEnvelope.getArea();
        final int i = (int) (Math.log( searchRatio ) / Math.log( Math.pow( 2, nbrDim ) )) + extraLevels;
        return i;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" + extraLevels + ")";
    }
}
