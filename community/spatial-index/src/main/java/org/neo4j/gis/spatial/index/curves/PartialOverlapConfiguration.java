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

public class PartialOverlapConfiguration extends StandardConfiguration
{
    private static double TOP_THRESHOLD = 0.99;
    private static double BOTTOM_THRESHOLD = 0.5;
    private double topThreshold;
    private double bottomThreshold;

    public PartialOverlapConfiguration()
    {
        this( StandardConfiguration.DEFAULT_EXTRA_LEVELS, TOP_THRESHOLD, BOTTOM_THRESHOLD );
    }

    public PartialOverlapConfiguration( int extraLevels, double topThreshold, double bottomThreshold )
    {
        super( extraLevels );
        this.topThreshold = topThreshold;
        this.bottomThreshold = bottomThreshold;
    }

    /**
     * This simply stops at the maxDepth calculated in the maxDepth() function, or
     * if the overlap is over some fraction 99% (by default) at the top levels, but reduces
     * linearly to 0.5 (by default) when we get to maxDepth.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public boolean stopAtThisDepth( double overlap, int depth, int maxDepth )
    {
        double slope = (bottomThreshold - topThreshold) / maxDepth;
        double threshold = slope * depth + topThreshold;
        return overlap >= threshold || depth >= maxDepth;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" + extraLevels + "," + topThreshold + "," + bottomThreshold + ")";
    }
}
