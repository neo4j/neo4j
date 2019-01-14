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

public interface SpaceFillingCurveMonitor
{
    /**
     * Tells the monitor that a range was added at a certain depth in the space filling curve.
     * Can be used to build a histogram showing how many ranges were added at which depth.
     *
     * @param depth the current recursion depth
     */
    void addRangeAtDepth( int depth );

    /**
     * Tell the monitor about the size of the search area in normalized space.
     */
    void registerSearchArea( long size );

    /**
     * Tell the monitor that a new area of the search space was covered (with the given size)
     * by adding a range.
     */
    void addToCoveredArea( long size );
}
