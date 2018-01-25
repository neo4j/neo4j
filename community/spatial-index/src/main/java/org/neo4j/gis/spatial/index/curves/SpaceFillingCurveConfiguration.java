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

public interface SpaceFillingCurveConfiguration
{

    /**
     * Decides whether to stop at this depth or recurse deeper.
     *
     * @param overlap the overlap between search space and the current extent
     * @param depth the current recursion depth
     * @param maxDepth the maximum depth that was calculated to recurse to,
     * @return if the algorithm should recurse deeper, returns {@code false}; if the algorithm
     * should stop at this depth, returns {@code true}
     */
    boolean stopAtThisDepth( double overlap, int depth, int maxDepth );

    /**
     * Decide how deep to recurse at max.
     *
     * @param referenceEnvelope the envelope describing the search area
     * @param range the envelope describing the indexed area
     * @param nbrDim the number of dimensions
     * @param maxLevel the depth of the spaceFillingCurve
     * @return the maximum depth to which the algorithm should recurse in the space filling curve.
     */
    int maxDepth( Envelope referenceEnvelope, Envelope range, int nbrDim, int maxLevel );
}
