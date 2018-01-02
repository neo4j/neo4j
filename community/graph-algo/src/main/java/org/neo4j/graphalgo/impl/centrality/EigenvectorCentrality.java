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
package org.neo4j.graphalgo.impl.centrality;

import org.neo4j.graphdb.Node;

/**
 * Interface representing the algorithms for computing eigenvector centrality.
 * NOTE: Currently only works on Doubles.
 * @author Patrik Larsson
 */
public interface EigenvectorCentrality
{
    public static double DETACHED_VERTEX_CENTRALITY = 0d;
    /**
     * This can be used to retrieve the result for every node. Might return null
     * if the node is not contained in the node set initially given.
     * Return {@link #DETACHED_VERTEX_CENTRALITY} if node has no relationships.
     * @param node
     *            The node for which we would like the value.
     * @return the centrality value for the given node.
     */
    public Double getCentrality( Node node );

    /**
     * This resets the calculation if we for some reason would like to redo it.
     */
    public void reset();

    /**
     * Internal calculate method that will do the calculation. This can however
     * be called externally to manually trigger the calculation.
     */
    public void calculate();
}
