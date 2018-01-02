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

import java.util.Comparator;
import java.util.Set;

import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphdb.Node;

/**
 * This can be used to calculate the eccentricity of nodes, which is defined as
 * the maximum distance to any other node.
 * @complexity Using a {@link SingleSourceShortestPath} algorithm with time
 *             complexity A, this algorithm runs in time O(A + n) for every
 *             vertex the eccentricity is to be computed for. Thus doing it for
 *             all vertices takes O(n * (A + n)) time.
 * @author Patrik Larsson
 */
public class Eccentricity<ShortestPathCostType> extends
    ShortestPathBasedCentrality<ShortestPathCostType,ShortestPathCostType>
{
    Comparator<ShortestPathCostType> distanceComparator;

    /**
     * Default constructor.
     * @param singleSourceShortestPath
     *            Underlying singleSourceShortestPath.
     * @param zeroValue
     *            Default value.
     * @param nodeSet
     *            A set containing the nodes for which centrality values should
     *            be computed.
     * @param distanceComparator
     *            Object being able to compare distances, in order to sort out
     *            the largest.
     */
    public Eccentricity(
        SingleSourceShortestPath<ShortestPathCostType> singleSourceShortestPath,
        ShortestPathCostType zeroValue, Set<Node> nodeSet,
        Comparator<ShortestPathCostType> distanceComparator )
    {
        super( singleSourceShortestPath, null, zeroValue, nodeSet );
        this.distanceComparator = distanceComparator;
    }

    /*
     * Since we dont need to do the calculation for all the nodes before we get
     * a usable result, we can just calculate the result for any given node when
     * it is asked for. This function just checks if the value has been computed
     * before, and computes it if needed.
     */
    @Override
    public ShortestPathCostType getCentrality( Node node )
    {
        ShortestPathCostType centrality = centralities.get( node );
        if ( centrality == null )
        {
            return null;
        }
        // Not calculated yet, or if it actually is 0 it is very fast to
        // compute so just do it.
        if ( centrality.equals( zeroValue ) )
        {
            singleSourceShortestPath.reset();
            singleSourceShortestPath.setStartNode( node );
            processShortestPaths( node, singleSourceShortestPath );
        }
        // When the value is calculated, just retrieve it normally
        return centralities.get( node );
    }

    @Override
    public void processShortestPaths( Node node,
        SingleSourceShortestPath<ShortestPathCostType> singleSourceShortestPath )
    {
        ShortestPathCostType maximumDistance = null;
        for ( Node targetNode : nodeSet )
        {
            ShortestPathCostType targetDistance = singleSourceShortestPath
                .getCost( targetNode );
            if ( maximumDistance == null
                || distanceComparator.compare( maximumDistance, targetDistance ) < 0 )
            {
                maximumDistance = targetDistance;
            }
        }
        if ( maximumDistance != null )
        {
            setCentralityForNode( node, maximumDistance );
        }
    }
}
