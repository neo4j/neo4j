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
 * This can be used to calculate the diameter of a network, which is defined as
 * the largest eccentricity of all the nodes in the network.
 * @complexity The distance comparator is called once for every vertex. The main
 *             work is the computation of {@link Eccentricity} for all vertices
 *             which is done in O(n * (A + n)) time.
 * @author Patrik Larsson
 */
public class NetworkDiameter<ShortestPathCostType> extends
    ShortestPathBasedCentrality<ShortestPathCostType,ShortestPathCostType>
{
    Comparator<ShortestPathCostType> distanceComparator;
    // Underlying eccentricity computation
    protected Eccentricity<ShortestPathCostType> eccentricity;
    protected ShortestPathCostType diameter;

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
     *            Object being able to compare eccentricity values (path
     *            distances), in order to sort out the largest.
     */
    public NetworkDiameter(
        SingleSourceShortestPath<ShortestPathCostType> singleSourceShortestPath,
        ShortestPathCostType zeroValue, Set<Node> nodeSet,
        Comparator<ShortestPathCostType> distanceComparator )
    {
        super( singleSourceShortestPath, null, zeroValue, nodeSet );
        this.distanceComparator = distanceComparator;
        eccentricity = new Eccentricity<ShortestPathCostType>(
            singleSourceShortestPath, zeroValue, nodeSet, distanceComparator );
    }

    @Override
    public void processShortestPaths( Node node,
        SingleSourceShortestPath<ShortestPathCostType> singleSourceShortestPath )
    {
        eccentricity.processShortestPaths( node, singleSourceShortestPath );
        ShortestPathCostType centrality = eccentricity.getCentrality( node );
        if ( diameter == null
            || distanceComparator.compare( centrality, diameter ) > 0 )
        {
            diameter = centrality;
        }
    }

    @Override
    public ShortestPathCostType getCentrality( Node node )
    {
        // This might be a bit ugly, but good for warnings
        if ( node != null )
        {
            throw new RuntimeException(
                "Getting network diameter with a specific node as argument, which means nonsense." );
        }
        calculate();
        return diameter;
    }
}
