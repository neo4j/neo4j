/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.centrality;

import java.util.Comparator;
import java.util.Set;
import org.neo4j.api.core.Node;
import org.neo4j.graphalgo.shortestpath.SingleSourceShortestPath;

/**
 * This can be used to calculate the radius of a network, which is defined as
 * the smallest eccentricity of all the nodes in the network.
 * @complexity Identical to {@link NetworkDiameter}.
 * @author Patrik Larsson
 */
public class NetworkRadius<ShortestPathCostType> extends
    ShortestPathBasedCentrality<ShortestPathCostType,ShortestPathCostType>
{
    Comparator<ShortestPathCostType> distanceComparator;
    // Underlying eccentricity computation
    protected Eccentricity<ShortestPathCostType> eccentricity;
    protected ShortestPathCostType radius;

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
     *            distances), in order to sort out the smallest.
     */
    public NetworkRadius(
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
        if ( radius == null
            || distanceComparator.compare( centrality, radius ) < 0 )
        {
            radius = centrality;
        }
    }

    @Override
    public ShortestPathCostType getCentrality( Node node )
    {
        // This might be a bit ugly, but good for warnings
        if ( node != null )
        {
            throw new RuntimeException(
                "Getting network radius with a specific node as argument, which means nonsense." );
        }
        calculate();
        return radius;
    }
}
