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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.impl.shortestpath.Util;
import org.neo4j.graphalgo.impl.shortestpath.Util.PathCounter;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Class for computing betweenness centrality as defined by Linton C. Freeman
 * (1977) using the algorithm by Ulrik Brandes (2001).
 * @complexity: Using a {@link SingleSourceShortestPath} algorithm with time
 *              complexity A, this algorithm runs in time O(n * (A + m)).
 *              Examples: This becomes O(n * m) for BFS search and O(n^2 *
 *              log(n) + n * m) for Dijkstra.
 * @author Patrik Larsson
 * @param <ShortestPathCostType>
 *            The datatype used by the underlying
 *            {@link SingleSourceShortestPath} algorithm, i.e. the type the edge
 *            weights are represented by.
 */
public class BetweennessCentrality<ShortestPathCostType> extends
    ShortestPathBasedCentrality<Double,ShortestPathCostType>
{
    protected Double globalFactor;

    /**
     * Default constructor.
     * @param singleSourceShortestPath
     *            Underlying singleSourceShortestPath.
     * @param nodeSet
     *            A set containing the nodes for which centrality values should
     *            be computed.
     */
    public BetweennessCentrality(
        SingleSourceShortestPath<ShortestPathCostType> singleSourceShortestPath,
        Set<Node> nodeSet )
    {
        super( singleSourceShortestPath, new DoubleAdder(), 0.0, nodeSet );
    }

    @Override
    public void reset()
    {
        super.reset();
        globalFactor = 1.0;
        if ( singleSourceShortestPath.getDirection().equals( Direction.BOTH ) )
        {
            globalFactor = 0.5;
        }
    }

    /**
     * This recursively updates the node dependencies
     * @param node
     *            The start node
     * @param skipFirstNode
     *            If true, the start node is not updated. Useful, since the
     *            first node in any path doesnt need to be updated.
     * @param successors
     * @param counter
     *            Object that can return the number of paths from the initial
     *            start node to any node.
     * @param dependencies
     *            A map used to limit the recursion where possible (dynamic
     *            programming)
     * @return
     */
    protected Double getAndUpdateNodeDependency( Node node,
        boolean skipFirstNode, Map<Node,List<Relationship>> successors,
        PathCounter counter, Map<Node,Double> dependencies )
    {
        Double dependency = dependencies.get( node );
        if ( dependency != null )
        {
            return dependency;
        }
        dependency = (double) 0;
        List<Relationship> succs = successors.get( node );
        if ( succs == null || succs.size() == 0 )
        {
            return (double) 0;
        }
        for ( Relationship relationship : succs )
        {
            Node otherNode = relationship.getOtherNode( node );
            Double otherDependency = getAndUpdateNodeDependency( otherNode,
                false, successors, counter, dependencies );
            dependency += (otherDependency + 1)
                * counter.getNumberOfPathsToNode( node )
                / counter.getNumberOfPathsToNode( otherNode );
        }
        if ( !skipFirstNode )
        {
            dependencies.put( node, dependency );
            // When adding to the final result (and only then), take the global
            // factor into account.
            addCentralityToNode( node, dependency * globalFactor );
        }
        return dependency;
    }

    @Override
    public void processShortestPaths( Node node,
        SingleSourceShortestPath<ShortestPathCostType> singleSourceShortestPath )
    {
        // Extract predecessors and successors
        Map<Node,List<Relationship>> predecessors = singleSourceShortestPath
            .getPredecessors();
        Map<Node,List<Relationship>> successors = Util
            .reversedPredecessors( predecessors );
        PathCounter counter = new Util.PathCounter( predecessors );
        // Recursively update the node dependencies
        getAndUpdateNodeDependency( node, true, successors, counter,
            new HashMap<Node,Double>() );
    }
}
