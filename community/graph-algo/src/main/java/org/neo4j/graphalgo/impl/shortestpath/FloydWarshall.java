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
package org.neo4j.graphalgo.impl.shortestpath;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphalgo.CostAccumulator;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * This provides an implementation of the Floyd Warshall algorithm solving the
 * all pair shortest path problem.
 * @complexity The {@link CostEvaluator} is called once for every relationship.
 *             The {@link CostAccumulator} and cost comparator are both called
 *             n^3 times. Assuming they run in constant time, the time
 *             complexity for this algorithm is O(n^3).
 * @author Patrik Larsson
 * @param <CostType>
 *            The datatype the edge weights are represented by.
 */
public class FloydWarshall<CostType>
{
    protected CostType startCost; // starting cost for all nodes
    protected CostType infinitelyBad; // starting value for calculation
    protected Direction relationDirection;
    protected CostEvaluator<CostType> costEvaluator = null;
    protected CostAccumulator<CostType> costAccumulator = null;
    protected Comparator<CostType> costComparator = null;
    protected Set<Node> nodeSet;
    protected Set<Relationship> relationshipSet;
    CostType[][] costMatrix;
    Integer[][] predecessors;
    Map<Node,Integer> nodeIndexes; // node ->index
    Node[] IndexedNodes; // index -> node
    protected boolean doneCalculation = false;

    /**
     * @param startCost
     *            The cost for just starting (or ending) a path in a node.
     * @param infinitelyBad
     *            A cost worse than all others. This is used to initialize the
     *            distance matrix.
     * @param costRelationType
     *            The relationship type to traverse.
     * @param relationDirection
     *            The direction in which the paths should follow the
     *            relationships.
     * @param costEvaluator
     * @see {@link CostEvaluator}
     * @param costAccumulator
     * @see {@link CostAccumulator}
     * @param costComparator
     * @see {@link CostAccumulator} or {@link CostEvaluator}
     * @param nodeSet
     *            The set of nodes the calculation should be run on.
     * @param relationshipSet
     *            The set of relationships that should be processed.
     */
    public FloydWarshall( CostType startCost, CostType infinitelyBad,
        Direction relationDirection, CostEvaluator<CostType> costEvaluator,
        CostAccumulator<CostType> costAccumulator,
        Comparator<CostType> costComparator, Set<Node> nodeSet,
        Set<Relationship> relationshipSet )
    {
        super();
        this.startCost = startCost;
        this.infinitelyBad = infinitelyBad;
        this.relationDirection = relationDirection;
        this.costEvaluator = costEvaluator;
        this.costAccumulator = costAccumulator;
        this.costComparator = costComparator;
        this.nodeSet = nodeSet;
        this.relationshipSet = relationshipSet;
    }

    /**
     * This resets the calculation if we for some reason would like to redo it.
     */
    public void reset()
    {
        doneCalculation = false;
    }

    /**
     * Internal calculate method that will do the calculation. This can however
     * be called externally to manually trigger the calculation.
     */
    @SuppressWarnings( "unchecked" )
    public void calculate()
    {
        // Don't do it more than once
        if ( doneCalculation )
        {
            return;
        }
        doneCalculation = true;
        // Build initial matrix
        int n = nodeSet.size();
        costMatrix = (CostType[][]) new Object[n][n];
        predecessors = new Integer[n][n];
        IndexedNodes = new Node[n];
        nodeIndexes = new HashMap<Node,Integer>();
        for ( int i = 0; i < n; ++i )
        {
            for ( int j = 0; j < n; ++j )
            {
                costMatrix[i][j] = infinitelyBad;
            }
            costMatrix[i][i] = startCost;
        }
        int nodeIndex = 0;
        for ( Node node : nodeSet )
        {
            nodeIndexes.put( node, nodeIndex );
            IndexedNodes[nodeIndex] = node;
            ++nodeIndex;
        }
        // Put the relationships in there
        for ( Relationship relationship : relationshipSet )
        {
            Integer i1 = nodeIndexes.get( relationship.getStartNode() );
            Integer i2 = nodeIndexes.get( relationship.getEndNode() );
            if ( i1 == null || i2 == null )
            {
                // TODO: what to do here? pretend nothing happened? cast
                // exception?
                continue;
            }
            if ( relationDirection.equals( Direction.BOTH )
                || relationDirection.equals( Direction.OUTGOING ) )
            {
                costMatrix[i1][i2] = costEvaluator
.getCost( relationship,
                        Direction.OUTGOING );
                predecessors[i1][i2] = i1;
            }
            if ( relationDirection.equals( Direction.BOTH )
                || relationDirection.equals( Direction.INCOMING ) )
            {
                costMatrix[i2][i1] = costEvaluator.getCost( relationship,
                        Direction.INCOMING );
                predecessors[i2][i1] = i2;
            }
        }
        // Do it!
        for ( int v = 0; v < n; ++v )
        {
            for ( int i = 0; i < n; ++i )
            {
                for ( int j = 0; j < n; ++j )
                {
                    CostType alternative = costAccumulator.addCosts(
                        costMatrix[i][v], costMatrix[v][j] );
                    if ( costComparator.compare( costMatrix[i][j], alternative ) > 0 )
                    {
                        costMatrix[i][j] = alternative;
                        predecessors[i][j] = predecessors[v][j];
                    }
                }
            }
        }
        // TODO: detect negative cycles?
    }

    /**
     * This returns the cost for the shortest path between two nodes.
     * @param node1
     *            The start node.
     * @param node2
     *            The end node.
     * @return The cost for the shortest path.
     */
    public CostType getCost( Node node1, Node node2 )
    {
        calculate();
        return costMatrix[nodeIndexes.get( node1 )][nodeIndexes.get( node2 )];
    }

    /**
     * This returns the shortest path between two nodes as list of nodes.
     * @param startNode
     *            The start node.
     * @param targetNode
     *            The end node.
     * @return The shortest path as a list of nodes.
     */
    public List<Node> getPath( Node startNode, Node targetNode )
    {
        calculate();
        LinkedList<Node> path = new LinkedList<Node>();
        int index = nodeIndexes.get( targetNode );
        int startIndex = nodeIndexes.get( startNode );
        Node n = targetNode;
        while ( !n.equals( startNode ) )
        {
            path.addFirst( n );
            index = predecessors[startIndex][index];
            n = IndexedNodes[index];
        }
        path.addFirst( n );
        return path;
    }
}
