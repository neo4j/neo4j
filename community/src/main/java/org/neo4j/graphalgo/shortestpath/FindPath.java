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
package org.neo4j.graphalgo.shortestpath;

import java.util.List;

import org.neo4j.graphalgo.shortestpath.std.IntegerAdder;
import org.neo4j.graphalgo.shortestpath.std.IntegerComparator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * FindPath class. This class can be used to perform shortest path computations
 * between two nodes on an unweighted network. Currently just wraps two Dijkstras
 * from sart and end node, trying to intersect in the middle.
 * {@link Dijkstra}.
 * @author Patrik Larsson
 */
public class FindPath implements SingleSourceSingleSinkShortestPath<Integer>
{
    Dijkstra<Integer> dijkstra;

    /**
     * @return
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#calculate()
     */
    public boolean calculate()
    {
        return dijkstra.calculate();
    }

    /**
     * @return
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#calculateMultiple()
     */
    public boolean calculateMultiple()
    {
        return dijkstra.calculateMultiple();
    }

    /**
     * @return
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#getCost()
     */
    public Integer getCost()
    {
        return dijkstra.getCost();
    }

    /**
     * @return
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#getDirection()
     */
    public Direction getDirection()
    {
        return dijkstra.getDirection();
    }

    /**
     * @return
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#getPath()
     */
    public List<PropertyContainer> getPath()
    {
        return dijkstra.getPath();
    }

    /**
     * @return
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#getPathAsNodes()
     */
    public List<Node> getPathAsNodes()
    {
        return dijkstra.getPathAsNodes();
    }

    /**
     * @return
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#getPathAsRelationships()
     */
    public List<Relationship> getPathAsRelationships()
    {
        return dijkstra.getPathAsRelationships();
    }

    /**
     * @return
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#getPaths()
     */
    public List<List<PropertyContainer>> getPaths()
    {
        return dijkstra.getPaths();
    }

    /**
     * @return
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#getPathsAsNodes()
     */
    public List<List<Node>> getPathsAsNodes()
    {
        return dijkstra.getPathsAsNodes();
    }

    /**
     * @return
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#getPathsAsRelationships()
     */
    public List<List<Relationship>> getPathsAsRelationships()
    {
        return dijkstra.getPathsAsRelationships();
    }

    /**
     * @return
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#getRelationshipTypes()
     */
    public RelationshipType[] getRelationshipTypes()
    {
        return dijkstra.getRelationshipTypes();
    }

    /**
     * @param maxNodesToTraverse
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#limitMaxNodesToTraverse(long)
     */
    public void limitMaxNodesToTraverse( long maxNodesToTraverse )
    {
        dijkstra.limitMaxNodesToTraverse( maxNodesToTraverse );
    }

    /**
     * @param maxRelationShipsToTraverse
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#limitMaxRelationShipsToTraverse(long)
     */
    public void limitMaxRelationShipsToTraverse( long maxRelationShipsToTraverse )
    {
        dijkstra.limitMaxRelationShipsToTraverse( maxRelationShipsToTraverse );
    }

    /**
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#reset()
     */
    public void reset()
    {
        dijkstra.reset();
    }

    /**
     * @param endNode
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#setEndNode(org.neo4j.api.core.Node)
     */
    public void setEndNode( Node endNode )
    {
        dijkstra.setEndNode( endNode );
    }

    /**
     * @param startNode
     * @see org.neo4j.graphalgo.shortestpath.Dijkstra#setStartNode(org.neo4j.api.core.Node)
     */
    public void setStartNode( Node startNode )
    {
        dijkstra.setStartNode( startNode );
    }
    /**
     * This is an algo that will initiate a Dijkstra from start- and end node
     * with relationship cost of 1 per path step along the costRelationshipTypes
     * @param startNode the node to start at
     * @param endNode the node to find a path to
     * @param relationDirection the direction to traverse all cost relationships in
     * @param costRelationTypes the types of relationships that are going to be on the path
     */
    public FindPath( Node startNode, Node endNode, Direction relationDirection,
        RelationshipType... costRelationTypes )
    {
        dijkstra = new Dijkstra<Integer>( 0, startNode, endNode,
            new CostEvaluator<Integer>()
            {
                public Integer getCost( Relationship relationship,
                    boolean backwards )
                {
                    return 1;
                }
            }, new IntegerAdder(), new IntegerComparator(), relationDirection,
            costRelationTypes );
    }  
    /**
     * A depth-limited variant of the double-starting Dijkstra. If one of the paths is costing more (longer than)
     * maxCost, the Dijkstra there will stop.
     * Potentially, if the shortest path between 2 nodes is length 12, at maxCost 4 it would not be found since the two segments
     * at max depth 4 would not meet.
     * For this, at least maxCost = 6 has to be set in order to find paths with length 12.
     * @param startNode the start node
     * @param endNode the end node
     * @param maxCost the maximum length of the path before giving up
     * @param relationDirection the direction to traverse all cost relationships in
     * @param costRelationTypes the types of relationships that are going to be on the path
     */
    public FindPath( Node startNode, Node endNode, final int maxCost, Direction relationDirection,
            RelationshipType... costRelationTypes )
        {
        	this(startNode, endNode, relationDirection, costRelationTypes);
            MaxCostEvaluator<Integer> maxCostComparator = new MaxCostEvaluator<Integer>() {

    			public boolean maxCostExceeded(Integer currentCost) {
    				return currentCost > maxCost;
    			}
    		};
    		dijkstra.limitMaxCostToTraverse(maxCostComparator);
        }
}
