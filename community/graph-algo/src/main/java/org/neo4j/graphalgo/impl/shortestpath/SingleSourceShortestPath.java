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

import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * An object implementing this encapsulates an algorithm able to solve the
 * single source shortest path problem. I.e. it can find the shortest path(s)
 * from a given start node to all other nodes in a network.
 * @author Patrik Larsson
 * @param <CostType>
 *            The datatype the edge weights are represented by.
 */
public interface SingleSourceShortestPath<CostType>
{
    /**
     * This resets the calculation if we for some reason would like to redo it.
     */
    public void reset();

    /**
     * This sets the start node. The found paths will start in this node.
     * @param node
     *            The start node.
     */
    public void setStartNode( Node node );

    /**
     * A call to this will run the algorithm to find a single shortest path, if
     * not already done, and return it as an alternating list of
     * Node/Relationship.
     * @return The path as an alternating list of Node/Relationship.
     */
    public List<PropertyContainer> getPath( Node targetNode );

    /**
     * A call to this will run the algorithm, if not already done, and return
     * the found path to the target node if found as a list of nodes.
     * @return The path as a list of nodes.
     */
    public List<Node> getPathAsNodes( Node targetNode );

    /**
     * A call to this will run the algorithm to find a single shortest path, if
     * not already done, and return it as a list of Relationships.
     * @return The path as a list of Relationships.
     */
    public List<Relationship> getPathAsRelationships( Node targetNode );

    /**
     * A call to this will run the algorithm to find all shortest paths, if not
     * already done, and return them as alternating lists of Node/Relationship.
     * @return A list of the paths as alternating lists of Node/Relationship.
     */
    public List<List<PropertyContainer>> getPaths( Node targetNode );

    /**
     * A call to this will run the algorithm to find all shortest paths, if not
     * already done, and return them as lists of nodes.
     * @return A list of the paths as lists of nodes.
     */
    public List<List<Node>> getPathsAsNodes( Node targetNode );

    /**
     * A call to this will run the algorithm to find all shortest paths, if not
     * already done, and return them as lists of relationships.
     * @return A list of the paths as lists of relationships.
     */
    public List<List<Relationship>> getPathsAsRelationships( Node targetNode );

    /**
     * A call to this will run the algorithm, if not already done, and return
     * the cost for the shortest paths between the start node and the target
     * node.
     * @return The total weight of the shortest path(s).
     */
    public CostType getCost( Node targetNode );

    /**
     * @param node
     * @return The nodes previous to the argument node in all found shortest
     *         paths or null if there are no such nodes.
     */
    public List<Node> getPredecessorNodes( Node node );

    /**
     * This can be used to retrieve the entire data structure representing the
     * predecessors for every node.
     * @return
     */
    public Map<Node,List<Relationship>> getPredecessors();

    /**
     * This can be used to retrieve the Direction in which relationships should
     * be in the shortest path(s).
     * @return The direction.
     */
    public Direction getDirection();

    /**
     * This can be used to retrieve the types of relationships that are
     * traversed.
     * @return The relationship type(s).
     */
    public RelationshipType[] getRelationshipTypes();
}
