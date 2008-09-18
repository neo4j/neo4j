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
package org.neo4j.graphalgo.benchmark.graphGeneration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.TraversalPosition;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;

/**
 * This class can be used to generate a subgraph from a graph, represented as a
 * set of nodes and a set of edges. The subgraph always starts out empty, and
 * can be emptied again with the clear() method. It can then be filled with
 * nodes and edges through the various methods supplied. This can of course be
 * used to retrieve the set of nodes and the set of edges from a NeoService.
 * @author Patrik Larsson
 */
public class SubGraph
{
    Set<Node> nodes = new HashSet<Node>();
    Set<Relationship> edges = new HashSet<Relationship>();

    /**
     * Empties this subgraph.
     */
    public void clear()
    {
        nodes = new HashSet<Node>();
        edges = new HashSet<Relationship>();
    }

    /**
     * Adds a tree to this subgraph by doing a breadth first search of a given
     * depth, adding all nodes found and the first edge leading to each node.
     * @param node
     *            The starting node
     * @param searchDepth
     *            The search depth.
     * @param relationshipType
     *            Relation type to traverse.
     * @param direction
     *            Direction in which to traverse edges.
     */
    public void addTreeFromCentralNode( Node node, final int searchDepth,
        RelationshipType relationshipType, Direction direction )
    {
        Traverser traverser = node.traverse( Order.BREADTH_FIRST,
            new StopEvaluator()
            {
                public boolean isStopNode( TraversalPosition currentPos )
                {
                    return currentPos.depth() >= searchDepth;
                }
            }, ReturnableEvaluator.ALL, relationshipType, direction );
        for ( Node node2 : traverser )
        {
            nodes.add( node2 );
            edges.add( traverser.currentPosition().lastRelationshipTraversed() );
        }
    }

    /**
     * Makes a search of a given depth from a given node and adds all found
     * nodes and edges to this subgraph.
     * @param node
     *            The starting node
     * @param searchDepth
     *            The search depth.
     * @param relationshipType
     *            Relation type to traverse.
     * @param direction
     *            Direction in which to traverse edges.
     * @param includeBoundaryEdges
     *            If false, edges between nodes where the maximum depth has been
     *            reached will not be included since the search depth is
     *            considered to have been exhausted at them.
     */
    public void addSubGraphFromCentralNode( Node node, final int searchDepth,
        RelationshipType relationshipType, Direction direction,
        boolean includeBoundaryEdges )
    {
        internalAddSubGraphFromCentralNode( node, searchDepth,
            relationshipType, direction, includeBoundaryEdges,
            new HashMap<Node,Integer>() );
    }

    /**
     * Same as addSubGraphFromCentralNode, but the internal version with some
     * extra data sent along.
     * @param nodeScanDepths
     *            This stores at what depth a certain node was added so we can
     *            ignore it when we reach it with a lower depth.
     */
    protected void internalAddSubGraphFromCentralNode( Node node,
        final int searchDepth, RelationshipType relationshipType,
        Direction direction, boolean includeBoundaryEdges,
        Map<Node,Integer> nodeScanDepths )
    {
        // We stop here if this node has already been scanned and we this time
        // have a "shorter" way to go beyond it.
        Integer previousDepth = nodeScanDepths.get( node );
        if ( previousDepth != null && previousDepth >= searchDepth )
        {
            return;
        }
        nodes.add( node );
        nodeScanDepths.put( node, searchDepth );
        if ( searchDepth == 0 && includeBoundaryEdges )
        {
            for ( Relationship relationship : node.getRelationships(
                relationshipType, direction ) )
            {
                if ( nodes.contains( relationship.getOtherNode( node ) ) )
                {
                    edges.add( relationship );
                }
            }
        }
        if ( searchDepth <= 0 )
        {
            return;
        }
        for ( Relationship relationship : node.getRelationships(
            relationshipType, direction ) )
        {
            edges.add( relationship );
            internalAddSubGraphFromCentralNode( relationship
                .getOtherNode( node ), searchDepth - 1, relationshipType,
                direction, includeBoundaryEdges, nodeScanDepths );
        }
    }

    /**
     * @return the edges
     */
    public Set<Relationship> getEdges()
    {
        return edges;
    }

    /**
     * @return the nodes
     */
    public Set<Node> getNodes()
    {
        return nodes;
    }
}
