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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

/**
 * This is a holder for some utility functions regarding paths, such as
 * constructing them from sets of predecessors or counting them. These functions
 * are lifted out here because they can be used by algorithms for too different
 * problems.
 * @author Patrik Larsson
 */
public class Util
{
    /**
     * Constructs a path to a given node, for a given set of predecessors
     * @param node
     *            The start node
     * @param predecessors
     *            The predecessors set
     * @param includeNode
     *            Boolean which determines if the start node should be included
     *            in the paths
     * @param backwards
     *            Boolean, if true the order of the nodes in the paths will be
     *            reversed
     * @return A path as a list of nodes.
     */
    public static List<Node> constructSinglePathToNodeAsNodes( Node node,
        Map<Node,List<Relationship>> predecessors, boolean includeNode,
        boolean backwards )
    {
        List<PropertyContainer> singlePathToNode = constructSinglePathToNode(
            node, predecessors, includeNode, backwards );
        Iterator<PropertyContainer> iterator = singlePathToNode.iterator();
        // When going backwards and not including the node the first element is
        // a relationship. Thus skip it.
        if ( backwards && !includeNode && iterator.hasNext() )
        {
            iterator.next();
        }
        LinkedList<Node> path = new LinkedList<Node>();
        while ( iterator.hasNext() )
        {
            path.addLast( (Node) iterator.next() );
            if ( iterator.hasNext() )
            {
                iterator.next();
            }
        }
        return path;
    }

    /**
     * Constructs a path to a given node, for a given set of predecessors
     * @param node
     *            The start node
     * @param predecessors
     *            The predecessors set
     * @param backwards
     *            Boolean, if true the order of the nodes in the paths will be
     *            reversed
     * @return A path as a list of relationships.
     */
    public static List<Relationship> constructSinglePathToNodeAsRelationships(
        Node node, Map<Node,List<Relationship>> predecessors, boolean backwards )
    {
        List<PropertyContainer> singlePathToNode = constructSinglePathToNode(
            node, predecessors, true, backwards );
        Iterator<PropertyContainer> iterator = singlePathToNode.iterator();
        // Skip the first, it is a node
        if ( iterator.hasNext() )
        {
            iterator.next();
        }
        LinkedList<Relationship> path = new LinkedList<Relationship>();
        while ( iterator.hasNext() )
        {
            path.addLast( (Relationship) iterator.next() );
            if ( iterator.hasNext() )
            {
                iterator.next();
            }
        }
        return path;
    }

    /**
     * Constructs a path to a given node, for a given set of predecessors. The
     * result is a list of alternating Node/Relationship.
     * @param node
     *            The start node
     * @param predecessors
     *            The predecessors set
     * @param includeNode
     *            Boolean which determines if the start node should be included
     *            in the paths
     * @param backwards
     *            Boolean, if true the order of the nodes in the paths will be
     *            reversed
     * @return A path as a list of alternating Node/Relationship.
     */
    public static List<PropertyContainer> constructSinglePathToNode( Node node,
        Map<Node,List<Relationship>> predecessors, boolean includeNode,
        boolean backwards )
    {
        LinkedList<PropertyContainer> path = new LinkedList<PropertyContainer>();
        if ( includeNode )
        {
            if ( backwards )
            {
                path.addLast( node );
            }
            else
            {
                path.addFirst( node );
            }
        }
        Node currentNode = node;
        List<Relationship> currentPreds = predecessors.get( currentNode );
        // Traverse predecessors until we have added a node without predecessors
        while ( currentPreds != null && currentPreds.size() != 0 )
        {
            // Get next node
            Relationship currentRelationship = currentPreds.get( 0 );
            currentNode = currentRelationship.getOtherNode( currentNode );
            // Add current
            if ( backwards )
            {
                path.addLast( currentRelationship );
                path.addLast( currentNode );
            }
            else
            {
                path.addFirst( currentRelationship );
                path.addFirst( currentNode );
            }
            // Continue with the next node
            currentPreds = predecessors.get( currentNode );
        }
        return path;
    }

    /**
     * Constructs all paths to a given node, for a given set of predecessors
     * @param node
     *            The start node
     * @param predecessors
     *            The predecessors set
     * @param includeNode
     *            Boolean which determines if the start node should be included
     *            in the paths
     * @param backwards
     *            Boolean, if true the order of the nodes in the paths will be
     *            reversed
     * @return
     */
    public static List<List<Node>> constructAllPathsToNodeAsNodes( Node node,
        Map<Node,List<Relationship>> predecessors, boolean includeNode,
        boolean backwards )
    {
        return new LinkedList<List<Node>>(
            constructAllPathsToNodeAsNodeLinkedLists( node, predecessors,
                includeNode, backwards ) );
    }

    /**
     * Same as constructAllPathsToNodeAsNodes, but different return type
     */
    protected static List<LinkedList<Node>> constructAllPathsToNodeAsNodeLinkedLists(
        Node node, Map<Node,List<Relationship>> predecessors,
        boolean includeNode, boolean backwards )
    {
        List<LinkedList<Node>> paths = new LinkedList<LinkedList<Node>>();
        List<Relationship> current = predecessors.get( node );
        // First build all paths to this node's predecessors
        if ( current != null )
        {
            for ( Relationship r : current )
            {
                Node n = r.getOtherNode( node );
                paths.addAll( constructAllPathsToNodeAsNodeLinkedLists( n,
                    predecessors, true, backwards ) );
            }
        }
        // If no paths exists to this node, just create an empty one (which will
        // have this node added to it)
        if ( paths.isEmpty() )
        {
            paths.add( new LinkedList<Node>() );
        }
        // Then add this node to all those paths
        if ( includeNode )
        {
            for ( LinkedList<Node> path : paths )
            {
                if ( backwards )
                {
                    path.addFirst( node );
                }
                else
                {
                    path.addLast( node );
                }
            }
        }
        return paths;
    }

    /**
     * Constructs all paths to a given node, for a given set of predecessors
     * @param node
     *            The start node
     * @param predecessors
     *            The predecessors set
     * @param includeNode
     *            Boolean which determines if the start node should be included
     *            in the paths
     * @param backwards
     *            Boolean, if true the order of the nodes in the paths will be
     *            reversed
     * @return List of lists of alternating Node/Relationship.
     */
    public static List<List<PropertyContainer>> constructAllPathsToNode(
        Node node, Map<Node,List<Relationship>> predecessors,
        boolean includeNode, boolean backwards )
    {
        return new LinkedList<List<PropertyContainer>>(
            constructAllPathsToNodeAsLinkedLists( node, predecessors,
                includeNode, backwards ) );
    }

    /**
     * Same as constructAllPathsToNode, but different return type
     */
    protected static List<LinkedList<PropertyContainer>> constructAllPathsToNodeAsLinkedLists(
        Node node, Map<Node,List<Relationship>> predecessors,
        boolean includeNode, boolean backwards )
    {
        List<LinkedList<PropertyContainer>> paths = new LinkedList<LinkedList<PropertyContainer>>();
        List<Relationship> current = predecessors.get( node );
        // First build all paths to this node's predecessors
        if ( current != null )
        {
            for ( Relationship r : current )
            {
                Node n = r.getOtherNode( node );
                List<LinkedList<PropertyContainer>> newPaths = constructAllPathsToNodeAsLinkedLists(
                    n, predecessors, true, backwards );
                paths.addAll( newPaths );
                // Add the relationship
                for ( LinkedList<PropertyContainer> path : newPaths )
                {
                    if ( backwards )
                    {
                        path.addFirst( r );
                    }
                    else
                    {
                        path.addLast( r );
                    }
                }
            }
        }
        // If no paths exists to this node, just create an empty one (which will
        // have this node added to it)
        if ( paths.isEmpty() )
        {
            paths.add( new LinkedList<PropertyContainer>() );
        }
        // Then add this node to all those paths
        if ( includeNode )
        {
            for ( LinkedList<PropertyContainer> path : paths )
            {
                if ( backwards )
                {
                    path.addFirst( node );
                }
                else
                {
                    path.addLast( node );
                }
            }
        }
        return paths;
    }

    /**
     * Constructs all paths to a given node, for a given set of predecessors.
     * @param node
     *            The start node
     * @param predecessors
     *            The predecessors set
     * @param backwards
     *            Boolean, if true the order of the nodes in the paths will be
     *            reversed
     * @return List of lists of relationships.
     */
    public static List<List<Relationship>> constructAllPathsToNodeAsRelationships(
        Node node, Map<Node,List<Relationship>> predecessors, boolean backwards )
    {
        return new LinkedList<List<Relationship>>(
            constructAllPathsToNodeAsRelationshipLinkedLists( node,
                predecessors, backwards ) );
    }

    /**
     * Same as constructAllPathsToNodeAsRelationships, but different return type
     */
    protected static List<LinkedList<Relationship>> constructAllPathsToNodeAsRelationshipLinkedLists(
        Node node, Map<Node,List<Relationship>> predecessors, boolean backwards )
    {
        List<LinkedList<Relationship>> paths = new LinkedList<LinkedList<Relationship>>();
        List<Relationship> current = predecessors.get( node );
        // First build all paths to this node's predecessors
        if ( current != null )
        {
            for ( Relationship r : current )
            {
                Node n = r.getOtherNode( node );
                List<LinkedList<Relationship>> newPaths = constructAllPathsToNodeAsRelationshipLinkedLists(
                    n, predecessors, backwards );
                paths.addAll( newPaths );
                // Add the relationship
                for ( LinkedList<Relationship> path : newPaths )
                {
                    if ( backwards )
                    {
                        path.addFirst( r );
                    }
                    else
                    {
                        path.addLast( r );
                    }
                }
            }
        }
        // If no paths exists to this node, just create an empty one
        if ( paths.isEmpty() )
        {
            paths.add( new LinkedList<Relationship>() );
        }
        return paths;
    }

    /**
     * This can be used for counting the number of paths from the start node
     * (implicit from the predecessors) and some target nodes.
     */
    public static class PathCounter
    {
        Map<Node,List<Relationship>> predecessors;
        Map<Node,Integer> pathCounts = new HashMap<Node,Integer>();

        public PathCounter( Map<Node,List<Relationship>> predecessors )
        {
            super();
            this.predecessors = predecessors;
        }

        public int getNumberOfPathsToNode( Node node )
        {
            Integer i = pathCounts.get( node );
            if ( i != null )
            {
                return i;
            }
            List<Relationship> preds = predecessors.get( node );
            if ( preds == null || preds.size() == 0 )
            {
                return 1;
            }
            int result = 0;
            for ( Relationship relationship : preds )
            {
                result += getNumberOfPathsToNode( relationship
                    .getOtherNode( node ) );
            }
            pathCounts.put( node, result );
            return result;
        }
    }

    /**
     * This can be used to generate the inverse of a structure with
     * predecessors, i.e. the successors.
     * @param predecessors
     * @return
     */
    public static Map<Node,List<Relationship>> reversedPredecessors(
        Map<Node,List<Relationship>> predecessors )
    {
        Map<Node,List<Relationship>> result = new HashMap<Node,List<Relationship>>();
        Set<Node> keys = predecessors.keySet();
        for ( Node node : keys )
        {
            List<Relationship> preds = predecessors.get( node );
            for ( Relationship relationship : preds )
            {
                Node otherNode = relationship.getOtherNode( node );
                // We add node as a predecessor to otherNode, instead of the
                // other way around
                List<Relationship> otherPreds = result.get( otherNode );
                if ( otherPreds == null )
                {
                    otherPreds = new LinkedList<Relationship>();
                    result.put( otherNode, otherPreds );
                }
                otherPreds.add( relationship );
            }
        }
        return result;
    }
}
