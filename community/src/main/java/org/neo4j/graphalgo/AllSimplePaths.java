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
package org.neo4j.graphalgo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;

/**
 * This can be used to find all simple paths of a maximal length between two
 * nodes. In a simple path each node can occur only once, so there will be no
 * cycles in these paths.
 * @complexity This algorithms runs in O(p) time where p is the number of paths
 *             to find. This is in the worst case O(2 ^ (m / 2)).
 * @author Patrik Larsson
 */
public class AllSimplePaths
{
    protected Node node1;
    protected Node node2;
    protected int maximumTotalDepth;
    protected Direction relationshipDirection;
    protected RelationshipType[] relationshipTypes;
    protected boolean doneCalculation = false;

    public AllSimplePaths( Node node1, Node node2, int maximumTotalDepth,
        Direction relationshipDirection, RelationshipType... relationshipTypes )
    {
        super();
        this.node1 = node1;
        this.node2 = node2;
        this.maximumTotalDepth = maximumTotalDepth;
        this.relationshipDirection = relationshipDirection;
        this.relationshipTypes = relationshipTypes;
    }

    protected Direction getOppositeDirection()
    {
        if ( relationshipDirection.equals( Direction.INCOMING ) )
        {
            return Direction.OUTGOING;
        }
        if ( relationshipDirection.equals( Direction.OUTGOING ) )
        {
            return Direction.INCOMING;
        }
        return relationshipDirection;
    }

    /**
     * This is used to track paths from one direction (the first traverser).
     */
    private class Path
    {
        public Node head;
        public Relationship rel; // relationship from tail to head
        public Path tail;

        public Path( Node head, Relationship rel, Path tail )
        {
            super();
            this.head = head;
            this.rel = rel;
            this.tail = tail;
        }
    }

    // All the paths found by the first traverser is stored in this
    HashMap<Node,LinkedList<Path>> pathsFromOneDirection = new HashMap<Node,LinkedList<Path>>();
    // All the final paths are stored in this
    // Note: The resulting paths are internally stored as alternating lists of
    // Node,Relationship,Node,Relationship,...,Node.
    List<List<?>> foundPaths = new ArrayList<List<?>>();

    // The first traverser uses this to store found paths for every node
    protected void addPathToNode( Node node, Path path )
    {
        LinkedList<Path> paths = pathsFromOneDirection.get( node );
        if ( paths == null )
        {
            paths = new LinkedList<Path>();
            pathsFromOneDirection.put( node, paths );
        }
        paths.add( path );
    }

    // Build all paths to all the nodes reachable
    // Note: currentPath contains currentNode
    protected void traverser1( Node currentNode, int currentDepth,
        Path currentPath )
    {
        // System.out.print( "Checking node "
        // + currentNode.getProperty( "graphBuilderId" ) + " from " );
        for ( Path p = currentPath.tail; p != null; p = p.tail )
        {
            // System.out.print( p.head.getProperty( "graphBuilderId" ) + " " );
            // We have a cycle
            if ( p.head.equals( currentNode ) )
            {
                // System.out.println( "Cycle (trav1)" );
                return;
            }
        }
        // System.out.println();
        addPathToNode( currentNode, currentPath );
        if ( currentDepth > 0 )
        {
            for ( RelationshipType relationshipType : relationshipTypes )
            {
                for ( Relationship relationship : currentNode.getRelationships(
                    relationshipType, getOppositeDirection() ) )
                {
                    Node targetNode = relationship.getOtherNode( currentNode );
                    traverser1( targetNode, currentDepth - 1, new Path(
                        targetNode, relationship, currentPath ) );
                }
            }
        }
    }

    // Try to find nodes the other traversion reached
    // currentPathPredecessors is (node,relationship,...,node,relationship)
    protected void traverser2( Node currentNode, int currentDepth,
        LinkedList<Object> currentPathPredecessors )
    {
        // Cycle?
        if ( currentPathPredecessors.contains( currentNode ) )
        {
            return;
        }
        LinkedList<Path> paths = pathsFromOneDirection.get( currentNode );
        if ( paths != null )
        {
            // For all paths that reached this node from the other direction...
            for ( Path path : paths )
            {
                boolean simplePath = true;
                // For all nodes in that path...
                for ( Path p = path; p != null; p = p.tail )
                {
                    // We must not have them in our current path
                    if ( currentPathPredecessors.contains( p.head ) )
                    {
                        simplePath = false;
                        // System.out.println( "Cycle (path)" );
                        break;
                    }
                }
                if ( simplePath && currentDepth > 0 && path.tail != null )
                {
                    // We will continue to the previous node in the path later
                    break;
                }
                if ( simplePath )
                {
                    // A path is found!
                    LinkedList<Object> newFoundPath = new LinkedList<Object>(
                        currentPathPredecessors );
                    // For all nodes in that path...
                    for ( Path p = path; p != null; p = p.tail )
                    {
                        newFoundPath.add( p.head );
                        if ( p.rel != null )
                        {
                            newFoundPath.add( p.rel );
                        }
                    }
                    foundPaths.add( newFoundPath );
                }
            }
        }
        if ( currentDepth > 0 )
        {
            for ( RelationshipType relationshipType : relationshipTypes )
            {
                for ( Relationship relationship : currentNode.getRelationships(
                    relationshipType, relationshipDirection ) )
                {
                    Node targetNode = relationship.getOtherNode( currentNode );
                    LinkedList<Object> newPath = new LinkedList<Object>(
                        currentPathPredecessors );
                    newPath.add( currentNode );
                    newPath.add( relationship );
                    traverser2( targetNode, currentDepth - 1, newPath );
                }
            }
        }
    }

    /**
     * This resets the calculation if we for some reason would like to redo it.
     */
    protected void reset()
    {
        pathsFromOneDirection = new HashMap<Node,LinkedList<Path>>();
        foundPaths = new ArrayList<List<?>>();
        doneCalculation = false;
    }

    /**
     * Internal calculate method that will do the calculation. This can however
     * be called externally to manually trigger the calculation.
     */
    public void calculate()
    {
        // Don't do it more than once
        if ( doneCalculation )
        {
            return;
        }
        doneCalculation = true;
        // System.out.print( "Trav1... " );
        traverser1( node2, maximumTotalDepth / 2, new Path( node2, null, null ) );
        // System.out.print( "trav2... " );
        traverser2( node1, maximumTotalDepth / 2 + maximumTotalDepth % 2,
            new LinkedList<Object>() );
    }

    /**
     * Returns the found paths as alternating lists of
     * Node,Relationship,Node,Relationship,...,Node.
     * @return
     */
    public List<List<?>> getPaths()
    {
        calculate();
        return foundPaths;
    }

    /**
     * Returns the found paths as lists of nodes.
     * @return
     */
    public List<List<Node>> getPathsAsNodes()
    {
        LinkedList<List<Node>> paths = new LinkedList<List<Node>>();
        List<List<?>> complexPaths = getPaths();
        for ( List<?> complexPath : complexPaths )
        {
            List<Node> path = new LinkedList<Node>();
            int flipflop = 0;
            for ( Object object : complexPath )
            {
                if ( flipflop == 0 )
                {
                    path.add( (Node) object );
                }
                flipflop = 1 - flipflop;
            }
            paths.add( path );
        }
        return paths;
    }
}
