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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * This generates all simple paths, so calculations may be done on them later.
 */
public class AllSimplePaths
{
    private final RelationshipType relationshipType;
    private final Direction direction;
    private final Node node1;
    private final Node node2;

    public AllSimplePaths( Node node1, Node node2, RelationshipType type,
            Direction direction )
    {
        if ( node1 == null || node2 == null )
        {
            throw new IllegalArgumentException( "Null node" );
        }
        this.node1 = node1;
        this.node2 = node2;
        this.relationshipType = type;
        this.direction = direction;
    }

    private class Path
    {
        private Node head;
        private Relationship rel; // relationship from tail to head
        private Path tail;

        public Path( Node head, Relationship rel, Path tail )
        {
            super();
            this.head = head;
            this.rel = rel;
            this.tail = tail;
        }
    }

    private HashMap<Node, LinkedList<Path>> pathsFromOneDirection =
            new HashMap<Node, LinkedList<Path>>();
    private List<List<PropertyContainer>> foundPaths =
            new ArrayList<List<PropertyContainer>>();

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
    // note: currentPath contains currentNode
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
        if ( currentDepth >= 0 )
        {
            for ( Relationship relationship :
                    currentNode.getRelationships( relationshipType, direction ) )
            {
                Node targetNode = relationship.getOtherNode( currentNode );
                traverser1( targetNode, currentDepth - 1, new Path( targetNode,
                        relationship, currentPath ) );
            }
        }
    }

    // Try to find nodes the other traversion reached
    // currentPathPredecessors is (node,relationship,...,node,relationship)
    protected void traverser2( Node currentNode, int currentDepth,
            LinkedList<PropertyContainer> currentPathPredecessors )
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
                if ( simplePath && currentDepth > 0 )
                {
                    // for all nodes we will continue to...
                    for ( Relationship relationship : currentNode.getRelationships(
                            relationshipType, direction ) )
                    {
                        Node targetNode = relationship.getOtherNode( currentNode );
                        // If we will find this exact path somewhere later, skip
                        // it
                        if ( path.tail != null
                             && path.tail.head.equals( targetNode ) )
                        {
                            simplePath = false;
                            break;
                        }
                    }
                }
                if ( simplePath )
                {
                    // A path is found!
                    LinkedList<PropertyContainer> newFoundPath = new LinkedList<PropertyContainer>(
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
        if ( currentDepth >= 0 )
        {
            for ( Relationship relationship : currentNode.getRelationships(
                    relationshipType, direction ) )
            {
                Node targetNode = relationship.getOtherNode( currentNode );
                LinkedList<PropertyContainer> newPath = new LinkedList<PropertyContainer>(
                        currentPathPredecessors );
                newPath.add( currentNode );
                newPath.add( relationship );
                traverser2( targetNode, currentDepth - 1, newPath );
            }
        }
    }

    protected void reset()
    {
        pathsFromOneDirection = new HashMap<Node, LinkedList<Path>>();
        foundPaths = new ArrayList<List<PropertyContainer>>();
    }

    public List<List<PropertyContainer>> getPaths( int totalDepth )
    {
        reset();
        
        // TODO The traverser only works if we go one step further
        // than expected
        totalDepth++;
        // System.out.print( "Trav1... " );
        traverser1( node2, totalDepth / 2, new Path( node2, null, null ) );
        // System.out.print( "trav2... " );
        traverser2( node1, totalDepth / 2 + totalDepth % 2,
                new LinkedList<PropertyContainer>() );
        // System.out.println( "end" );
        return foundPaths;
    }

    /**
     * Returns the found paths as lists of nodes.
     * @return
     */
    public List<List<Node>> getPathsAsNodes( int totalDepth )
    {
        LinkedList<List<Node>> paths = new LinkedList<List<Node>>();
        List<List<PropertyContainer>> complexPaths = getPaths( totalDepth );
        for ( List<PropertyContainer> complexPath : complexPaths )
        {
            List<Node> path = new LinkedList<Node>();
            int flipflop = 0;
            for ( PropertyContainer object : complexPath )
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
