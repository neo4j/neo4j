/*
 * Copyright 2010 Network Engine for Objects in Lund AB [neotechnology.com]
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class FindSingleShortestPath
{
//    private final Map<Node,Relationship> firstSet = 
//        new HashMap<Node,Relationship>();
//    private final Map<Node,Relationship> secondSet = 
//        new HashMap<Node,Relationship>();
    private OneDirection firstDirection;
    private OneDirection secondDirection;
    private final int maxDepth;
    private final Node startNode;
    private final Node endNode;
    private final Object[] relTypesAndDirections;
    private boolean doneCalculation;
    Node matchNode;

    public void reset()
    {
        initializeDirectionData();
        doneCalculation = false;
        matchNode = null;
    }
    
    private void initializeDirectionData()
    {
        firstDirection = new OneDirection( startNode, maxDepth / 2 );
        secondDirection = new OneDirection( endNode,
                firstDirection.depth + ( maxDepth % 2 ) );
    }

    private static final Relationship NULL_REL = new FakeRelImpl();
    
    private class OneDirection
    {
        private List<Node> nodeList = new ArrayList<Node>();
        private int depth;
        private List<Node> nextNodeList = new ArrayList<Node>();
        private Iterator<Node> iterator;
        private int currentDepth;
        private Map<Node, Relationship> path =
            new HashMap<Node, Relationship>();
        
        OneDirection( Node node, int depth )
        {
            this.depth = depth;
            this.nextNodeList.add( node );
            this.path.put( node, NULL_REL );
            switchToNext();
        }

        void switchToNext()
        {
            this.nodeList = this.nextNodeList;
            this.iterator = this.nodeList.iterator();
            this.nextNodeList = new ArrayList<Node>();
        }
        
        void checkNextDepth()
        {
            if ( !iterator.hasNext() && currentDepth + 1 <= depth )
            {
                currentDepth++;
                switchToNext();
            }
        }
    }

    private boolean calculate()
    {
        if ( doneCalculation )
        {
            return true;
        }
        if ( startNode.equals( endNode ) )
        {
            matchNode = startNode;
            doneCalculation = true;
            return true;
        }
        
        boolean hasRecalculatedDepth = false;
        initializeDirectionData();
        while ( firstDirection.iterator.hasNext() ||
                secondDirection.iterator.hasNext() )
        {
            if ( tryMatch( firstDirection, secondDirection ) )
            {
                return true;
            }
            if ( tryMatch( secondDirection, firstDirection ) )
            {
                return true;
            }
            
            firstDirection.checkNextDepth();
            secondDirection.checkNextDepth();
            
            if ( !hasRecalculatedDepth && firstDirection.currentDepth == 1 &&
                    this.maxDepth % 2 == 1 )
            {
                // The one with the least relationships gets the greater depth
                boolean firstHasMore = firstDirection.nodeList.size() >
                        secondDirection.nodeList.size();
                if ( firstHasMore ==
                    firstDirection.depth > secondDirection.depth )
                {
                    // Switch 'em
                    int tempDepth = firstDirection.depth;
                    firstDirection.depth = secondDirection.depth;
                    secondDirection.depth = tempDepth;
                }
                hasRecalculatedDepth = true;
            }
        }
        doneCalculation = true;
        return false;
    }
    
    private boolean tryMatch( OneDirection direction,
            OneDirection otherDirection )
    {
        if ( !direction.iterator.hasNext() )
        {
            return false;
        }
        
        Node node = direction.iterator.next();
        if ( otherDirection.path.containsKey( node ) )
        {
            matchNode = node;
            doneCalculation = true;
            return true;
        }
        if ( direction.currentDepth + 1 <= direction.depth )  
        {
            for ( int i = 0; i < relTypesAndDirections.length / 2; i++ )
            {
                RelationshipType type = 
                    (RelationshipType) relTypesAndDirections[i*2];
                Direction dir = (Direction) relTypesAndDirections[i*2+1];
                for ( Relationship rel : node.getRelationships( 
                    type,dir ) )
                {
                    Node otherNode = rel.getOtherNode( node );
                    Relationship oldRel = direction.path.put( otherNode, rel );
                    if ( oldRel == null )
                    {
                        direction.nextNodeList.add( otherNode );
                    }
                    else
                    {
                        direction.path.put( otherNode, oldRel );
                    }
                }
            }
        }
        return false;
    }

    /**
     * @param startNode
     *            The node in which the path should start.
     * @param endNode
     *            The node in which the path should end.
     * @param relationshipType
     *            The type of relationship to follow.
     * @param maxDepth
     *            A maximum search length.
     */
    public FindSingleShortestPath( Node startNode, Node endNode,
        RelationshipType relationshipType, int maxDepth )
    {
        super();
        reset();
        this.startNode = startNode;
        this.endNode = endNode;
        this.relTypesAndDirections = new Object[2];
        this.relTypesAndDirections[0] = relationshipType;
        this.relTypesAndDirections[1] = Direction.BOTH;
        this.maxDepth = maxDepth;
    }

    public FindSingleShortestPath( Node startNode, Node endNode,
        RelationshipType[] relationshipTypes, Direction[] directions, int maxDepth )
    {
        super();
        reset();
        this.startNode = startNode;
        this.endNode = endNode;
        relTypesAndDirections = new Object[ relationshipTypes.length * 2 ];
        for ( int i = 0; i < relationshipTypes.length; i++ )
        {
            relTypesAndDirections[i*2] = relationshipTypes[i];
            relTypesAndDirections[i*2 + 1] = directions[i];
        }
        this.maxDepth = maxDepth;
    }
    
    /**
     * @return One of the shortest paths found or null.
     */
    public List<PropertyContainer> getPath()
    {
        calculate();
        if ( matchNode == null )
        {
            return null;
        }
        return constructPath( true, true );
    }

    private List<PropertyContainer> constructPath( 
            boolean includeNodes, boolean includeRels )
    {
        LinkedList<PropertyContainer> path = new LinkedList<PropertyContainer>();
        Relationship rel = firstDirection.path.get( matchNode );
        Node currentNode = matchNode;
        while ( rel != NULL_REL && rel != null )
        {
            if ( includeRels )
            {    
                path.addFirst( rel );
            }
            currentNode = rel.getOtherNode( currentNode );
            if ( includeNodes && !currentNode.equals( matchNode ) && 
                    !currentNode.equals( startNode ) )
            {
                path.addFirst( currentNode );
            }
            rel = firstDirection.path.get( currentNode );
        }
        if ( includeNodes )
        {
            path.addFirst( startNode );
            if ( !matchNode.equals( startNode ) )
            {
                path.addLast( matchNode );
            }
        }
        rel = secondDirection.path.get( matchNode );
        currentNode = matchNode;
        while ( rel != NULL_REL && rel != null )
        {
            if ( includeRels )
            {
                path.addLast( rel );
            }
            currentNode = rel.getOtherNode( currentNode );
            if ( includeNodes && !currentNode.equals( endNode ) )
            {
                path.addLast( currentNode );
            }
            rel = secondDirection.path.get( currentNode );
        }
        if ( includeNodes && !endNode.equals( matchNode ) )
        {
            path.addLast( endNode );
        }
        return path;
    }
    
    /**
     * @return One of the shortest paths found or null.
     */
    public List<Node> getPathAsNodes()
    {
        calculate();
        if ( matchNode == null )
        {
            return null;
        }
        List<PropertyContainer> path = constructPath( true, false );
        List<Node> converted = new ArrayList<Node>();
        for ( PropertyContainer node : path )
        {
            converted.add( (Node) node );
        }
        return converted;
    }

    /**
     * @return One of the shortest paths found or null.
     */
    public List<Relationship> getPathAsRelationships()
    {
        calculate();
        if ( matchNode == null )
        {
            return null;
        }
        List<PropertyContainer> path = constructPath( false, true );
        List<Relationship> converted = new ArrayList<Relationship>();
        for ( PropertyContainer rel : path )
        {
            converted.add( (Relationship) rel );
        }
        return converted;
    }
}