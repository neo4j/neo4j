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
    private final Map<Node,Relationship> firstSet = 
        new HashMap<Node,Relationship>();
    private final Map<Node,Relationship> secondSet = 
        new HashMap<Node,Relationship>();
    private final int maxDepth;
    private final Node startNode;
    private final Node endNode;
    private final Object[] relTypesAndDirections;
    private boolean doneCalculation;
    Node matchNode;

    public void reset()
    {
        firstSet.clear();
        secondSet.clear();
        doneCalculation = false;
        matchNode = null;
    }
    
    private static final Relationship NULL_REL = new FakeRelImpl();

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
        List<Node> firstList = new ArrayList<Node>();
        firstList.add( startNode );
        List<Node> secondList = new ArrayList<Node>();
        secondList.add( endNode );
        int firstDepth = maxDepth / 2;
        int secondDepth = firstDepth + (maxDepth % 2);
        List<Node> nextFirstList = new ArrayList<Node>();
        List<Node> nextSecondList = new ArrayList<Node>();
        Iterator<Node> firstItr = firstList.iterator();
        Iterator<Node> secondItr = secondList.iterator();
        int currentFirstDepth = 0;
        int currentSecondDepth = 0;
        firstSet.put( startNode, NULL_REL );
        secondSet.put( endNode, NULL_REL );
        while ( firstItr.hasNext() || secondItr.hasNext() )
        {
            if ( firstItr.hasNext() )
            {
                Node node = firstItr.next();
                if ( secondSet.containsKey( node ) )
                {
                    matchNode = node;
                    doneCalculation = true;
                    return true;
                }
                if ( currentFirstDepth + 1 <= firstDepth )  
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
                            Relationship oldRel = firstSet.put( otherNode, rel );
                            if ( oldRel == null )
                            {
                                nextFirstList.add( otherNode );
                            }
                            else
                            {
                                firstSet.put( otherNode, oldRel );
                            }
                        }
                    }
                }
            }
            if ( secondItr.hasNext() )
            {
                Node node = secondItr.next();
                if ( firstSet.containsKey( node ) )
                {
                    matchNode = node;
                    doneCalculation = true;
                    return true;
                }
                if ( currentSecondDepth + 1 <= secondDepth )  
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
                            Relationship oldRel = secondSet.put( otherNode, rel );
                            if ( oldRel == null )
                            {
                                nextSecondList.add( otherNode );
                            }
                            else
                            {
                                secondSet.put( otherNode, oldRel );
                            }
                        }
                    }
                }
            }
            if ( !firstItr.hasNext() && currentFirstDepth + 1 <= firstDepth )
            {
                currentFirstDepth++;
                firstList = nextFirstList;
                nextFirstList = new ArrayList<Node>();
                firstItr = firstList.iterator();
            }
            if ( !secondItr.hasNext() && currentSecondDepth + 1 <= secondDepth )
            {
                currentSecondDepth++;
                secondList = nextSecondList;
                nextSecondList = new ArrayList<Node>();
                secondItr = secondList.iterator();
            }
        }
        doneCalculation = true;
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
        Relationship rel = firstSet.get( matchNode );
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
            rel = firstSet.get( currentNode );
        }
        if ( includeNodes )
        {
            path.addFirst( startNode );
            if ( !matchNode.equals( startNode ) )
            {
                path.addLast( matchNode );
            }
        }
        rel = secondSet.get( matchNode );
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
            rel = secondSet.get( currentNode );
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