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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.PropertyContainer;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.TraversalPosition;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;

/**
 * This class can be used to find one of the shortest paths in an unweighted
 * network. Only supports one relationship type and direction is always BOTH.
 * Based on code from an early IMDB example application.
 * @see FindPath
 * @author Patrik Larsson
 */
public class FindSinglePath
{
    private HashMap<Node,List<Relationship>> predecessors1;
    private HashMap<Node,List<Relationship>> predecessors2;
    private int maxDepth;
    private Node startNode;
    private Node endNode;
    private RelationshipType relationshipType;
    private boolean doneCalculation;
    Node matchNode;

    /**
     * Resets the result data to force the computation to be run again when some
     * result is asked for.
     */
    public void reset()
    {
        predecessors1 = new HashMap<Node,List<Relationship>>();
        predecessors2 = new HashMap<Node,List<Relationship>>();
        doneCalculation = false;
        matchNode = null;
    }

    /**
     * Makes the main calculation. If some limit is set, the shortest path(s)
     * that could be found within those limits will be calculated.
     * @return True if a path was found.
     */
    public boolean calculate()
    {
        // Do this first as a general error check since this is supposed to be
        // called whenever a result is asked for.
        if ( startNode == null || endNode == null )
        {
            throw new RuntimeException( "Start or end node undefined." );
        }
        // Don't do it more than once
        if ( doneCalculation )
        {
            return true;
        }
        doneCalculation = true;
        // Special case when path length is zero
        if ( startNode.equals( endNode ) )
        {
            matchNode = startNode;
            return true;
        }
        PathStopEval stopEval1 = new PathStopEval( maxDepth / 2 );
        PathStopEval stopEval2 = new PathStopEval( maxDepth / 2 + maxDepth % 2 );
        PathReturnEval returnEval1 = new PathReturnEval( predecessors1,
            predecessors2 );
        PathReturnEval returnEval2 = new PathReturnEval( predecessors2,
            predecessors1 );
        Traverser trav1 = startNode.traverse( Order.BREADTH_FIRST, stopEval1,
            returnEval1, relationshipType, Direction.BOTH );
        Traverser trav2 = endNode.traverse( Order.BREADTH_FIRST, stopEval2,
            returnEval2, relationshipType, Direction.BOTH );
        Iterator<Node> itr1 = trav1.iterator();
        Iterator<Node> itr2 = trav2.iterator();
        while ( itr1.hasNext() || itr2.hasNext() )
        {
            if ( itr1.hasNext() )
            {
                itr1.next();
            }
            if ( returnEval1.getMatchNode() != null )
            {
                matchNode = returnEval1.getMatchNode();
                return true;
            }
            if ( itr2.hasNext() )
            {
                itr2.next();
            }
            if ( returnEval2.getMatchNode() != null )
            {
                matchNode = returnEval2.getMatchNode();
                return true;
            }
        }
        return false;
    }

    private static class PathStopEval implements StopEvaluator
    {
        private int maximumDepth;

        public PathStopEval( int maximumDepth )
        {
            super();
            this.maximumDepth = maximumDepth;
        }

        public boolean isStopNode( TraversalPosition currentPos )
        {
            return currentPos.depth() >= maximumDepth;
        }
    }
    private static class PathReturnEval implements ReturnableEvaluator
    {
        final Map<Node,List<Relationship>> myNodes;
        final Map<Node,List<Relationship>> otherNodes;
        private Node matchNode = null;

        public PathReturnEval( final Map<Node,List<Relationship>> myNodes,
            final Map<Node,List<Relationship>> otherNodes )
        {
            super();
            this.myNodes = myNodes;
            this.otherNodes = otherNodes;
        }

        public boolean isReturnableNode( TraversalPosition currentPos )
        {
            Node currentNode = currentPos.currentNode();
            Relationship relationshipToCurrent = currentPos
                .lastRelationshipTraversed();
            if ( relationshipToCurrent != null )
            {
                LinkedList<Relationship> predList = new LinkedList<Relationship>();
                predList.add( relationshipToCurrent );
                myNodes.put( currentNode, predList );
            }
            else
            {
                myNodes.put( currentNode, null );
            }
            if ( otherNodes.containsKey( currentNode ) )
            {
                // match
                matchNode = currentNode;
            }
            return true;
        }

        public Node getMatchNode()
        {
            return matchNode;
        }
    }

    /**
     * Set the maximum depth.
     * @param maxDepth
     */
    public void setMaxDepth( int maxDepth )
    {
        this.maxDepth = maxDepth;
    }

    /**
     * Set the end node. Will reset the calculation.
     * @param endNode
     *            the endNode to set
     */
    public void setEndNode( Node endNode )
    {
        reset();
        this.endNode = endNode;
    }

    /**
     * Set the start node. Will reset the calculation.
     * @param startNode
     *            the startNode to set
     */
    public void setStartNode( Node startNode )
    {
        this.startNode = startNode;
        reset();
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
    public FindSinglePath( Node startNode, Node endNode,
        RelationshipType relationshipType, int maxDepth )
    {
        super();
        reset();
        this.startNode = startNode;
        this.endNode = endNode;
        this.relationshipType = relationshipType;
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
        LinkedList<PropertyContainer> path = new LinkedList<PropertyContainer>();
        path.addAll( Util.constructSinglePathToNode( matchNode, predecessors1,
            true, false ) );
        path.addAll( Util.constructSinglePathToNode( matchNode, predecessors2,
            false, true ) );
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
        LinkedList<Node> pathNodes = new LinkedList<Node>();
        pathNodes.addAll( Util.constructSinglePathToNodeAsNodes( matchNode,
            predecessors1, true, false ) );
        pathNodes.addAll( Util.constructSinglePathToNodeAsNodes( matchNode,
            predecessors2, false, true ) );
        return pathNodes;
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
        List<Relationship> path = new LinkedList<Relationship>();
        path.addAll( Util.constructSinglePathToNodeAsRelationships( matchNode,
            predecessors1, false ) );
        path.addAll( Util.constructSinglePathToNodeAsRelationships( matchNode,
            predecessors2, true ) );
        return path;
    }
}
