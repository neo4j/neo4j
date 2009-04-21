/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.traversal;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.TraversalPosition;
import org.neo4j.api.core.Traverser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.NoSuchElementException;

/**
 * This class provides a skeletal implementation of the {@link Traverser}
 * interface to minimize the effort required to implement the
 * {@link BreadthFirstTraverser} and {@link DepthFirstTraverser}. This is a
 * package private class used only for implementation-reuse purposes. Any
 * documentation interesting to a client programmer should reside in
 * {@link Traverser}.
 * <P>
 * The AbstractTraverser contains the logic and functionality that is common to
 * both traverser subtypes. In reality, this means ALL functionality except for
 * the data structure used to store the traverser list (captured via the four
 * abstract operations {@link #initializeList initializeList},
 * {@link #addPositionToList addPositionToList},
 * {@link #getNextPositionFromList getNextPositionFromList} and
 * {@link #listIsEmpty listIsEmpty}) and whether children are processed in
 * natural- or reverse order (captured via the abstract operation
 * {@link #traverseChildrenInNaturalOrder traverserChildrenInNaturalOrder}).
 * <P>
 * In order to minimize overhead, the AbstractTraverser caches the result of
 * {@link #hasNext} so that the subsequent implementation of {@link #next} or
 * {@link #nextNode} won't have to redo the traversal.
 * 
 * @see Traverser
 * @see BreadthFirstTraverser
 * @see DepthFirstTraverser
 */
abstract class AbstractTraverser implements Traverser, Iterator<Node>
{
    private RelationshipType[] traversableRels = null;
    private Direction[] traversableDirs = null;
    private RelationshipType[] preservingRels = null;
    private Direction[] preservingDirs = null;
    private StopEvaluator stopEvaluator = null;
    private ReturnableEvaluator returnableEvaluator = null;

    private Set<Node> visitedNodes = new HashSet<Node>();
    private Node cachedNode = null;
    private int returnedNodesCount = 0;
    private TraversalPositionImpl traversalPosition = null;

    /**
     * Creates an AbstractTraverser subclass, for information about the
     * arguments please see the documentation of {@link InternalTraverserFactory}.
     */
    AbstractTraverser( Node startNode, RelationshipType[] traversableRels,
        Direction[] traversableDirs, RelationshipType[] preservingRels,
        Direction[] preservingDirs, StopEvaluator stopEvaluator,
        ReturnableEvaluator returnableEvaluator, RandomEvaluator randomEvaluator )
    {
        // Sanity check
        if ( startNode == null || traversableRels == null
            || stopEvaluator == null || returnableEvaluator == null )
        {
            String s = "startNode = " + startNode + ", traversableRels = "
                + Arrays.toString( traversableRels ) + ", stopEvaluator = "
                + stopEvaluator + ", returnableEvaluator = "
                + returnableEvaluator;
            throw new IllegalArgumentException( "null argument(s): " + s );
        }

        // Assign attributes
        this.traversableRels = traversableRels;
        this.traversableDirs = traversableDirs;
        this.preservingRels = preservingRels;
        this.preservingDirs = preservingDirs;
        this.stopEvaluator = stopEvaluator;
        this.returnableEvaluator = returnableEvaluator;

        // Initialize the (subclass-specific) traverser list
        this.initializeList();

        // Add the first position to the list
        TraversalPositionImpl firstPos = this.createPosition( startNode, null,
            null, 0 );
        this.addPositionToList( firstPos );
    }

    public Iterator<Node> iterator()
    {
        return this;
    }

    // javadoc: see java.util.Iterator or Traverser
    public boolean hasNext()
    {
        // If we have one cached, then we're definitely go
        if ( this.cachedNode != null )
        {
            return true;
        }
        // If not, check if we can find one
        else
        {
            this.cachedNode = this.traverseToNextNode();
            return this.cachedNode != null;
        }
    }

    // javadoc: see java.util.Iterator or Traverser
    public Node next()
    {
        return this.nextNode();
    }

    // javadoc: see java.util.Iterator or Traverser
    public Node nextNode()
    {
        Node nodeToReturn = this.cachedNode;

        // If no node is cached, then traverse to next
        if ( nodeToReturn == null )
        {
            nodeToReturn = this.traverseToNextNode();
        }

        // If we couldn't find a node by traversing, report this by
        // throwing a NoSuchElementException (as per java.util.Iterator)
        if ( nodeToReturn == null )
        {
            throw new NoSuchElementException();
        }

        // Clear the cache
        this.cachedNode = null;

        return nodeToReturn;
    }

    // Traverses to the next node and returns it, or null if there are
    // no more nodes in this traversal
    private Node traverseToNextNode()
    {
        Node nodeToReturn = null;

        while ( !this.listIsEmpty() && nodeToReturn == null )
        {
            // Get next node from the list
            TraversalPositionImpl currentPos = this.getNextPositionFromList();
            traversalPosition = currentPos;
            Node currentNode = currentPos.currentNode();

            // Make sure we haven't visited this node before: add() returns
            // true if the set doesn't contain the node -- which means that
            // we're fine.
            if ( visitedNodes.add( currentNode ) )
            {
                // Update position with however many nodes we've returned
                // from the traversal up until now, this may be used to
                // determine whether we should stop and/or return currentPos
                currentPos.setReturnedNodesCount( this.returnedNodesCount );

                // If we're not stopping, then add related nodes to the list
                // or current position not valid (last trav rel deleted)
                if ( // !currentPos.isValid() ||
                    !this.stopEvaluator.isStopNode( currentPos ) )
                {
                    // Add the nodes at the end of all traversable- and
                    // preserving relationships
                    try
                    {
                        this.addEndNodesToList( currentPos,
                            this.traversableRels, this.traversableDirs );
                        this.addEndNodesToList( currentPos,
                            this.preservingRels, this.preservingDirs );
                    }
                    catch ( NotFoundException e )
                    {
                        // currentNode deleted in other tx
                        // try next position from list
                        continue;
                    }
                }

                // Check if we should return currentPos
                if ( // currentPos.isValid() &&
                    this.returnableEvaluator.isReturnableNode( currentPos ) )
                {
                    this.returnedNodesCount++;
                    nodeToReturn = currentPos.currentNode();
                }
            }
        }

        return nodeToReturn;
    }

    // Adds the nodes at the end or start (depending on 'dirs') of all
    // relationships of a type in 'relTypes' that are attached to the
    // node in 'currentPos' to the list
    private void addEndNodesToList( TraversalPositionImpl currentPos,
        RelationshipType[] relTypes, Direction[] dirs )
    {
        if ( relTypes == null )
        {
            return;
        }

        // Get the node and compute new depth
        Node currentNode = currentPos.currentNode();
        int newDepth = currentPos.depth() + 1;

        // For all relationship types...
        for ( int i = 0; i < relTypes.length; i++ )
        {
            // ... get all rels of that type and direction from currentNode
            Iterable<Relationship> rels = null;
            try
            {
                if ( dirs == null || dirs[i] == Direction.BOTH
                    || dirs[i] == null )
                {
                    rels = currentNode.getRelationships( relTypes[i] );
                }
                else
                {
                    rels = currentNode.getRelationships( relTypes[i], dirs[i] );
                }
            }
            catch ( NotFoundException e )
            {
                // node deleted in other tx
            }

            // The order we process relationships is really irrelevant, but
            // as long as we have a non-deterministic ordering in
            // currentNode.getRelationship(), we'll have to resort to ugly
            // hacks like this to be able to return the branches in the
            // the order that people generally expect.

            if ( rels != null )
            {
                for ( Relationship rel : rels )
                {
                    this.processRel( currentNode, rel, newDepth );
                }
            }
        }
    }

    private void processRel( Node currentNode, Relationship rel, int newDepth )
    {
        Node endNode = rel.getOtherNode( currentNode );
        TraversalPositionImpl newPos = this.createPosition( endNode,
            currentNode, rel, newDepth );
        this.addPositionToList( newPos );
    }

    // Creates a traversal position populated with the specified data
    private TraversalPositionImpl createPosition( Node currentNode,
        Node previousNode, Relationship lastRelTraversed, int currentDepth )
    {
        return new TraversalPositionImpl( currentNode, previousNode,
            lastRelTraversed, currentDepth );
    }

    // javadoc: see Traverser
    public Collection<Node> getAllNodes()
    {
        // Temp storage
        java.util.List<Node> tempList = new java.util.ArrayList<Node>();

        // Traverse until the end, my beautiful friend
        while ( this.hasNext() )
        {
            tempList.add( this.nextNode() );
        }

        // Return nodes
        return tempList;
    }

    // javadoc: see java.util.Iterator
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    // javadoc: see Traverser
    public Traverser sort( NodeSortInfo<Node> nsi )
    {
        ArrayList<Node> tempList = new ArrayList<Node>();

        // Traverse and get all remaining nodes
        while ( this.hasNext() )
        {
            tempList.add( this.nextNode() );
        }

        Collections.sort( tempList, nsi );
        return new SortedTraverser( tempList );
    }

    public TraversalPosition currentPosition()
    {
        return traversalPosition;
    }

    /**
     * Instantiates the underlying container used to store future traversal
     * positions. This operation is required to overcome, ehum, "limitations" in
     * the way java resolves constructors and field initialization in complex
     * inheritance hierarchies.
     */
    abstract void initializeList();

    /**
     * Adds <CODE>position</CODE> to the end of the list.
     */
    abstract void addPositionToList( TraversalPositionImpl position );

    /**
     * Returns the next position from the list.
     */
    abstract TraversalPositionImpl getNextPositionFromList();

    /**
     * Returns <CODE>true</CODE> if there are no more nodes to traverse in the
     * list, <CODE>false</CODE> otherwise.
     */
    abstract boolean listIsEmpty();

    /**
     * Returns <CODE>true</CODE> if the traverser subtype wants children
     * processed in natural order, <CODE>false</CODE> if it prefers them
     * processed in reverse order. See implementation comments in 
     * <CODE>traverseToNextNode()</CODE> for more information.
     */
    abstract boolean traverseChildrenInNaturalOrder();
}
