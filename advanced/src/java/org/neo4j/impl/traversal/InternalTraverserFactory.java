/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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

import java.util.Arrays;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;

/**
 * The factory for {@link Traverser} instances. The TraverserFactory is
 * responsible for creating and parameterizing traversers. It has a single
 * {@link #createTraverser createTraverser()} factory method which is overloaded
 * with a number of different default parameters, which are described briefly
 * below:
 * <P>
 * <UL>
 * <LI><CODE>type</CODE> - which is either one of
 * {@link Traverser#DEPTH_FIRST} and {@link Traverser#BREADTH_FIRST}. This
 * parameter decides the strategy used when traversering the node space.
 * <LI><CODE>startNode</CODE> - the starting node for the traverser.
 * <LI><CODE>traversableRels</CODE> - a list of the relationship types that
 * will be traversed.
 * <LI><CODE>traversableDirs</CODE> - a list of directions in which the
 * traversable relationships will be traversed. The values in this array should
 * be either one of {@link Traverser#DIRECTION_FORWARDS},
 * {@link Traverser#DIRECTION_BACKWARDS} or {@link Traverser#DIRECTION_BOTH}.
 * This parameter is ignored in the case of non-directed relationships.
 * <LI><CODE>stopEvaluator</CODE> - the visitor that is used to evaluate
 * whether the traverser should stop before traversing a specific node. Use this
 * parameter to limit the size of the traversal.
 * <LI><CODE>returnableEvaluator</CODE> - the visitor that is used to
 * evaluate whether the traverser should return a specific node. Use this
 * parameter to affect the selection of nodes from the traversal.
 * </UL>
 * <P>
 * The factory methods treat all parameters as immutable and thus do not modify
 * them. This guarantees that the client can optimize by reusing parameters
 * between invocations of <CODE>createTraverser()</CODE>.
 */
public final class InternalTraverserFactory
{
    public InternalTraverserFactory()
    {
    }

    /**
     * Creates a parameterized traverser, using non-default values on all
     * parameters.
     * @param type
     *            the traverser type, either one of
     *            {@link Traverser#DEPTH_FIRST} and
     *            {@link Traverser#BREADTH_FIRST}
     * @param startNode
     *            the start node for the new traverser
     * @param traversableRels
     *            the relationship types that the new traverser will traverse
     * @param traversableDirs
     *            the directions that the traversable relationships will be
     *            traversed in
     * @param stopEvaluator
     *            the client hook for limiting the traversal size
     * @param returnableEvaluator
     *            the client hook for evaluating nodes before they are returned
     * @throws IllegalArgumentException
     *             if one or more of the parameters are invalid
     */
    public Traverser createTraverser( Order traversalOrder, Node startNode,
        RelationshipType[] traversableRels, Direction[] traversableDirs,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator )
    {
        if ( traversableRels == null || traversableDirs == null )
        {
            throw new IllegalArgumentException(
                "Using this constructor requires that traversable "
                    + "relationships array and traversable directions array "
                    + "isn't null: travRels["
                    + Arrays.toString( traversableRels ) + "] " + "travDirs["
                    + Arrays.toString( traversableDirs ) + "]" );
        }
        if ( traversableRels.length != traversableDirs.length )
        {
            throw new IllegalArgumentException(
                "Length of traversable relationships array isn't equal to "
                    + "length of traversable directions array: "
                    + "travRels.length[" + traversableRels.length + "] != "
                    + "travDirs.length[" + traversableDirs.length + "]" );
        }
        if ( traversalOrder == Order.BREADTH_FIRST )
        {
            return new BreadthFirstTraverser( startNode, traversableRels,
                traversableDirs, null, null, // preserving rels
                stopEvaluator, returnableEvaluator, null );
        }
        else if ( traversalOrder == Order.DEPTH_FIRST )
        {
            return new DepthFirstTraverser( startNode, traversableRels,
                traversableDirs, null, null, // preserving rels
                stopEvaluator, returnableEvaluator, null );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown traverser type: "
                + traversalOrder );
        }
    }

    /**
     * Creates a parameterized traverser which traverses all relationships in
     * the {@link Traverser#DIRECTION_BOTH default} direction.
     * @param type
     *            the traverser type, either one of
     *            {@link Traverser#DEPTH_FIRST} and
     *            {@link Traverser#BREADTH_FIRST}
     * @param startNode
     *            the start node for the new traverser
     * @param traversableRels
     *            the relationship types that the new traverser will traverse
     * @param stopEvaluator
     *            the client hook for limiting the traversal size
     * @param returnableEvaluator
     *            the client hook for evaluating nodes before they are returned
     * @throws IllegalArgumentException
     *             if one or more of the parameters are invalid
     */
    public Traverser createTraverser( Order traversalOrder, Node startNode,
        RelationshipType[] traversableRels, StopEvaluator stopEvaluator,
        ReturnableEvaluator returnableEvaluator )
    {
        if ( traversalOrder == Order.BREADTH_FIRST )
        {
            return new BreadthFirstTraverser( startNode, traversableRels, null, 
                null, null, // preserving rels
                stopEvaluator, returnableEvaluator, null );
        }
        else if ( traversalOrder == Order.DEPTH_FIRST )
        {
            return new DepthFirstTraverser( startNode, traversableRels, null,
                null, null, // preserving rels
                stopEvaluator, returnableEvaluator, null );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown traverser type: "
                + traversalOrder );
        }
    }

    /**
     * Creates a parameterized traverser which traverses a single relationship
     * type.
     * @param type
     *            the traverser type, either one of
     *            {@link Traverser#DEPTH_FIRST} and
     *            {@link Traverser#BREADTH_FIRST}
     * @param startNode
     *            the start node for the new traverser
     * @param traversableRel
     *            the relationship type that the new traverser will traverse
     * @param direction
     *            the direction the <CODE>traversableRel</CODE> will be
     *            traversed in
     * @param stopEvaluator
     *            the client hook for limiting the traversal size
     * @param returnableEvaluator
     *            the client hook for evaluating nodes before they are returned
     * @throws IllegalArgumentException
     *             if one or more of the parameters are invalid
     */
    public Traverser createTraverser( Order traversalOrder, Node startNode,
        RelationshipType traversableRel, Direction direction,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator )
    {
        RelationshipType traversableRels[] = 
            new RelationshipType[] { traversableRel };
        Direction traversableDirs[] = new Direction[] { direction };
        if ( traversalOrder == Order.BREADTH_FIRST )
        {
            return new BreadthFirstTraverser( startNode, traversableRels,
                traversableDirs, null, null, // preserving rels
                stopEvaluator, returnableEvaluator, null );
        }
        else if ( traversalOrder == Order.DEPTH_FIRST )
        {
            return new DepthFirstTraverser( startNode, traversableRels,
                traversableDirs, null, null, // preserving rels
                stopEvaluator, returnableEvaluator, null );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown traverser type: "
                + traversalOrder );
        }
    }

    public Traverser createTraverser( Order traversalOrder, Node startNode,
        RelationshipType[] traversableRels, Direction[] traversableDirs,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        RandomEvaluator randomEvaluator )
    {
        if ( traversableRels == null || traversableDirs == null )
        {
            throw new IllegalArgumentException(
                "Using this constructor requires that traversable "
                    + "relationships array and traversable directions array "
                    + "isn't null: travRels["
                    + Arrays.toString( traversableRels ) + "] " + "travDirs["
                    + Arrays.toString( traversableDirs ) + "]" );
        }
        if ( traversableRels.length != traversableDirs.length )
        {
            throw new IllegalArgumentException(
                "Length of traversable relationships array isn't equal to "
                    + "length of traversable directions array: "
                    + "travRels.length[" + traversableRels.length + "] != "
                    + "travDirs.length[" + traversableDirs.length + "]" );
        }
        if ( traversalOrder == Order.BREADTH_FIRST )
        {
            return new BreadthFirstTraverser( startNode, traversableRels,
                traversableDirs, null, null, // preserving rels
                stopEvaluator, returnableEvaluator, randomEvaluator );
        }
        else if ( traversalOrder == Order.DEPTH_FIRST )
        {
            return new DepthFirstTraverser( startNode, traversableRels,
                traversableDirs, null, null, // preserving rels
                stopEvaluator, returnableEvaluator, randomEvaluator );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown traverser type: "
                + traversalOrder );
        }
    }
}