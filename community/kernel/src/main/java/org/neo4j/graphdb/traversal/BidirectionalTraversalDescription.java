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
package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

/**
 * Represents a description of a bidirectional traversal. A Bidirectional
 * traversal has a start side and an end side and an evaluator to handle
 * collisions between those two sides, collisions which generates paths
 * between start and end node(s).
 * 
 * A {@link BidirectionalTraversalDescription} is immutable and each
 * method which adds or modifies the behavior returns a new instances that
 * includes the new modification, leaving the instance which returns the new
 * instance intact.
 * 
 * The interface is still experimental and may still change significantly.
 * 
 * @author Mattias Persson
 * @see TraversalDescription
 */
public interface BidirectionalTraversalDescription
{
    /**
     * Sets the start side {@link TraversalDescription} of this bidirectional
     * traversal. The point of a bidirectional traversal is that the start
     * and end side will meet (or collide) in the middle somewhere and
     * generate paths evaluated and returned by this traversal.
     * @param startSideDescription the {@link TraversalDescription} to use
     * for the start side traversal.
     * @return a new traversal description with the new modifications.
     */
    BidirectionalTraversalDescription startSide( TraversalDescription startSideDescription );

    /**
     * Sets the end side {@link TraversalDescription} of this bidirectional
     * traversal. The point of a bidirectional traversal is that the start
     * and end side will meet (or collide) in the middle somewhere and
     * generate paths evaluated and returned by this traversal.
     * @param endSideDescription the {@link TraversalDescription} to use
     * for the end side traversal.
     * @return a new traversal description with the new modifications.
     */
    BidirectionalTraversalDescription endSide( TraversalDescription endSideDescription );
    
    /**
     * Sets both the start side and end side of this bidirectional traversal,
     * the {@link #startSide(TraversalDescription) start side} is assigned the
     * {@code sideDescription} and the {@link #endSide(TraversalDescription) end side}
     * is assigned the same description, although
     * {@link TraversalDescription#reverse() reversed}. This will replace any
     * traversal description previously set by {@link #startSide(TraversalDescription)}
     * or {@link #endSide(TraversalDescription)}.
     * 
     * @param sideDescription the {@link TraversalDescription} to use for both sides
     * of the bidirectional traversal. The end side will have it
     * {@link TraversalDescription#reverse() reversed}
     * @return a new traversal description with the new modifications.
     */
    BidirectionalTraversalDescription mirroredSides( TraversalDescription sideDescription );
    
    /**
     * Sets the collision policy to use during this traversal. Branch collisions
     * happen between {@link TraversalBranch}es where start and end branches
     * meet and {@link Path}s are generated from it.
     * 
     * @param collisionDetection the {@link BranchCollisionPolicy} to use during
     * this traversal.
     * @return a new traversal description with the new modifications.
     */
    BidirectionalTraversalDescription collisionPolicy( BranchCollisionPolicy collisionDetection );

    /**
     * @deprecated Please use {@link #collisionPolicy(BranchCollisionPolicy)}
     * @param collisionDetection the {@code BranchCollisionPolicy} to use during
     * this traversal.
     * @return a new traversal description with the new modifications.
     */
    BidirectionalTraversalDescription collisionPolicy( org.neo4j.kernel.impl.traversal.BranchCollisionPolicy collisionDetection );

    /**
     * Sets the {@link Evaluator} to use for branch collisions. The outcome
     * returned from the evaluator affects the colliding branches.
     * @param collisionEvaluator the {@link Evaluator} to use for evaluating
     * branch collisions.
     * @return a new traversal description with the new modifications.
     */
    BidirectionalTraversalDescription collisionEvaluator( Evaluator collisionEvaluator );
    
    /**
     * Sets the {@link PathEvaluator} to use for branch collisions. The outcome
     * returned from the evaluator affects the colliding branches.
     * @param collisionEvaluator the {@link PathEvaluator} to use for evaluating
     * branch collisions.
     * @return a new traversal description with the new modifications.
     */
    BidirectionalTraversalDescription collisionEvaluator( PathEvaluator collisionEvaluator );
    
    /**
     * In a bidirectional traversal the traverser alternates which side
     * (start or end) to move further for each step. This sets the
     * {@link SideSelectorPolicy} to use.
     * 
     * @param sideSelector the {@link SideSelectorPolicy} to use for this
     * traversal.
     * @param maxDepth optional max depth parameter to the side selector.
     * Why is max depth a concern of the {@link SideSelector}? Because it has
     * got knowledge of both the sides of the traversal at any given point.
     * @return a new traversal description with the new modifications.
     */
    BidirectionalTraversalDescription sideSelector( SideSelectorPolicy sideSelector, int maxDepth );
    
    /**
     * Traverse between a given {@code start} and {@code end} node with all
     * applied rules and behavior in this traversal description.
     * A {@link Traverser} is returned which is used to step through the
     * graph and getting results back. The traversal is not guaranteed to
     * start before the Traverser is used.
     *
     * @param start {@link Node} to use as starting point for the start
     * side in this traversal.
     * @param end {@link Node} to use as starting point for the end
     * side in this traversal.
     * @return a {@link Traverser} used to step through the graph and to get
     * results from.
     */
    Traverser traverse( Node start, Node end );
    
    /**
     * Traverse between a set of {@code start} and {@code end} nodes with all
     * applied rules and behavior in this traversal description.
     * A {@link Traverser} is returned which is used to step through the
     * graph and getting results back. The traversal is not guaranteed to
     * start before the Traverser is used.
     *
     * @param start set of nodes to use as starting points for the start
     * side in this traversal.
     * @param end set of nodes to use as starting points for the end
     * side in this traversal.
     * @return a {@link Traverser} used to step through the graph and to get
     * results from.
     */
    Traverser traverse( Iterable<Node> start, Iterable<Node> end );
}
