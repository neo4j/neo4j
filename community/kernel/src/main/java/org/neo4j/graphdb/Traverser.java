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
package org.neo4j.graphdb;

import java.util.Collection;
import java.util.Iterator;

import org.neo4j.graphdb.traversal.TraversalDescription;

/**
 * A traversal in the graph. A Traverser is an {@link Iterable} that
 * encapsulates a number of traversal parameters (defined at traverser creation)
 * and returns an iterator of nodes that match those parameters. It is created
 * by invoking {@link Node#traverse Node.traverse(...)}. Upon creation, the
 * traverser is positioned at the start node, but it doesn't actually start
 * traversing until its {@link #iterator() iterator().next()} method is invoked
 * and will then traverse lazily one step each time {@code next} is called.
 * <p>
 * When a Traverser is created it is parameterized with two evaluators and the
 * relationship types to traverse, with the direction to traverse each type in.
 * The evaluators are used for determining for each node in the set of candidate
 * nodes if it should be returned or not, and if the traversal should be pruned
 * (stopped) at this point. The nodes that are traversed by a Traverser are each
 * visited exactly once, meaning that the returned iterator of nodes will never
 * contain duplicate nodes. This also means that the traversed relationships
 * will form a spanning tree over the traversed nodes, with the side effect that
 * some internal relationships between nodes in the traversal are not traversed
 * (and hence not visible {@link TraversalPosition#lastRelationshipTraversed()
 * in the evaluators}).
 * <p>
 * Typically a Traverser is used in a for-each loop as follows:
 * 
 * <pre>
 * <code>
 * Traverser friends = node.{@link Node#traverse(Traverser.Order, StopEvaluator, ReturnableEvaluator, RelationshipType, Direction) traverse}( {@link Order#BREADTH_FIRST Order.BREADTH_FIRST},
 *     {@link StopEvaluator#END_OF_GRAPH StopEvaluator.END_OF_GRAPH}, {@link ReturnableEvaluator#ALL_BUT_START_NODE ReturnableEvaluator.ALL_BUT_START_NODE},
 *     {@link RelationshipType MyRelationshipTypes.KNOWS}, {@link Direction#OUTGOING Direction.OUTGOING} );
 * for ( {@link Node Node} friend : friends )
 * {
 *     // ...
 * }
 * </code>
 * </pre>
 *
 * Relationships are equally well traversed regardless of their direction,
 * performance-wise.
 *
 * @see Node#traverse
 * @deprecated because of an unnatural and too tight coupling with
 * {@link Node}. Also because of the introduction of a new traversal framework
 * and the usage of it. The new way of doing traversals is by creating a
 * new {@link TraversalDescription} from
 * {@link org.neo4j.graphdb.GraphDatabaseService#traversalDescription()}, add rules and
 * behaviors to it and then calling
 * {@link TraversalDescription#traverse(Node...)}.
 */
public interface Traverser extends Iterable<Node>
{
    /**
     * Defines a traversal order as used by the traversal framework.
     * <p>
     * Nodes can be traversed either {@link #BREADTH_FIRST breadth first} or
     * {@link #DEPTH_FIRST depth first}. A depth first traversal is often more
     * likely to find one matching node before a breadth first traversal. A
     * breadth first traversal will always find the closest matching nodes
     * first, which means that {@link TraversalPosition#depth()} will return the
     * length of the shortest path from the start node to the node at that
     * position, which is not guaranteed for depth first traversals.
     * <p>
     * A breadth first traversal usually needs to store more state about where
     * the traversal should go next than a depth first traversal does. Depth
     * first traversals are thus more memory efficient.
     */
    static enum Order
    {
        /**
         * Sets a depth first traversal meaning the traverser will go as deep as
         * possible (increasing depth for each traversal) before traversing next
         * relationship on same depth.
         */
        DEPTH_FIRST,

        /**
         * Sets a breadth first traversal meaning the traverser will traverse
         * all relationships on the current depth before going deeper.
         */
        BREADTH_FIRST
    }

    /**
     * Returns the current traversal position, that is where the traversal is at
     * the moment. It contains information such as which node we're at, which
     * the last traversed relationship was (if any) and at which depth the
     * current position is (relative to the starting node). You can use it in
     * your traverser for-loop like this:
     *
     * <pre>
     * <code>
     * Traverser traverser = node.{@link Node#traverse traverse}( ... );
     * for ( {@link Node Node} node : traverser )
     * {
     *     {@link TraversalPosition TraversalPosition} currentPosition = traverser.currentPosition();
     *     // Get "current position" information right here.
     * }
     * </code>
     * </pre>
     *
     * @return The current traversal position
     */
    TraversalPosition currentPosition();

    /**
     * Returns a collection of all nodes for this traversal. It traverses
     * through the graph (according to given filters and evaluators) and
     * collects those encountered nodes along the way. When this method has
     * returned, this traverser will be at the end of its traversal, such that a
     * call to {@code hasNext()} for the {@link #iterator()} will return {@code
     * false}.
     *
     * @return A collection of all nodes for this this traversal.
     */
    Collection<Node> getAllNodes();

    // Doc: especially remove() thing
    /**
     * Returns an {@link Iterator} representing the traversal of the graph. The
     * iteration is completely lazy in that it will only traverse one step (to
     * the next "hit") for every call to {@code hasNext()}/{@code next()}.
     *
     * Consecutive calls to this method will return the same instance.
     *
     * @return An iterator for this traverser
     */
    // TODO completely resolve issues regarding this (Iterable/Iterator ...)
    // Doc: does it create a new iterator or reuse the existing one? This is
    // very important! It must be re-use, how else would currentPosition()
    // make sense?
    Iterator<Node> iterator();
}
