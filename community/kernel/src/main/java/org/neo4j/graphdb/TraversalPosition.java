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


/**
 * Encapsulates information about the current traversal position.
 *
 * The TraversalPosition is mainly used in
 * {@link StopEvaluator#isStopNode(TraversalPosition)} and
 * {@link ReturnableEvaluator#isReturnableNode(TraversalPosition)} for
 * evaluating whether a position in a traversal is a point where the traversal
 * should stop or if the node at that position is to be part of the result
 * respectively.
 */
/*
 * @deprecated because of the introduction of a new traversal framework,
 * see more information at {@link TraversalDescription} and
 * {@link Traversal} and the new traversal framework's equivalent
 * {@link Path}.
 */
// @Deprecated
public interface TraversalPosition
{
    /**
     * Returns the current node.
     * 
     * @return The current node
     */
    Node currentNode();

    /**
     * Returns the previous node.
     *
     * If this TraversalPosition represents the start node <code>null</code> is
     * returned.
     *
     * @return The previous node, or <code>null</code>
     */
    Node previousNode();

    /**
     * Return the last relationship traversed.
     *
     * If this TraversalPosition represents the start node <code>null</code> is
     * returned.
     *
     * @return The last relationship traversed, or <code>null</code>.
     */
    Relationship lastRelationshipTraversed();

    /**
     * Returns the current traversal depth.
     *
     * The traversal depth is the length of the path taken to reach the current
     * traversal position. This is not necessarily the length of shortest path
     * from the start node to the node at the current position. When traversing
     * {@link Traverser.Order#BREADTH_FIRST breadth first} the depth is the
     * length of the shortest path from the start node to the node at the
     * current position, but when traversing {@link Traverser.Order#DEPTH_FIRST
     * depth first} there might exist shorter paths from the start node to the
     * node at this position.
     *
     * @return The current traversal depth
     */
    int depth();

    /**
     * Returns the number of nodes returned by the traverser so far.
     * 
     * @return The number of returned nodes.
     */
    int returnedNodesCount();

    /**
     * Returns <code>true</code> if the current position is anywhere except on
     * the start node, <code>false</code> if it is on the start node. This is
     * useful because code in {@link StopEvaluator the}
     * {@link ReturnableEvaluator evaluators} usually have to treat the edge
     * case of the start node separately and using this method makes that code a
     * lot cleaner. This allows for much cleaner code where <code>null</code>
     * checks can be avoided for return values from
     * {@link #lastRelationshipTraversed()} and {@link #previousNode()}, such as
     * in this example:
     *
     * <pre>
     * <code>
     * public boolean {@link StopEvaluator#isStopNode(TraversalPosition) isStopNode}( TraversalPosition currentPos )
     * {
     *     // Stop at nodes reached through a SOME_RELATIONSHIP.
     *     return currentPos.notStartNode()
     *         &amp;&amp; currentPos.{@link #lastRelationshipTraversed() lastRelationshipTraversed}().{@link Relationship#isType(RelationshipType) isType}(
     *             {@link RelationshipType MyRelationshipTypes.SOME_RELATIONSHIP} );
     * }
     * </code>
     * </pre>
     *
     * @return <code>true</code> if the this TraversalPosition is not at the
     *         start node, <code>false</code> if it is.
     */
    boolean notStartNode();

    /**
     * Returns <code>true</code> if the current position is the start node,
     * <code>false</code> otherwise. This is useful because code in
     * {@link StopEvaluator the} {@link ReturnableEvaluator evaluators} usually
     * have to treat the edge case of the start node separately and using this
     * method makes that code a lot cleaner. This allows for much cleaner code
     * where <code>null</code> checks can be avoided for return values from
     * {@link #lastRelationshipTraversed()} and {@link #previousNode()}, such as
     * in this example:
     *
     * <pre>
     * <code>
     * public boolean {@link ReturnableEvaluator#isReturnableNode(TraversalPosition) isReturnableNode}( TraversalPosition currentPos )
     * {
     *     // The start node, and nodes reached through SOME_RELATIONSHIP
     *     // are returnable.
     *     return currentPos.isStartNode()
     *         || currentPos.{@link #lastRelationshipTraversed() lastRelationshipTraversed}().{@link Relationship#isType(RelationshipType) isType}(
     *             {@link RelationshipType MyRelationshipTypes.SOME_RELATIONSHIP} );
     * }
     * </code>
     * </pre>
     *
     * @return <code>true</code> if the this TraversalPosition is at the start
     *         node, <code>false</code> if it is not.
     */
    boolean isStartNode();
}