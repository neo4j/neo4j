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

import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;

/**
 * A client hook for evaluating whether a specific node should be returned from
 * a traverser.
 * <p>
 * When a traverser is created the client parameterizes it with an instance of a
 * ReturnableEvaluator. The traverser then invokes the {@link #isReturnableNode
 * isReturnableNode()} operation just before returning a specific node, allowing
 * the client to either approve or disapprove of returning that node.
 * <p>
 * When implementing a ReturnableEvaluator, the client investigates the
 * information encapsulated in a {@link TraversalPosition} to decide whether a
 * node is returnable. For example, here's a snippet detailing a
 * ReturnableEvaluator that will return all leaf nodes:
 *
 * <pre>
 * <code>
 * ReturnableEvaluator returnEvaluator = new ReturnableEvaluator()
 * {
 *     public boolean {@link #isReturnableNode(TraversalPosition) isReturnableNode}( {@link TraversalPosition TraversalPosition} position )
 *     {
 *         // Return nodes that don't have any outgoing relationships,
 *         // only incoming relationships, i.e. leaf nodes.
 *         return !position.{@link TraversalPosition#currentNode() currentNode}().{@link Node#hasRelationship(Direction) hasRelationship}(
 *             {@link Direction#OUTGOING Direction.OUTGOING} );
 *     }
 * };
 * </code>
 * </pre>
 * @deprecated because of the introduction of a new traversal framework,
 * see more information at {@link TraversalDescription} and
 * {@link Traversal} and the new traversal framework's equivalent
 * {@link org.neo4j.function.Predicate}.
 */
public interface ReturnableEvaluator
{
    /**
     * A returnable evaluator that returns all nodes encountered.
     */
    ReturnableEvaluator ALL = new ReturnableEvaluator()
    {
        public boolean isReturnableNode( final TraversalPosition currentPosition )
        {
            return true;
        }
    };

    /**
     * A returnable evaluator that returns all nodes except the start node.
     */
    ReturnableEvaluator ALL_BUT_START_NODE = new ReturnableEvaluator()
    {
        public boolean isReturnableNode( final TraversalPosition currentPosition )
        {
            return currentPosition.notStartNode();
        }
    };

    /**
     * Method invoked by traverser to see if the current position is a
     * returnable node.
     *
     * @param currentPos the traversal position
     * @return True if current position is a returnable node
     */
    boolean isReturnableNode( TraversalPosition currentPos );
}
