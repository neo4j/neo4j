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

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;

/**
 * A traverser that traverses the node space depth-first. Depth-first means that
 * it fully visits the end-node of the first relationship before it visits the
 * end-nodes of the second and subsequent relationships. This class is package
 * private: any documentation interesting to a client programmer should reside
 * in {@link InternalTraverser}. For some implementation documentation, see
 * {@link AbstractTraverser}.
 * 
 * @see InternalTraverser
 * @see AbstractTraverser
 * @see BreadthFirstTraverser
 */
// TODO: document reason for initializeList() and the declaration of 'stack'
class DepthFirstTraverser extends AbstractTraverser
{
    private java.util.Stack<TraversalPositionImpl> stack;

    /**
     * Creates a DepthFirstTraverser according the contract of
     * {@link AbstractTraverser#AbstractTraverser AbstractTraverser}.
     */
    DepthFirstTraverser( Node startNode, RelationshipType[] traversableRels,
        Direction[] traversableDirections, RelationshipType[] preservingRels,
        Direction[] preservingDirections, StopEvaluator stopEvaluator,
        ReturnableEvaluator returnableEvaluator, RandomEvaluator randomEvaluator )
    {
        super( startNode, traversableRels, traversableDirections,
            preservingRels, preservingDirections, stopEvaluator,
            returnableEvaluator, randomEvaluator );
    }

    void addPositionToList( TraversalPositionImpl position )
    {
        this.stack.push( position );
    }

    TraversalPositionImpl getNextPositionFromList()
    {
        return this.stack.pop();
    }

    boolean listIsEmpty()
    {
        return this.stack.empty();
    }

    final boolean traverseChildrenInNaturalOrder()
    {
        return false;
    }

    void initializeList()
    {
        this.stack = new java.util.Stack<TraversalPositionImpl>();
    }
}