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
package org.neo4j.graphdb;

/**
 * A client hook for evaluating whether the traverser should traverse beyond a
 * specific node. This is used for pruning the traversal.
 * <P>
 * When a traverser is created, the client parameterizes it with a
 * StopEvaluator. The traverser then invokes the {@link #isStopNode
 * isStopNode()} operation just before traversing the relationships of a node,
 * allowing the client to either approve or disapprove of traversing beyond that
 * node.
 * <P>
 * When implementing a StopEvaluator, the client investigates the information
 * encapsulated in a {@link TraversalPosition} to decide whether to block
 * traversal beyond a node. For example, here's a snippet detailing a
 * StopEvaluator that blocks traversal beyond a node if it has a certain
 * property value:
 * 
 * <CODE><PRE>
 * StopEvaluator stopEvaluator = new StopEvaluator()
 * {
 *     // Block traversal if the node has a property with key 'key' and value
 *     // 'someValue'
 *     public boolean {@link #isStopNode(TraversalPosition) isStopNode}( {@link TraversalPosition} position )
 *     {
 *         if ( position.{@link TraversalPosition#isStartNode() isStartNode}() )
 *         {
 *             return false;
 *         }
 *         {@link Node} node = position.{@link TraversalPosition#currentNode() currentNode}();
 *         Object someProp = node.{@link Node#getProperty(String, Object) getProperty}( "key", null );
 *         return someProp instanceof String &&
 *             ((String) someProp).equals( "someValue" );
 *     }
 * };
 * </PRE></CODE>
 */
public interface StopEvaluator
{
    /**
     * Traverse until the end of the graph. This evaluator returns 
     * <CODE>false</CODE> all the time.
     */
	public static final StopEvaluator END_OF_GRAPH = new StopEvaluator()
	{
        public boolean isStopNode( TraversalPosition currentPosition )
        {
            return false;
        }
	};
	
	/**
	 * Traverses to depth 1.
	 */
	public static final StopEvaluator DEPTH_ONE = new StopEvaluator()
	{
		public boolean isStopNode( TraversalPosition currentPosition )
		{
			return currentPosition.depth() >= 1;
		}
	};
	
	/**
	 * Method invoked by traverser to see if current position is a stop node. 
	 * 
	 * @param currentPos The traversal position
	 * @return True if current position is a stop node
	 */
	public boolean isStopNode( TraversalPosition currentPos );
}
