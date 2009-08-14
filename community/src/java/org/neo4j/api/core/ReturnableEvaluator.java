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
package org.neo4j.api.core;

/**
 * A client hook for evaluating whether a specific node should be returned from
 * a traverser. When a traverser is created the client parameterizes it with an 
 * instance of a ReturnableEvaluator. The traverser then invokes the
 * {@link #isReturnableNode isReturnableNode()} operation just before returning
 * a specific node, allowing the client to either approve or disapprove of
 * returning that node.
 * <P>
 * When implementing a ReturnableEvaluator, the client investigates the
 * information encapsulated in a {@link TraversalPosition} to decide whether
 * a node is returnable. For example, here's a snippet detailing a
 * ReturnableEvaluator that will only return 5 nodes:
 * <CODE>
 * <PRE>
 * ReturnableEvaluator returnEvaluator = new ReturnableEvaluator()
 * {
 *     public boolean isReturnableNode( TraversalPosition position )
 *     {
 *         // Return nodes until we've reached 5 nodes or end of graph
 *         return position.returnedNodesCount() < 5;
 *     }
 * };
 * </PRE>
 * </CODE>
 */
public interface ReturnableEvaluator
{
	/**
	 * A returnable evaluator that returns all nodes encountered.
	 */
	public static final ReturnableEvaluator ALL = new ReturnableEvaluator()
	{
		public boolean isReturnableNode( TraversalPosition currentPosition )
		{
			return true;
		}
	};
	
	/**
	 * A returnable evaluator that returns all nodes except start node.
	 */
	public static final ReturnableEvaluator ALL_BUT_START_NODE = 
		new ReturnableEvaluator()
	{
		public boolean isReturnableNode( TraversalPosition currentPosition )
		{
			return currentPosition.notStartNode();
		}
	};
	
	/**
	 * Method invoked by traverser to see if current position is a returnable 
	 * node. 
	 * 
	 * @param currentPos The traversal position
	 * @return True if current position is a returnable node
	 */
	public boolean isReturnableNode( TraversalPosition currentPos );
}
