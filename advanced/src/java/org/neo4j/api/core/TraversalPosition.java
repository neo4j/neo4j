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
package org.neo4j.api.core;

/**
 * Encapsulates information about the current traversal position.
 */
public interface TraversalPosition
{
	/**
	 * Return the current node.
	 * 
	 * @return The current node
	 */
	public Node currentNode();
	
	/**
	 * Returns the previous node, may be null.
	 * 
	 * @return The previous node
	 */
	// null if start node
	public Node previousNode();
	/**
	 * Return the last relationship traversed, may be null.
	 * 
	 * @return The last relationship traversed
	 */
	// null if start node
	public Relationship lastRelationshipTraversed();
	
	/**
	 * Returns the current traversal depth.
	 * 
	 * @return The current traversal depth
	 */
	public int depth();
	
	/**
	 * Returns the number of nodes returned by traverser so far.
	 * 
	 * @return The number of returned nodes.
	 */
	public int returnedNodesCount();
	
	/**
	 * Returns <code>true</code> if the current position is anywhere except on
	 * the start node, <code>false</code> if it is on the start node. This is
	 * useful because code in {@link StopEvaluator the}
	 * {@link ReturnableEvaluator evaluators} usually have to treat the edge
	 * case of the start node separately and using this method makes that code a
	 * lot cleaner. For example, old code would be:
	 * 
	 * <pre>
	 * <code>
	 * public boolean isReturnableNode( TraversalPosition currentPos )
	 * {
	 * 	if ( currentPos.lastRelationshipTraversed() == null )
	 * 	{
	 * 		return false;
	 * 	}
	 * 	else
	 * 	{
	 * 		return currentPos.lastRelationshipTraversed().isType(
	 * 		    MyRelationshipTypes.SOME_REL );
	 * 	}
	 * }
	 * </code>
	 * </pre>
	 * 
	 * But using <code>notStartNode()</code>:
	 * 
	 * <pre>
	 * <code>
	 * public boolean isReturnableNode( TraversalPosition currentPos )
	 * {
	 * 	return currentPos.notStartNode()
	 * 	    &amp;&amp; currentPos.lastRelationshipTraversed().isType(
	 * 	        MyRelationshipTypes.SOME_REL );
	 * }
	 * </code>
	 * </pre>
	 * 
	 * @return <code>true</code> if the traversal is not currently positioned
	 * on the start node, <code>false</code> if it is
	 */
	public boolean notStartNode();

   /**
    * Returns <code>true</code> if the current position is the start node,
    * <code>false</code> otherwise. This is useful because code in
    * {@link StopEvaluator the} {@link ReturnableEvaluator evaluators} usually
    * have to treat the edge case of the start node separately and using this
    * method makes that code a lot cleaner. For example, old code would be:
    * 
    * <pre>
    * <code>
    * public boolean isReturnableNode( TraversalPosition currentPos )
    * {
    *      if ( currentPos.lastRelationshipTraversed() == null )
    *      {
    *              return false;
    *      }
    *      else
    *      {
    *              return currentPos.lastRelationshipTraversed().isType(
    *                  MyRelationshipTypes.SOME_REL );
    *      }
    * }
    * </code>
    * </pre>
    * 
    * But using <code>notStartNode()</code>:
    * 
    * <pre>
    * <code>
    * public boolean isReturnableNode( TraversalPosition currentPos )
    * {
    *      return !currentPos.isStartNode()
    *          &amp;&amp; currentPos.lastRelationshipTraversed().isType(
    *              MyRelationshipTypes.SOME_REL );
    * }
    * </code>
    * </pre>
    * 
    * @return <code>true</code> if the traversal is on the start node,
    * <code>false</code> otherwise.
    */
    public boolean isStartNode();
}