/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.api.core;

import java.util.Collection;
import java.util.Iterator;

/**
 * Represents a traversal in the node space. A Traverser is an {@link Iterable}
 * that encapsulates a number of traversal parameters (defined at traverser
 * creation) and returns a list of nodes that match those parameters. It is
 * created by invoking {@link Node#traverse Node.traverse(...)}. Upon creation,
 * the traverser is positioned at the start node, but it doesn't actually start
 * traversing until its {@link #iterator() iterator().next()} method is invoked.
 * Typically it's used in a for-each loop as follows:
 * <code><pre>
 * Traverser friends = node.traverse(
 *     Order.BREADTH_FIRST,
 *     StopEvaluator.END_OF_NETWORK,
 *     ReturnableEvaluator.ALL_BUT_START_NODE,
 *     MyRelationshipTypes.KNOWS, Direction.OUTGOING );
 *     
 * for ( Node friend : friends )
 * {
 *     // ...
 * }
 * </pre></code>
 * @see Node#traverse
 */
public interface Traverser extends Iterable<Node>
{
	/**
	 * Enum defining the two types of traversals.
	 */
	public static enum Order { DEPTH_FIRST, BREADTH_FIRST }
	
	/**
	 * Returns the current traversal postion.
	 * @return The current traversal position
	 */
	public TraversalPosition currentPosition();
	
	/**
	 * Returns a collection of all nodes returned by this traverser.
	 * @return A collection of all node returned by this traverser
	 */
	public Collection<Node> getAllNodes();
	
	// Doc: especially remove() thing
	/**
	 * Returns an iterator for this traverser.
	 * @return An iterator for this traverser
	 */
	// Doc: does it create a new iterator or reuse the existing one? This is
	// very important! It must be re-use, how else would currentPosition()
    // make sense?
	public Iterator<Node> iterator();
}
