package org.neo4j.api.core;

import java.util.Collection;
import java.util.Iterator;

/**
 * See {@link Node#traverse Node.traverse(...)}.
 */
public interface Traverser extends Iterable<Node>
{
	/**
	 * Enum defining the two types of traversals.
	 */
	public static enum Order { DEPTH_FIRST, BREADTH_FIRST }
	
	/**
	 * Returns the current traversal postion.
	 * 
	 * @return The current traversal position
	 */
	public TraversalPosition currentPosition();
	
	/**
	 * Returns a collection of all nodes returned by this traverser.
	 * 
	 * @return A collection of all node returned by this traverser
	 */
	public Collection<Node> getAllNodes();
	
	// Doc: especially remove() thing
	/**
	 * Returns an iterator for this traverser.
	 * 
	 * @return An iterator for this traverser
	 */
	public Iterator<Node> iterator();
}
