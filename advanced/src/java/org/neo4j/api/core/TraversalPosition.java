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
	public Node previousNode();
	/**
	 * Return the last relationship traversed, may be null.
	 * 
	 * @return The last relationship traversed
	 */
	public Relationship lastRelationshipTraversed();
	
	/**
	 * Returns the current traversal depth.
	 * 
	 * @return The current traversal depth
	 */
	public int depth();
	
	/**
	 * Return the number of nodes returned by traverser so far.
	 * 
	 * @return The number of returned nodes.
	 */
	public int returnedNodesCount();
}
