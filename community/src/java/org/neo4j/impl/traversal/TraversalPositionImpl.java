package org.neo4j.impl.traversal;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.TraversalPosition;
import org.neo4j.impl.core.NodeManager;

class TraversalPositionImpl implements TraversalPosition
{
	private Node				currentNode			= null;
	private Node				previousNode		= null;
	private Relationship		lastRelTraversed	= null;
	private int					currentDepth		= -1;
	private int					returnedNodesCount	= -1;

	TraversalPositionImpl(
						Node currentNode,
						Node previousNode,
						Relationship lastRelTraversed,
						int currentDepth
					 )
	{
		this.currentNode = currentNode;
		this.previousNode = previousNode;
		this.lastRelTraversed = lastRelTraversed;
		this.currentDepth = currentDepth;
	}
	
	void setReturnedNodesCount( int returnedNodesCount )
	{
		this.returnedNodesCount = returnedNodesCount;
	}
	
	public Node currentNode()
	{
		return this.currentNode;
	}
	
	public Node previousNode()
	{
		return this.previousNode;
	}
	
	public Relationship lastRelationshipTraversed()
	{
		return this.lastRelTraversed;
	}
	
	public int depth()
	{
		return this.currentDepth;
	}
	
	public int returnedNodesCount()
	{
		return this.returnedNodesCount;
	}
	
	boolean isValid()
	{
		if ( lastRelTraversed == null )
		{
			return true;
		}
		return NodeManager.getManager().isValidRelationship( 
			lastRelTraversed );
	}
}
