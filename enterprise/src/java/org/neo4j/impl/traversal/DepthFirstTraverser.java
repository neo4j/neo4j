package org.neo4j.impl.traversal;

// Kernel imports

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;

/**
 * A traverser that traverses the node space depth-first. Depth-first means
 * that it fully visits the end-node of the first relationship before it visits
 * the end-nodes of the second and subsequent relationships. This class is
 * package private: any documentation interesting to a client programmer should
 * reside in {@link Traverser}. For some implementation documentation, see
 * {@link AbstractTraverser}.
 *
 * @see Traverser
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
	DepthFirstTraverser
						(
							Node startNode,
							RelationshipType[] traversableRels,
							Direction[] traversableDirections,
							RelationshipType[] preservingRels,
							Direction[] preservingDirections,
							StopEvaluator stopEvaluator,
							ReturnableEvaluator returnableEvaluator, 
							RandomEvaluator randomEvaluator
						)
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
		return (TraversalPositionImpl) this.stack.pop();
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
