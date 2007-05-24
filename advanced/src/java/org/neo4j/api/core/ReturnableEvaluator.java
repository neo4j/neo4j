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
			return currentPosition.depth() != 0;
		}
	};
	
	/**
	 * Method invoked by traverser to see if current position is a returnable 
	 * node. 
	 * 
	 * @param currentPosition The traversal position
	 * @return True if current position is a returnable node
	 */
	public boolean isReturnableNode( TraversalPosition currentPosition );
}
