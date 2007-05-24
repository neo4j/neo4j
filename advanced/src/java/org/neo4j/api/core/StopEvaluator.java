package org.neo4j.api.core;

/**
 * A client hook for evaluating whether the traverser should traverse beyond
 * a specific node. When a traverser is created, the client parameterizes it 
 * with a StopEvaluator. The traverser then invokes the 
 * {@link #isStopNode isStopNode()} operation just before traversing
 * the relationships of a node, allowing the client to either approve or
 * disapprove of traversing beyond that node.
 * <P>
 * When implementing a StopEvaluator, the client investigates the
 * information encapsulated in a {@link TraversalPosition} to decide whether
 * to block traversal beyond a node. For example, here's a snippet detailing
 * a StopEvaluator that blocks traversal beyond a node if it has a certain
 * property value:
 * <CODE>
 * <PRE>
 * StopEvaluator stopEvaluator = new StopEvaluator()
 * {
 *     // Block traversal if the node has a property with key 'key' and value
 *     // 'someValue'
 *     public boolean isStopNode( TraversalPosition position )
 *     {
 *         Node node = position.previousNode();
 *         Object someProp = node.getProperty( "key" );
 *         return someProp instanceof String &&
 *             ((String) someProp).equals( "someValue" );
 *     }
 * };
 * </PRE>
 * </CODE>
 */
public interface StopEvaluator
{
	/**
	 * Traverser until the end of network, this evaluator returns 
	 * <CODE>false</CODE> all the time.
	 */
	public static final StopEvaluator END_OF_NETWORK = new StopEvaluator()
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
	 * @param currentPosition The traversal position
	 * @return True if current position is a stop node
	 */
	public boolean isStopNode( TraversalPosition currentPosition );
}
