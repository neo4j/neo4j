package org.neo4j.api.core;

/**
 * Represents a relationship between two nodes in the network. A relationship
 * has a start node, an end node and a type. You can attach properties to
 * relationships with the exact same API as with nodes.
 * <p>
 * Relationships are created by invoking the {@link Node#createRelationshipTo
 * Node.createRelationshipTo()} method on a node as follows:
 * <p>
 * <code>
 * Relationship rel = node.createRelationshipTo( otherNode, MyRels.REL_TYPE );
 * </code>
 * <p>
 * The fact that the relationship API gives meaning to {@link #getStartNode()
 * start} and {@link #getEndNode() end} nodes implicitly means that all
 * relationships have a direction. In the example above, <code>rel</code> would
 * be directed <i>from</i> <code>node</code> <i>to</i> <code>otherNode</code>.
 * A relationship's start node and end node and their relation to
 * {@link Direction#OUTGOING} and {@link Direction#INCOMING} are defined so that
 * the assertions in the following code are <code>true</code>:
 * <code><pre>
 * Node a = neo.createNode(), b = neo.createNode();
 * Relationship rel = a.createRelationshipTo( b, MyRels.REL_TYPE );
 * // Now we have: (a) --- REL_TYPE ---> (b)
 * 
 * assert rel.getStartNode().equals( a );
 * assert rel.getEndNode().equals( b );
 * assert rel.getNodes()[0].equals( a ) && rel.getNodes()[1].equals( b );
 * 
 * rel = b.getSingleRelationship( MyRels.REL_TYPE, Direction.INCOMING ) ;
 * assert rel.getStartNode().equals( b );
 * assert rel.getEndNode().equals( a );
 * assert rel.getNodes()[0].equals( b ) && rel.getNodes()[1].equals( a );
 * </code></pre>
 * Furthermore, Neo guarantees that a relationship is never "hanging freely,"
 * i.e. {@link #getStartNode()}, {@link #getEndNode()}, {@link #getOtherNode(Node)}
 * and {@link #getNodes()} are guaranteed to always return valid, non-null
 * nodes.
 */
public interface Relationship
{
	public long getId();
	
	/**
	 * Deletes this relationship. Invoking any methods on this relationship
	 * after <code>delete()</code> has returned is invalid and will lead to
	 * unspecified behavior.
	 */
	public void delete();	

	// Node acessors
	/**
	 * Returns the start node of this relationship. For a definition of how
	 * start node relates to {@link Direction directions} as arguments to the
	 * {@link Node#getRelationships() relationship accessors} in Node, see the
	 * class documentation of Relationship.
	 * @return the start node of this relationship
	 */
	public Node getStartNode();
	
	/**
	 * Returns the end node of this relationship. For a definition of how
	 * end node relates to {@link Direction directions} as arguments to the
	 * {@link Node#getRelationships() relationship accessors} in Node, see the
	 * class documentation of Relationship.
	 * @return the end node of this relationship
	 */
	public Node getEndNode();
	
	/**
	 * A convenience operation that, given a node that is attached to this
	 * relationship, returns the other node. For example if <code>node</code>
	 * is a start node, the end node will be returned, and vice versa. This
	 * is a very convenient operation when you're manually traversing the
	 * node space by invoking one of the {@link Node#getRelationships()
	 * getRelationships()} operations on node. For example, to get the node
	 * "at the other end" of a relationship, use the following:
	 * <p>
	 * <code>
	 * Node endNode = node.getSingleRelationship( MyRels.REL_TYPE ).getOtherNode ( node );
	 * </code>
	 * <p>
	 * This operation will throw a runtime exception if <code>node</code> is
	 * neither this relationship's start node nor its end node.
	 * @param node the start or end node of this relationship
	 * @return the other node
	 * @throws RuntimeException if the given node is neither the start nor
	 * end node of this relationship
	 */
	public Node getOtherNode( Node node );
	
	/**
	 * Returns the two nodes that are attached to this relationship. First
	 * element in the array will be the start node, the second element the
	 * end node.
	 * @return the two nodes that are attached to this relationship
	 */
	public Node[] getNodes();
	
	/**
	 * Returns the type of this relationship. A relationship's type is an
	 * immutable property that is specified at Relationship
	 *  {@link Node#createRelationshipTo creation}. It will always be one of
	 * the elements of the {@link RelationshipType RelationshipType} enumeration
	 * passed in to {@link EmbeddedNeo#EmbeddedNeo EmbeddedNeo} at startup.
	 * @return the type of this relationship
	 */	
	public RelationshipType getType();	
	
	// Property operations
	/**
	 * See {@link Node#hasProperty(String)}
	 */
	public boolean hasProperty( String key );
	/**
	 * See {@link Node#getProperty(String)}
	 */
	public Object getProperty( String key );
	/**
	 * See {@link Node#getProperty(String, Object)}
	 */
	public Object getProperty( String key, Object defaultValue );
	/**
	 * See {@link Node#setProperty(String, Object)}
	 */
	public void setProperty( String key, Object value );
	/**
	 * See {@link Node#removeProperty(String)}
	 */
	public Object removeProperty( String key );
	/**
	 * See {@link Node#getPropertyKeys()}
	 */
	public Iterable<String> getPropertyKeys();
	/**
	 * See {@link Node#getPropertyValues()}
	 */
	public Iterable<Object> getPropertyValues();
}
