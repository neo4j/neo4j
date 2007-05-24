package org.neo4j.api.core;

/**
 * Represents a node in the network with properties and relationships to other
 * entities. Along with {@link Relationship relationships}, nodes are the
 * core building blocks of the Neo data representation model. Nodes are
 * created by invoking the {@link EmbeddedNeo#createNode} method.
 * <p>
 * Node has three major groups of operations: operations that deal with 
 * relationships, operations that deal with properties and operations that
 * create {@link Traverser traversers}.
 * <p>
 * The relationship operations provide a number of overloaded accessors (such as
 * <code>getRelationships(...)</code> with "filters" for type, direction, etc),
 * as well as the factory method {@link #createRelationshipTo
 * createRelationshipTo(...)} that connects two nodes with a relationship.
 * It also includes the convenience method {@link
 * #getSingleRelationship getSingleRelationship(...)} for accessing the
 * commonly occuring one-to-zero-or-one association. Of particular interest
 * might be that there are no <code>hasRelationship(...)</code> methods.
 * The idiomatic way to check if a node has any relationships matching
 * a "filter" of type and direction is:
 * <code>if ( node.getRelationships(filter).iterator().hasNext() ) { ... } </code>
 * <p>
 * The property operations give access to the key-value property pairs. Property
 * keys are always strings. Valid property value types are all the Java
 * primitives (<code>int</code>, <code>byte</code>, <code>float</code>, etc),
 * <code>java.lang.String</code>s and arrays of primitives and Strings.
 * <b>Please note</b> that Neo does NOT accept arbitrary objects as property
 * values. {@link #setProperty(String, Object) setProperty()} takes a
 * <code>java.lang.Object</code> for design reasons only.
 * <p>
 * The traversal factory methods instantiate a {@link Traverser traverser} that
 * starts traversing from this node.
 */
public interface Node
{
	/**
	 * Returns the unique id of this node. Ids are garbage collected over time
	 * so are only guaranteed to be unique at a specific set of time: if the
	 * node is deleted, it's likely that a new node at some point will get the
	 * old id. This make node ids brittle as public APIs.
	 * @return the id of this node
	 */
	public long getId();
	
	/**
	 * Deletes this node. Invoking any methods on this node after
	 * <code>delete()</code> has returned is invalid and will lead to
	 * unspecified behavior.
	 */
	public void delete();
	
	// Relationships
	/**
	 * Returns all the relationships attached to this node. If no relationships
	 * are attached to this node, an empty iterable will be returned.
	 * @return all relationships attached to this node
	 */
	public Iterable<Relationship> getRelationships();
	/**
	 * Returns all the relationships of type <code>type</code> that are
	 * attached to this node, regardless of direction. If no relationships
	 * of the given type are attached to this node, an empty iterable will be
	 * returned.
	 * @param type the given relationship type(s)
	 * @return all relationships of the given type(s) that are attached to this
	 * node
	 */
	public Iterable<Relationship> getRelationships( RelationshipType... type );
	
	/**
	 * Returns all {@link Direction#OUTGOING OUTGOING} or {@link
	 * Direction#INCOMING INCOMING} relationships from this node. If there are
	 * no relationships with the given direction attached to this node, an empty
	 * iterable will be returned. If {@link Direction#BOTH BOTH} is passed in
	 * as a direction, relationships of both directions are returned
	 * (effectively turning this into <code>getRelationships()</code>).
	 * @param dir the given direction, where <code>Direction.OUTGOING</code>
	 * means all relationships that have this node as
	 * {@link Relationship#getStartNode() start node} and <code>
	 * Direction.INCOMING</code> means all relationships that have this
	 * node as {@link Relationship#getEndNode() end node}
	 * @return all relationships with the given direction that are attached to
	 * this node
	 */
	public Iterable<Relationship> getRelationships( Direction dir );
	/**
	 * Returns all relationships with the given type and direction that are
	 * attached to this node. If there are no matching relationships, an empty
	 * iterable will be returned.
	 * @param type the given type
	 * @param dir the given direction, where <code>Direction.OUTGOING</code>
	 * means all relationships that have this node as
	 * {@link Relationship#getStartNode() start node} and <code>
	 * Direction.INCOMING</code> means all relationships that have this
	 * node as {@link Relationship#getEndNode() end node}
	 * @return all relationships attached to this node that match the given
	 * type and direction
	 */
	public Iterable<Relationship> getRelationships( RelationshipType type,
		Direction dir );
	/**
	 * Returns the only relationship of a given type and direction that
	 * is attached to this node, or <code>null</code>. This is a convenience
	 * method that is used in the commonly occuring situation where a node
	 * has exactly zero or one relationships of a given type and direction to
	 * another node. Typically this invariant is maintained by the rest of the
	 * code: if at any time more than one such relationships exist, it is a
	 * fatal error that should generate an unchecked exception. This method
	 * reflects that semantics and returns either <code>null</code> if there
	 * are zero relationships of the given type and direction, the single
	 * relationship if exactly one exists or throws an unchecked exception in
	 * all other cases.
	 * <p>
	 * This method should be used only in situations with an invariant as
	 * described above. In those situations, a "state-checking" method (e.g.
	 * <code>hasSingleRelationship(...)</code>)
	 * is not required, because this method behaves correctly "out of the box."
	 * If, for some reason, a state-checking method is needed, one can always
	 * use the usual "hasRelationship"-idiom:
	 * <code>if ( node.getRelationships( type, dir ).iterator().hasNext() )
	 * { ... }</code>
	 * @param type the type of the wanted relationship
	 * @param dir the direction of the wanted relationship (where
	 * <code>Direction.OUTGOING</code> means a relationship that has this node
	 * as {@link Relationship#getStartNode() start node} and <code>
	 * Direction.INCOMING</code> means a relationship that has this
	 * node as {@link Relationship#getEndNode() end node}) or
	 * {@link Direction#BOTH} if direction is irrelevant
	 * @return the single relationship matching the given type and direction if
	 * exactly one such relationship exists, or <code>null</code> if exactly
	 * zero such relationships exists
	 * @throws RuntimeException if more than one relationship matches the
	 * given type and direction
	 */
	public Relationship getSingleRelationship( RelationshipType type,
		Direction dir );
	/**
	 * Creates a relationship between this node and another node. The
	 * relationship is of type <code>type</code>. It starts at this node and
	 * ends at <code>otherNode</code>.
	 * @param otherNode the end node of the new relationship
	 * @param type the type of the new relationship
	 * @return the newly created relationship
	 */
	public Relationship createRelationshipTo( Node otherNode, 
		RelationshipType type );

	// Properties
	/**
	 * Returns <code>true</code> if this node has a property accessible
	 * through the given key, <code>false</code> otherwise. If key is
	 * <code>null</code>, this method returns <code>false</code>.
	 * @param key the property key
	 * @return <code>true</code> if this node has a property accessible
	 * through the given key, <code>false</code> otherwise
	 */
	public boolean hasProperty( String key );
	/**
	 * Returns the property value associated with the given key. The value
	 * is of one of the valid property types, i.e. a Java primitive, a
	 * {@link String String} or an array of any of the valid types. If there's
	 * no property associated with <code>key</code> an unchecked exception is
	 * raised.
	 * @param key the property key
	 * @return the property value associated with the given key
	 * @throws RuntimeException if there's no property associated with
	 * <code>key</code>
	 */
	// TODO: change exception type
	public Object getProperty( String key );
	/**
	 * Returns the property value associated with the given key, or a default
	 * value. The value is of one of the valid property types, i.e. a Java
	 * primitive, a {@link String String} or an array of any of the valid types.
	 * If <code>defaultValue</code> is not of a supported type, an unchecked
	 * exception is raised.
	 * @param key the property key
	 * @return the property value associated with the given key
	 * @throws IllegalArgumentException if <code>defaultValue</code> is of an
	 * unsupported type
	 */
	public Object getProperty( String key, Object defaultValue );
	
	/**
	 * Sets the property value for the given key to <code>value</code>. The
	 * property value must be one of the valid property types, i.e:
	 * <ul>
	 * <li><code>boolean</code> or <code>boolean[]</code></li>
	 * <li><code>byte</code> or <code>byte[]</code></li>
	 * <li><code>short</code> or <code>short[]</code></li>
	 * <li><code>int</code> or <code>int[]</code></li>
	 * <li><code>long</code> or <code>long[]</code></li>
	 * <li><code>float</code> or <code>float[]</code></li>
	 * <li><code>double</code> or <code>double[]</code></li>
	 * <li><code>char</code> or <code>char[]</code></li>
	 * <li><code>java.lang.String</code> or <code>String[]</code></li>
	 * </ul>
	 * @param key the key with which the new property value will be associated
	 * @param value the new property value, of one of the valid property types
	 * @throws IllegalArgumentException if <code>value</code> is of an
	 * unsuppoprted type
	 */
	// TODO: figure out semantics for setProp( blah, null );
	public void setProperty( String key, Object value );
	/**
	 * Removes and returns the property associated with the given key. If
	 * there's no property associated with the key, then <code>null</code>
	 * is returned.
	 * @param key the property key
	 * @return the property value that used to be associated with the given key
	 */
	public Object removeProperty( String key );
	/**
	 * Returns all currently valid property keys, or an empty iterable if this
	 * node has no properties.
	 * @return all property keys
	 */
	// TODO: figure out concurrency semantics
	public Iterable<String> getPropertyKeys();
	
	/**
	 * Returns all currently valid property values, or an empty iterable if this
	 * node has no properties. All values are of a supported property type, i.e.
	 * a Java primitive, a {@link String String} or an array of any of the
	 * supported types.
	 * @return all property values
	 */
	// TODO: figure out concurrency semantics
	public Iterable<Object> getPropertyValues();
	
	// Traversal
	/**
	 * Instantiates a traverser that will start at this node and traverse
	 * according to the given order and evaluators along the specified
	 * relationship type and direction. If the traverser should traverse more
	 * than one <code>RelationshipType</code>/<code>Direction</code> pair,
	 * use one of the overloaded variants of this method. For more information
	 * about traversal, see the {@link Traverser} documentation.
	 * @param traversalOrder the traversal order
	 * @param stopEvaluator an evaluator instructing the new traverser about
	 * when to stop traversing, either a predefined evaluator such as
	 * {@link StopEvaluator#END_OF_NETWORK} or a custom-written evaluator
	 * @param returnableEvaluator an evaluator instructing the new traverser
	 * about whether a specific node should be returned from the traversal,
	 * either a predefined evaluator such as {@link ReturnableEvaluator#ALL}
	 * or a customer-written evaluator 
	 * @param relationshipType the relationship type that the traverser will
	 * traverse along
	 * @param direction the direction in which the relationships of type
	 * <code>relationshipType</code> will be traversed
	 * @return a new traverser, configured as above
	 */
	public Traverser traverse( Traverser.Order traversalOrder,
		StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
		RelationshipType relationshipType, Direction direction );

	/**
	 * Instantiates a traverser that will start at this node and traverse
	 * according to the given order and evaluators along the two specified
	 * relationship type and direction pairs. If the traverser should traverse
	 * more than two  <code>RelationshipType</code>/<code>Direction</code>
	 * pairs, use the overloaded {@link #traverse(org.neo4j.api.Traverser.Order,
	 * StopEvaluator, ReturnableEvaluator, Object[]) varargs variant} of this
	 * method. For more information about traversal, see the {@link Traverser}
	 * documentation.
	 * @param traversalOrder the traversal order
	 * @param stopEvaluator an evaluator instructing the new traverser about
	 * when to stop traversing, either a predefined evaluator such as
	 * {@link StopEvaluator#END_OF_NETWORK} or a custom-written evaluator
	 * @param returnableEvaluator an evaluator instructing the new traverser
	 * about whether a specific node should be returned from the traversal,
	 * either a predefined evaluator such as {@link ReturnableEvaluator#ALL}
	 * or a customer-written evaluator 
	 * @param firstRelationshipType the first of the two relationship types that
	 * the traverser will traverse along
	 * @param firstDirection the direction in which the first relationship type
	 * will be traversed
	 * @param secondRelationshipType the second of the two relationship types
	 * that the traverser will traverse along
	 * @param secondDirection the direction that the second relationship type
	 * will be traversed
	 * @return a new traverser, configured as above
	 */
	// TODO: document the importance of reltype/dir order
	public Traverser traverse( Traverser.Order traversalOrder,
		StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
		RelationshipType firstRelationshipType, Direction firstDirection,
		RelationshipType secondRelationshipType, Direction secondDirection );

	/**
	 * Instantiates a traverser that will start at this node and traverse
	 * according to the given order and evaluators along the specified
	 * relationship type and direction pairs. Unlike the overloaded variants
	 * of this method, the relationship types and directions are passed in
	 * as a "varargs" variable-length argument which means that an arbitrary
	 * set of relationship type and direction pairs can be specified. The
	 * variable-length argument list should be every other relationship type and
	 * direction, starting with relationship type, e.g:
	 * <p>
	 * <code>node.traverse( BREADTH_FIRST, stopEval, returnableEval,
	 * MyRels.REL1, Direction.OUTGOING, MyRels.REL2, Direction.OUTGOING,
	 * MyRels.REL3, Direction.BOTH, MyRels.REL4, Direction.INCOMING );</code>
	 * <p>
	 * Unfortunately, the compiler cannot enforce this so an unchecked exception
	 * is raised if the variable-length argument has a different constitution.
	 * <p>
	 * For more information about traversal, see the {@link Traverser}
	 * documentation.
	 * @param traversalOrder the traversal order
	 * @param stopEvaluator an evaluator instructing the new traverser about
	 * when to stop traversing, either a predefined evaluator such as
	 * {@link StopEvaluator#END_OF_NETWORK} or a custom-written evaluator
	 * @param returnableEvaluator an evaluator instructing the new traverser
	 * about whether a specific node should be returned from the traversal,
	 * either a predefined evaluator such as {@link ReturnableEvaluator#ALL}
	 * or a customer-written evaluator
	 * @param relationshipTypesAndDirections a variable-length list of
	 * relationship types and their directions, where the first argument is
	 * a relationship type, the second argument the first type's direction,
	 * the third a relationship type, the fourth its direction, etc 
	 * @return a new traverser, configured as above
	 * @throws RuntimeException if the variable-length relationship type /
	 * direction list is not as described above
	 */
	public Traverser traverse( Traverser.Order traversalOrder,
		StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
		Object... relationshipTypesAndDirections );
}
