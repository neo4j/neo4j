package org.neo4j.api.core;

import org.neo4j.impl.core.NodeManager;

/**
 * The main Neo factory, with functionality to start and shutdown Neo, create
 * and get nodes and define valid relationship types. This class is typically
 * used in the outer loop in a Neo-enabled application, for example as follows:
 * <pre><code>
 * EmbeddedNeo neo = new EmbeddedNeo( MyRelationshipTypes.class, "var/neo", true );
 * // ... use neo
 * neo.shutdown();
 * </code></pre>
 * Neo is started when this class is instantiated. It provides operations to
 * {@link #createNode() create notes}, {@link #getNodeById(long) get nodes
 * given an id}, get the {@link #getReferenceNode() reference node} and
 * ultimately {@link #shutdown() shutdown Neo}. Typically, once instantiated
 * the reference to EmbeddedNeo is stored away in a service registry or in
 * a singleton instance.
 * <p>
 * Please note that after startup (i.e. constructor invocation), all operations
 * that read or write to the node space must be invoked in a {@link Transaction
 * transactional context}.
 */
public class EmbeddedNeo
{
    private Class<? extends RelationshipType> validRelationshipTypes;
	
	/**
	 * Creates an embedded neo instance with a given set of relationship types
	 * and that reads data from a given store.
	 * @param validRelationshipTypes an enum class containing your relationship types
	 * @param storeDir the store directory for the neo db files
	 * @param create whether a new store directory will be created if it doesn't
	 * already exist
 	 * @throws NullPointerException if clazz is <code>null</code>
 	 * @throws IllegalArgumentException if clazz is not an enum
	 */
	public EmbeddedNeo( Class<? extends RelationshipType> validRelationshipTypes,
		String storeDir, boolean create )
	{	    
		this.validRelationshipTypes = validRelationshipTypes;
		NeoJvmInstance.start( validRelationshipTypes, storeDir, create );
	}
	
	/**
	 * Creates an embedded neo instance with a given set of relationship types,
	 * that reads data from a given store which will be created if it doesn't
	 * already exist. Invoking this constructor is equivalent to invoke
	 * <code>new EmbeddedNeo( clazz, storeDir, true )</code>.
	 * @param validRelationshipTypes an enum class containing your relationship types
	 * @param storeDir the store directory for the neo db files
 	 * @throws NullPointerException if clazz is <code>null</code>
 	 * @throws IllegalArgumentException if clazz not an enum
	 */
	public EmbeddedNeo( Class<? extends RelationshipType> validRelationshipTypes,
		String storeDir )
	{
		this( validRelationshipTypes, storeDir, true );
	}
	
	/**
	 * Creates a {@link Node}.
	 * @return the created node.
	 */
	public Node createNode()
	{
		return NodeManager.getManager().createNode();
	}
	
	/**
	 * Looks up a node by id.
	 * @param id the id of the node 
	 * @return the node with id <code>id</code> if found
	 * @throws RuntimeException if not found
	 */
	public Node getNodeById( long id )
	{
		return NodeManager.getManager().getNodeById( (int) id );
	}
	
	/**
	 * Returns the reference node.
	 * @return the reference node
	 * @throws RuntimeException if unable to get the reference node
	 */
	// TODO: Explain this concept
	public Node getReferenceNode()
	{
		return NodeManager.getManager().getReferenceNode();
	}
	
	/**
	 * Shuts down Neo. After this method has been invoked, it's invalid to
	 * invoke any methods in the Neo API.
	 */
	public void shutdown()
	{
		NeoJvmInstance.shutdown();
	}
	
	/**
	 * Returns the valid relationship types for this Neo instance invocation.
	 * This is the exact same class instance that was passed to the constructor
	 * of this EmbeddedNeo.
	 * @return the valid relationship types for this Neo instance invocation
	 */
	public Class<? extends RelationshipType> getRelationshipTypes()
	{
	    return this.validRelationshipTypes;
	}
}
