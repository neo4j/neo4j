package org.neo4j.impl.nioneo.xa;

import java.io.IOException;

import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.RelationshipData;

/**
 * Defines the operations in Neo that are node related.
 */
public interface NodeEventConsumer
{
	/**
	 * Creates a node. The <CODE>nodeId</CODE> is the position of the 
	 * record where the node will be created.
	 *  
	 * @param nodeId The id of the node
	 * @throws IOException If unable to create or if node already exist
	 */
	public void createNode( int nodeId ) throws IOException;
	
	/**
	 * Deletes a node. The <CODE>nodeId</CODE> is the position of the record
	 * where the node exist and will be deleted.
	 * 
	 * @param nodeId The id of the node
	 * @throws IOException If 
	 */
	public void deleteNode( int nodeId ) throws IOException;

	/**
	 * Checks if a node exists. If the record <CODE>nodeId</CODE> is in use
	 * <CODE>true</CODE> is returned.
	 * 
	 * @param nodeId The id of the node
	 * @return True if node exists
	 * @throws IOException If unable to check for node 
	 */
	public boolean loadLightNode( int nodeId ) throws IOException;

	/**
	 * Adds a property to the node.
	 * 
	 * @param nodeId The id of the node to add the property to
	 * @param propertyId The id of the property
	 * @param key The key of the property
	 * @param value The value of the property
	 * @throws IOException If unable to add property
	 */
	public void addProperty( int nodeId, int propertyId, String key, 
		Object value ) throws IOException;

	/**
	 * Changes the value of a property on a node.
	 * 
	 * @param propertyId The id of the property
	 * @param value The new value
	 * @throws IOException If unable to change property
	 */
	public void changeProperty( int nodeId, int propertyId, Object value ) 
		throws IOException;
	
	/**
	 * Removed a property from a node.
	 * 
	 * @param nodeId The id of the node
	 * @param propertyId The id of the property
	 * @throws IOException If unable to remove property
	 */
	public void removeProperty( int nodeId, int propertyId ) 
		throws IOException;
	
	/**
	 * Returns all properties connected to a node.
	 * 
	 * @param nodeId The id of the node
	 * @return An array containing all properties connected to the node 
	 * @throws IOException If unable to get the properties
	 */
	public PropertyData[] getProperties( int nodeId ) throws IOException;
	
	/**
	 * Returns all relationships connected to the node.
	 * 
	 * @param nodeId The id of the node
	 * @return An array containing all relationships connected to the node
	 * @throws IOException If unable to get the relationships
	 */
	public RelationshipData[] getRelationships( int nodeId ) 
		throws IOException;
}
