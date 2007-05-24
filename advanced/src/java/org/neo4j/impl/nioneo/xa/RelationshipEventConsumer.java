package org.neo4j.impl.nioneo.xa;

import java.io.IOException;

import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.RelationshipData;

/**
 * Defines the operations in Neo that are relationship related.
 */
public interface RelationshipEventConsumer
{
	/**
	 * Creates a relationship.
	 * 
	 * @param id The id of the relationship
	 * @param directed Set to <CODE>true</CODE> if relationship is directed
	 * @param firstNode The first node connected
	 * @param secondNode The second node connected
	 * @param type The id of the relationship type
	 * @throws IOException If unable to create the relationship
	 */
	public void createRelationship( int id, int firstNode, int secondNode, 
		int type ) throws IOException;

	/**
	 * Deletes relationship with the given id.
	 * 
	 * @param id The id of the relationship
	 * @throws IOException If unable to delete the relationship
	 */
	public void deleteRelationship( int id ) throws IOException;

	/**
	 * Adds a property to the relationship.
	 * 
	 * @param relId The id of the relationship to add the property to
	 * @param propertyId The id of the property
	 * @param key The key of the property
	 * @param value The value of the property
	 * @throws IOException If unable to add property
	 */
	public void addProperty( int relId, int propertyId, String key, 
		Object value ) throws IOException;

	/**
	 * Changes the value of a property on a relationship.
	 * 
	 * @param propertyId The id of the property
	 * @param value The new value
	 * @throws IOException If unable to change property
	 */
	public void changeProperty( int relId, int propertyId, Object value ) 
		throws IOException;
	
	/**
	 * Removed a property from a relationship.
	 * 
	 * @param relId The id of the relationship
	 * @param propertyId The id of the property
	 * @throws IOException If unable to remove property
	 */
	public void removeProperty( int relId, int propertyId ) 
		throws IOException;
	
	/**
	 * Returns all properties connected to a relationship.
	 * 
	 * @param relId The id of the relationship
	 * @return An array containing all properties connected to the 
	 * relationship 
	 * @throws IOException If unable to get the properties
	 */
	public PropertyData[] getProperties( int relId )
		throws IOException;

	/**
	 * Gets a relationship with a given id.
	 * 
	 * @param id The id of the relationship
	 * @return The relationship data 
	 * @throws IOException if unable to get the relationship
	 */
	public RelationshipData getRelationship( int id ) throws IOException;

}
