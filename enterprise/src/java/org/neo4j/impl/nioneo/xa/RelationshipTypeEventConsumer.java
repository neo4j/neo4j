package org.neo4j.impl.nioneo.xa;

import java.io.IOException;

import org.neo4j.impl.nioneo.store.RelationshipTypeData;

public interface RelationshipTypeEventConsumer
{
	/**
	 * Adds a new relationship type. 
	 * 
	 * @param id The id of the relationship type
	 * @param name The name of the relationship type
	 * @throws IOException If unable to add the relationship type
	 */
	public void addRelationshipType( int id, String name ) 
		throws IOException;

	/**
	 * Gets a relationship type with a given id.
	 * 
	 * @param id The id of the relationship type
	 * @return The relationship type data
	 * @throws IOException If unable to get relationship type
	 */
	public RelationshipTypeData getRelationshipType( int id ) 
		throws IOException;

	/**
	 * Gets all relationship types.
	 * 
	 * @return An array containing the relationship type data
	 * @throws IOException If unable to get the relationship types
	 */
	public RelationshipTypeData[] getRelationshipTypes()
		throws IOException;
}
