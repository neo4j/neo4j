package org.neo4j.impl.nioneo.store;

/**
 * Wrapper class for the data contained in a relationship type record.
 */
public class RelationshipTypeData
{
	private int id;
	private String name;
	
	/**
	 * @param id The id of the relationship type
	 * @param name The name of the relationship type
	 */
	public RelationshipTypeData( int id, String name )
	{
		this.id = id;
		this.name = name;
	}
	
	public int getId()
	{
		return id;
	}
	
	public String getName()
	{
		return name;
	}
}

