package org.neo4j.impl.core;

public class RawRelationshipTypeData
{
	private final String name;
	private final int id;
	
	public RawRelationshipTypeData( int id, String name )
	{
		this.id = id;
		this.name = name;
	}
	
	public int getId()
	{
		return this.id;
	}
	
	public String getName()
	{
		return this.name;
	}
}
