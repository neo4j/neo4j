package org.neo4j.impl.core;

public class RawRelationshipTypeData
{
	private String name = null;
	private int id = -1;
	
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
