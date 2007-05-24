package org.neo4j.impl.core;

public class RawRelationshipData
{
	private int id = -1;
	private int firstNode = -1;
	private int secondNode = -1;
	private int type = -1;
	
	public RawRelationshipData( int id, int firstNode, int secondNode,
								int type )
	{
		this.id = id;
		this.firstNode = firstNode;
		this.secondNode = secondNode;
		this.type = type;
	}
	
	int getId()
	{
		return this.id;
	}
	
	public int getFirstNode()
	{
		return firstNode;
	}
	
	public int getSecondNode()
	{
		return secondNode;
	}
	
	public int getType()
	{
		return type;
	}
}