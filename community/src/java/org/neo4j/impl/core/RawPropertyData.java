package org.neo4j.impl.core;

public class RawPropertyData
{
	private final int id;
	private final int indexId;
	private final Object value;

	public RawPropertyData( int id, int indexId, Object value )
	{
		this.id = id;
		this.indexId = indexId;
		this.value = value;
	}
		
	int getId() 
	{ 
		return this.id; 
	}
	
	int getIndex() 
	{ 
		return this.indexId; 
	}
	
	Object getValue() 
	{ 
		return this.value; 
	}
}
