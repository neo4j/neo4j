package org.neo4j.impl.core;

public class RawPropertyData
{
	private int id = -1;
	private int indexId = -1;
	private Object value = null;

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
