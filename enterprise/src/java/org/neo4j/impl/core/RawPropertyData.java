package org.neo4j.impl.core;

public class RawPropertyData
{
	private int id = -1;
	private String key = null;
	private Object value = null;

	public RawPropertyData( int id, String key, Object value )
	{
		this.id = id;
		this.key = key;
		this.value = value;
	}
		
	int getId() 
	{ 
		return this.id; 
	}
	
	String getKey() 
	{ 
		return this.key; 
	}
	
	Object getValue() 
	{ 
		return this.value; 
	}
}
