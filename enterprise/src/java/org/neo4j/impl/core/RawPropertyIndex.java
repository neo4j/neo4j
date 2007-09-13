package org.neo4j.impl.core;

public class RawPropertyIndex
{
	private final int keyId;
	private final String value;

	public RawPropertyIndex( int keyId, String value )
	{
		this.keyId = keyId;
		this.value = value;
	}
		
	int getKeyId() 
	{ 
		return this.keyId; 
	}
	
	String getValue() 
	{ 
		return this.value; 
	}
}
