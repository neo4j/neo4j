package org.neo4j.impl.core;

class Property
{
	private int id = -1;
	private Object value = null;
	
	Property( int id, Object value )
	{
		this.id = id;
		this.value = value;
	}
	
	int getId()
	{
		return id;
	}
	
	synchronized Object getValue()
	{
		if ( value == null )
		{
			value = NodeManager.getManager().loadPropertyValue( id );
		}
		return value;
	}
	
	void setId( int id )
	{
		this.id = id;
	}
}
