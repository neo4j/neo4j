package org.neo4j.impl.core;

class Property
{
	private static NodeManager nodeManager = NodeManager.getManager();
	
	private final int id;
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
			value = nodeManager.loadPropertyValue( id );
		}
		return value;
	}
	
//	void setId( int id )
//	{
//		this.id = id;
//	}
	
	void setNewValue( Object newValue )
	{
		this.value = newValue;
	}
}
