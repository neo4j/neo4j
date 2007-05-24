package org.neo4j.impl.nioneo.store;

/**
 * Defines valid property types.
 */
public enum PropertyType
{
	ILLEGAL(0),
	INT(1), 
	STRING(2),
	BOOL(3), 
	DOUBLE(4),
	FLOAT(5), 
	LONG(6), 	
	BYTE(7);
	
	private int type;
	
	PropertyType( int type )
	{
		this.type = type;
	}

	/**
	 * Returns an int value representing the type.
	 * 
	 * @return The int value for this property type
	 */
	public int intValue()
	{
		return type;
	}
}
