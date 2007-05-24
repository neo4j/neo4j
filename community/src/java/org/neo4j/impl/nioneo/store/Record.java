package org.neo4j.impl.nioneo.store;

/**
 * Various constants used in records for different stores.
 */
public enum Record
{
	NOT_IN_USE ( (byte) 0, 0),
	IN_USE ( (byte) 1, 1), 
	RESERVED ( (byte) -1, -1), 
	NO_NEXT_PROPERTY( (byte) -1, -1 ),
	NO_PREVIOUS_PROPERTY( (byte) -1, -1 ),
	NO_NEXT_RELATIONSHIP( (byte) -1, -1 ),
	NO_PREV_RELATIONSHIP( (byte) -1, -1 ),
	NOT_DIRECTED( (byte) 0, 0 ),
	DIRECTED( (byte) 2, 2 ),
	NO_NEXT_BLOCK( (byte) -1, -1 ),
	NO_PREV_BLOCK( (byte) -1, -1 );
	
	private byte byteValue;
	private int intValue;
	
	Record( byte byteValue, int intValue )
	{
		this.byteValue = byteValue;
		this.intValue = intValue;
	}
	
	/**
	 * Returns a byte value representation for this record type.
	 * 
	 * @return The byte value for this record type
	 */
	public byte byteValue()
	{
		return byteValue;
	}
	
	/**
	 * Returns a int value representation for this record type.
	 * 
	 * @return The int value for this record type
	 */
	public int intValue()
	{
		return intValue; 
	}
}
