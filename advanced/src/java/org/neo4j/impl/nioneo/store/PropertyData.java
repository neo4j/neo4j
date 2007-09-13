package org.neo4j.impl.nioneo.store;


/**
 * Wrapper class for the data contained in a property record.
 */
public class PropertyData
{
	private final int id;
	private final int keyIndexId;
	private final Object value;
	
	/**
	 * @param id The id of the property
	 * @param key The key of the property
	 * @param value The value of the property
	 * @param nextPropertyId The next property id in the property chain, 
	 * -1 if last property
	 */
	public PropertyData( int id, int keyIndexId, Object value )
	{
		this.id = id;
		this.keyIndexId = keyIndexId;
		this.value = value;
	}
	
	public int getId()
	{
		return id;
	}
	
	public int getIndex()
	{
		return keyIndexId;
	}
	
	public Object getValue()
	{
		return value;
	}
}

