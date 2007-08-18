package org.neo4j.impl.nioneo.store;


/**
 * Wrapper class for the data contained in a property record.
 */
public class PropertyData
{
	private int id;
	private int keyIndexId;
	private Object value;
//	private int nextPropertyId;
	
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
//		this.nextPropertyId = nextPropertyId;
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
	
//	public int nextPropertyId()
//	{
//		return nextPropertyId;
//	}
}

