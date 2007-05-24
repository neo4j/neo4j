package org.neo4j.impl.nioneo.store;

/**
 * Wrapper class for the data contained in a property record.
 */
public class PropertyData
{
	private int id;
	private String key;
	private Object value;
	private int nextPropertyId;
	
	/**
	 * @param id The id of the property
	 * @param key The key of the property
	 * @param value The value of the property
	 * @param nextPropertyId The next property id in the property chain, 
	 * -1 if last property
	 */
	public PropertyData( int id, String key, Object value, 
		int nextPropertyId )
	{
		this.id = id;
		this.key = key;
		this.value = value;
		this.nextPropertyId = nextPropertyId;
	}
	
	public int getId()
	{
		return id;
	}
	
	public String getKey()
	{
		return key;
	}
	
	public Object getValue()
	{
		return value;
	}
	
	public int nextPropertyId()
	{
		return nextPropertyId;
	}
}

