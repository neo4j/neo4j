package org.neo4j.util.shell;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SameJvmSession implements Session
{
	private Map<String, Serializable> properties =
		new HashMap<String, Serializable>();
	
	public void set( String key, Serializable value )
	{
		this.properties.put( key, value );
	}
	
	public Serializable get( String key )
	{
		return this.properties.get( key );
	}

	public Serializable remove( String key )
	{
		return this.properties.remove( key );
	}

	public String[] keys()
	{
		return this.properties.keySet().toArray(
			new String[ this.properties.size() ] );
	}
}
