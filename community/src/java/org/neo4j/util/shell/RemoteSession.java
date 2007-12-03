package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link Session} optimized to use with a
 * {@link RemoteClient}.
 */
public class RemoteSession extends UnicastRemoteObject implements Session
{
	private Map<String, Serializable> properties =
		new HashMap<String, Serializable>();
	
	private RemoteSession() throws RemoteException
	{
		super();
	}
	
	/**
	 * Convenient method for creating a new {@link RemoteSession} without
	 * catching the {@link RemoteException}.
	 * @return a new {@link RemoteSession}.
	 */
	public static RemoteSession newSession()
	{
		try
		{
			return new RemoteSession();
		}
		catch ( RemoteException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	public Serializable get( String key )
	{
		return this.properties.get( key );
	}

	public String[] keys()
	{
		return this.properties.keySet().toArray(
			new String[ this.properties.size() ] );
	}

	public Serializable remove( String key )
	{
		return this.properties.remove( key );
	}

	public void set( String key, Serializable value )
	{
		this.properties.put( key, value );
	}
}
