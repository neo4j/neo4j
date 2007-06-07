package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractServer extends UnicastRemoteObject
	implements ShellServer
{
	public static final String DEFAULT_NAME = "shell";
	public static final int DEFAULT_PORT = 1337;

	private Map<String, Serializable> properties =
		new HashMap<String, Serializable>();
	
	public AbstractServer()
		throws RemoteException
	{
		super();
	}
	
	public String getName()
	{
		return DEFAULT_NAME;
	}

	public Serializable getProperty( String key ) throws RemoteException
	{
		return this.properties.get( key );
	}

	public void setProperty( String key, Serializable value )
		throws RemoteException
	{
		this.properties.put( key, value );
	}
	
	public String welcome()
	{
		return "Welcome to the shell";
	}
	
	public void shutdown()
	{
		try
		{
			unexportObject( this, true );
		}
		catch ( NoSuchObjectException e )
		{
			// Ok
			System.out.println( "Couldn't shutdown server" );
		}
	}

	public void makeRemotelyAvailable( int port, String name )
		throws RemoteException
	{
		RmiLocation location =
			RmiLocation.location( "localhost", port, name );
		location.bind( this );
	}
}
