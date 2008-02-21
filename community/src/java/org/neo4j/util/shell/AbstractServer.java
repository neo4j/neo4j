package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

/**
 * A common implementation of a {@link ShellServer}.
 */
public abstract class AbstractServer extends UnicastRemoteObject
	implements ShellServer
{
	/**
	 * The default RMI name for a shell server,
	 * see {@link #makeRemotelyAvailable(int, String)}.
	 */
	public static final String DEFAULT_NAME = "shell";
	
	/**
	 * The default RMI port for a shell server,
	 * see {@link #makeRemotelyAvailable(int, String)}.
	 */
	public static final int DEFAULT_PORT = 1337;
	
	private Map<String, Serializable> properties =
		new HashMap<String, Serializable>();
	
	/**
	 * Constructs a new server.
	 * @throws RemoteException if an RMI exception occurs.
	 */
	public AbstractServer()
		throws RemoteException
	{
		super();
	}
	
	public String getName()
	{
		return DEFAULT_NAME;
	}

	public Serializable getProperty( String key )
	{
		return this.properties.get( key );
	}

	public void setProperty( String key, Serializable value )
	{
		this.properties.put( key, value );
	}
	
	public Serializable interpretVariable( String key, Serializable value,
		Session session ) throws RemoteException
	{
		return session.get( key );
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
//			System.out.println( "Couldn't shutdown server" );
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
