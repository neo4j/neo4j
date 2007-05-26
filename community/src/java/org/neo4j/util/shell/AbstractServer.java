package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AbstractServer extends UnicastRemoteObject
	implements ShellServer
{
	public static final String DEFAULT_NAME = "shell";
	public static final int DEFAULT_PORT = 8080;

	private Map<String, Serializable> properties =
		new HashMap<String, Serializable>();
	private Set<String> packages = new HashSet<String>();
	
	public AbstractServer()
		throws RemoteException
	{
		super();
	}
	
	public String getName()
	{
		return DEFAULT_NAME;
	}

	public void addPackage( String pkg )
	{
		this.packages.add( pkg );
	}

	public App findApp( String command ) throws RemoteException
	{
		for ( String pkg : this.packages )
		{
			String name = pkg + "." +
				command.substring( 0, 1 ).toUpperCase() +
				command.substring( 1, command.length() ).toLowerCase();
			try
			{
				App theApp = ( App ) Class.forName( name ).newInstance();
				theApp.setServer( this );
				return theApp;
			}
			catch ( Exception e )
			{
			}
		}
		return null;
	}

	public String interpretLine( String line, Session session, Output out )
		throws ShellException, RemoteException
	{
		if ( line == null || line.trim().length() == 0 )
		{
			return "";
		}
		
		CommandParser parser = new CommandParser( this, line );
		return parser.app().execute( parser, session, out );
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
		return "Welcome to the windh-utils shell";
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
